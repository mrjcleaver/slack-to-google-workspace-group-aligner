import os
import logging
import yaml
import time
from typing import List, Set, Dict, Any, Optional
from dataclasses import dataclass, field

# Third-party libraries
from google.oauth2 import service_account
from googleapiclient.discovery import build
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# --- CONFIGURATION & MODEL ---

@dataclass
class SyncStats:
    mapping_name: str
    added: int = 0
    removed: int = 0
    skipped: int = 0
    missing_accounts: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    status: str = "Success"

class Config:
    def __init__(self, config_path="config.yaml"):
        with open(config_path, 'r') as f:
            self.data = yaml.safe_load(f)
        
        self.settings = self.data.get('settings', {})
        self.mappings = self.data.get('mappings', [])
        
        # Overrides from Env Vars (optional, useful for CI)
        if os.getenv('DRY_RUN'):
            self.settings['dry_run'] = os.getenv('DRY_RUN').lower() == 'true'

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CLIENTS ---

class GoogleDirectoryClient:
    SCOPES = [
        'https://www.googleapis.com/auth/admin.directory.group.readonly',
        'https://www.googleapis.com/auth/admin.directory.user.readonly'
    ]

    def __init__(self, service_account_file: str, subject_email: str):
        self.creds = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=self.SCOPES
        ).with_subject(subject_email)
        self.service = build('admin', 'directory_v1', credentials=self.creds)

    def get_group_members(self, group_email: str) -> Set[str]:
        """Returns a set of normalized (lowercase) emails from the Google Group."""
        members = set()
        page_token = None
        try:
            while True:
                results = self.service.members().list(
                    groupKey=group_email, pageToken=page_token
                ).execute()
                
                for member in results.get('members', []):
                    if member.get('type') == 'USER':
                        members.add(member['email'].lower())
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
        except Exception as e:
            logger.error(f"Google API Error for {group_email}: {e}")
            raise
        return members

class SlackSyncClient:
    def __init__(self, token: str):
        self.client = WebClient(token=token)
        # Cache: { email: {id, is_admin, is_owner, is_bot, is_restricted} }
        self.user_cache = {}
        # Cache: { id: email }
        self.id_map = {}

    def populate_user_cache(self):
        """Fetches all users to build a lookup map for ID resolving and safety checks."""
        logger.info("Populating Slack user cache...")
        cursor = None
        try:
            while True:
                response = self.client.users_list(cursor=cursor, limit=200)
                for member in response['members']:
                    if member['deleted']:
                        continue
                        
                    uid = member['id']
                    profile = member.get('profile', {})
                    email = profile.get('email', '').lower()
                    
                    user_data = {
                        'id': uid,
                        'email': email,
                        'is_admin': member.get('is_admin', False),
                        'is_owner': member.get('is_owner', False),
                        'is_bot': member.get('is_bot', False),
                        'is_app_user': member.get('is_app_user', False),
                        'is_restricted': member.get('is_restricted', False) # Multi-channel guests
                    }
                    
                    if email:
                        self.user_cache[email] = user_data
                    self.id_map[uid] = user_data # Store by ID too for channel member lookup
                
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
        except SlackApiError as e:
            logger.error(f"Slack User List Error: {e.response['error']}")
            raise

    def get_channel_members(self, channel_id: str) -> Set[str]:
        """Returns a set of User IDs currently in the channel."""
        member_ids = set()
        cursor = None
        try:
            while True:
                response = self.client.conversations_members(
                    channel=channel_id, cursor=cursor, limit=200
                )
                member_ids.update(response['members'])
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
        except SlackApiError as e:
            logger.error(f"Slack Channel Member Error ({channel_id}): {e.response['error']}")
            raise
        return member_ids

    def invite_users(self, channel_id: str, emails: List[str], dry_run: bool) -> (int, List[str]):
        """Invites users by email. Returns count added and list of missing accounts."""
        added_count = 0
        missing_accounts = []
        
        user_ids_to_invite = []
        
        for email in emails:
            if email in self.user_cache:
                user_ids_to_invite.append(self.user_cache[email]['id'])
            else:
                logger.warning(f"  [!] No Slack account found for {email}")
                missing_accounts.append(email)

        # Batch invite (Slack allows up to 1000, we do chunks of 30 for safety)
        chunk_size = 30
        for i in range(0, len(user_ids_to_invite), chunk_size):
            chunk = user_ids_to_invite[i:i + chunk_size]
            if dry_run:
                logger.info(f"  [DRY RUN] Would invite: {chunk}")
                added_count += len(chunk)
            else:
                try:
                    self.client.conversations_invite(channel=channel_id, users=chunk)
                    added_count += len(chunk)
                    time.sleep(1) # Rate limit politeness
                except SlackApiError as e:
                    # Partial failures are tricky, but 'already_in_channel' is common
                    if e.response['error'] == 'already_in_channel':
                        pass
                    else:
                        logger.error(f"  [X] Failed to invite chunk: {e.response['error']}")
        
        return added_count, missing_accounts

    def kick_user(self, channel_id: str, user_id: str, dry_run: bool) -> bool:
        """Kicks a single user. Returns True if successful."""
        if dry_run:
            logger.info(f"  [DRY RUN] Would kick user {user_id}")
            return True
        
        try:
            self.client.conversations_kick(channel=channel_id, user=user_id)
            logger.info(f"  [-] Kicked user {user_id}")
            return True
        except SlackApiError as e:
            logger.error(f"  [X] Failed to kick {user_id}: {e.response['error']}")
            return False

    def post_report(self, channel_id: str, stats_list: List[SyncStats], dry_run: bool):
        """Posts a summary block to the Ops channel."""
        if not channel_id or dry_run:
            return

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔄 Membership Sync Report"}
            }
        ]

        for stats in stats_list:
            icon = "✅" if stats.status == "Success" else "⚠️"
            text = (f"*{stats.mapping_name}*\n"
                    f"{icon} Added: {stats.added} | Removed: {stats.removed} | Skipped: {stats.skipped}")
            
            if stats.missing_accounts:
                text += f"\n❓ Missing Slack Accts: {len(stats.missing_accounts)}"
            if stats.errors:
                text += f"\n⛔ Errors: {', '.join(stats.errors)}"
            
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": text}
            })
            
        try:
            self.client.chat_postMessage(channel=channel_id, blocks=blocks)
        except Exception as e:
            logger.error(f"Failed to post report: {e}")

# --- LOGIC ---

def run_sync():
    # 1. Load Config & Env
    config = Config()
    
    sa_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
    sa_subject = os.getenv('GOOGLE_SUBJECT_EMAIL')
    slack_token = os.getenv('SLACK_BOT_TOKEN')

    if not all([sa_file, sa_subject, slack_token]):
        logger.error("Missing credentials environment variables.")
        return

    # 2. Initialize Clients
    g_client = GoogleDirectoryClient(sa_file, sa_subject)
    s_client = SlackSyncClient(slack_token)

    # 3. Pre-fetch Slack Users (Optimization)
    s_client.populate_user_cache()

    stats_list = []
    
    logger.info(f"--- Starting Sync Job (Dry Run: {config.settings.get('dry_run')}) ---")

    # 4. Iterate Mappings
    for m in config.mappings:
        if not m.get('enabled', True):
            continue

        stats = SyncStats(mapping_name=m['name'])
        g_group = m['google_group']
        s_channel = m['slack_channel']
        protected_ids = set(m.get('protected_slack_users', []))
        
        logger.info(f"Processing: {m['name']} ({g_group} -> {s_channel})")

        try:
            # A. Fetch Source (Google)
            google_emails = g_client.get_group_members(g_group)
            
            # B. Fetch Destination (Slack)
            slack_uids = s_client.get_channel_members(s_channel)
            
            # Map Slack UIDs to Emails for diffing
            slack_member_map = {} # {email: uid}
            for uid in slack_uids:
                user_data = s_client.id_map.get(uid)
                if user_data and user_data['email']:
                    slack_member_map[user_data['email']] = uid

            slack_emails = set(slack_member_map.keys())

            # C. Compute Diff
            to_add_emails = google_emails - slack_emails
            
            # Determine removals (Candidate list)
            # Only remove if email is KNOWN in Slack but NOT in Google
            # Ignore UIDs that don't have emails (bots usually) unless we want to strict check bots
            candidate_remove_emails = slack_emails - google_emails

            # Safety Check: Max Changes
            total_changes = len(to_add_emails) + len(candidate_remove_emails)
            if total_changes > config.settings.get('max_changes_per_run', 50):
                msg = f"Aborting: {total_changes} changes exceeds limit of {config.settings.get('max_changes_per_run')}"
                logger.error(msg)
                stats.errors.append(msg)
                stats.status = "Aborted"
                stats_list.append(stats)
                continue

            # D. Execute Adds
            if to_add_emails:
                added, missing = s_client.invite_users(
                    s_channel, list(to_add_emails), config.settings['dry_run']
                )
                stats.added = added
                stats.missing_accounts = missing
            
            # E. Execute Removes
            for email in candidate_remove_emails:
                uid = slack_member_map[email]
                user_details = s_client.user_cache.get(email, {})
                
                # REJECTION LOGIC (Safety)
                if uid in protected_ids:
                    logger.info(f"  [Skip] {email} is in protected list.")
                    stats.skipped += 1
                    continue
                
                if user_details.get('is_admin') or user_details.get('is_owner'):
                    logger.info(f"  [Skip] {email} is Admin/Owner.")
                    stats.skipped += 1
                    continue
                
                if user_details.get('is_bot') or user_details.get('is_app_user'):
                    logger.info(f"  [Skip] {email} is a Bot.")
                    stats.skipped += 1
                    continue

                # KICK
                success = s_client.kick_user(s_channel, uid, config.settings['dry_run'])
                if success:
                    stats.removed += 1
                else:
                    stats.errors.append(f"Failed to kick {email}")

        except Exception as e:
            logger.error(f"Mapping Failed: {e}")
            stats.status = "Failed"
            stats.errors.append(str(e))
        
        stats_list.append(stats)

    # 5. Report
    s_client.post_report(config.settings.get('notify_channel_id'), stats_list, config.settings.get('dry_run'))
    logger.info("--- Sync Complete ---")

if __name__ == "__main__":
    run_sync()
