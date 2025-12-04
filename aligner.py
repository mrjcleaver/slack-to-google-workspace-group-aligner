import os
import logging
import argparse
from typing import List, Set, Dict, Optional

# Third-party libraries
# pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib slack_sdk
from google.oauth2 import service_account
from googleapiclient.discovery import build
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configuration via Environment Variables
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', 'credentials.json')
GOOGLE_SUBJECT_EMAIL = os.getenv('GOOGLE_SUBJECT_EMAIL') # Admin user to impersonate
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleWorkspaceClient:
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
        """Fetches all member emails from a Google Group."""
        members = set()
        page_token = None
        try:
            while True:
                results = self.service.members().list(
                    groupKey=group_email, pageToken=page_token
                ).execute()
                
                for member in results.get('members', []):
                    # We only care about users, not nested groups for this specific spec
                    if member.get('type') == 'USER':
                        members.add(member['email'].lower())
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
        except Exception as e:
            logger.error(f"Error fetching Google Group members: {e}")
            raise
        return members

class SlackClient:
    def __init__(self, token: str):
        self.client = WebClient(token=token)
        self.email_to_id_map = {}
        self.id_to_email_map = {}

    def populate_user_cache(self):
        """Fetches all Slack users to map Emails to User IDs."""
        logger.info("Populating Slack user cache...")
        cursor = None
        try:
            while True:
                response = self.client.users_list(cursor=cursor, limit=200)
                for member in response['members']:
                    if not member['deleted'] and not member['is_bot'] and 'profile' in member and 'email' in member['profile']:
                        email = member['profile']['email'].lower()
                        user_id = member['id']
                        self.email_to_id_map[email] = user_id
                        self.id_to_email_map[user_id] = email
                
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
        except SlackApiError as e:
            logger.error(f"Error fetching Slack users: {e.response['error']}")
            raise

    def get_channel_members(self, channel_id: str) -> Set[str]:
        """Fetches all member emails from a Slack Channel."""
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
            logger.error(f"Error fetching channel members: {e.response['error']}")
            raise

        # Convert IDs to Emails
        member_emails = set()
        for uid in member_ids:
            if uid in self.id_to_email_map:
                member_emails.add(self.id_to_email_map[uid])
        return member_emails

    def add_users_to_channel(self, channel_id: str, emails: Set[str]):
        """Adds users to a channel by their email."""
        user_ids = [self.email_to_id_map[email] for email in emails if email in self.email_to_id_map]
        if not user_ids:
            return
        
        # Slack API allows adding up to 1000 users at once, but safer to chunk
        chunk_size = 50 
        for i in range(0, len(user_ids), chunk_size):
            chunk = user_ids[i:i + chunk_size]
            try:
                self.client.conversations_invite(channel=channel_id, users=chunk)
                logger.info(f"Added {len(chunk)} users to channel {channel_id}.")
            except SlackApiError as e:
                # Handle case where user is already in channel (sometimes state lags)
                if e.response['error'] != 'already_in_channel':
                    logger.error(f"Failed to add users: {e.response['error']}")

    def remove_users_from_channel(self, channel_id: str, emails: Set[str]):
        """Removes users from a channel. Note: Bots can't usually kick unless they are admins."""
        user_ids = [self.email_to_id_map[email] for email in emails if email in self.email_to_id_map]
        for uid in user_ids:
            try:
                self.client.conversations_kick(channel=channel_id, user=uid)
                logger.info(f"Removed user {uid} from channel {channel_id}.")
            except SlackApiError as e:
                logger.error(f"Failed to remove user {uid}: {e.response['error']}")

def sync_group_to_channel(google_client, slack_client, group_email, channel_id, remove_extras=False, dry_run=False):
    logger.info(f"Syncing Google Group '{group_email}' <-> Slack Channel '{channel_id}'")

    # 1. Fetch Source of Truth (Google)
    google_members = google_client.get_group_members(group_email)
    logger.info(f"Found {len(google_members)} members in Google Group.")

    # 2. Fetch Current State (Slack)
    slack_members = slack_client.get_channel_members(channel_id)
    logger.info(f"Found {len(slack_members)} mapped members in Slack Channel.")

    # 3. Calculate Diff
    to_add = google_members - slack_members
    to_remove = slack_members - google_members

    logger.info(f"Analysis: {len(to_add)} to add, {len(to_remove)} to remove.")

    # 4. Execute
    if dry_run:
        logger.info("[DRY RUN] Would add: " + ", ".join(to_add))
        if remove_extras:
            logger.info("[DRY RUN] Would remove: " + ", ".join(to_remove))
        return

    if to_add:
        slack_client.add_users_to_channel(channel_id, to_add)
    
    if remove_extras and to_remove:
        slack_client.remove_users_from_channel(channel_id, to_remove)
    elif to_remove:
        logger.info(f"Skipping removal of {len(to_remove)} users (remove_extras=False).")

def main():
    parser = argparse.ArgumentParser(description='Align Google Workspace Groups to Slack Channels')
    parser.add_argument('--group', required=True, help='Google Group Email Address')
    parser.add_argument('--channel', required=True, help='Slack Channel ID (e.g., C12345678)')
    parser.add_argument('--remove', action='store_true', help='Remove users from Slack who are not in the Google Group')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making changes')
    args = parser.parse_args()

    if not all([GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SUBJECT_EMAIL, SLACK_BOT_TOKEN]):
        logger.error("Missing environment variables. Please set GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SUBJECT_EMAIL, and SLACK_BOT_TOKEN.")
        return

    # Initialize Clients
    g_client = GoogleWorkspaceClient(GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SUBJECT_EMAIL)
    s_client = SlackClient(SLACK_BOT_TOKEN)

    # Pre-fetch user mapping
    s_client.populate_user_cache()

    # Run Sync
    sync_group_to_channel(g_client, s_client, args.group, args.channel, args.remove, args.dry_run)

if __name__ == "__main__":
    main()
