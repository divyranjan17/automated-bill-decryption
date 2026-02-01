import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
FILE = "token.json"

def obtain_credentials(file: str) -> Credentials:
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    creds = None
    if os.path.exists(file):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Token refresh failed: {e}")
                print("Re-authenticating...")
                creds = None  # Force new auth flow

            if not creds:  # Either no creds or refresh failed
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
                with open(file, "w") as token:
                    token.write(creds.to_json())    
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
            # Save the credentials for the next run
            with open(file, "w") as token:
                token.write(creds.to_json())

        return creds

# TODO: check if we need to convert this to interface with different fetching strategies
def fetch_unread_emails(creds: Credentials) -> None:
    try:
        service = build("gmail", "v1", credentials=creds)
        results = service.users().messages().list(userId="me").execute()
        print(results)
        print(type(results))
    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")

def fetch_unread_emails_by_labels(creds: Credentials, label: str) -> None:
    try:
        service = build("gmail", "v1", credentials=creds)
        results = service.users().messages().list(userId="me", labelIds = [label]).execute()
        print(results)
        print(type(results))
    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")

def main():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    try:
        creds = obtain_credentials(FILE)
        fetch_unread_emails_by_labels(creds, label="Label_5852119335048407156")
    except RefreshError:
        print("Credentials expired or revoked. Deleting token.json...")
        if os.path.exists(FILE):
            os.remove(FILE)
        print("Please run the script again to re-authenticate.")

if __name__ == "__main__":
    main()