import os
import time
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these SCOPES, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

def authenticate():
    """Authenticate the user and return the Drive service."""
    creds = None
    # Check if token.json exists and is valid
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    
    # Build the service
    return build('drive', 'v3', credentials=creds)

def delete_file(service, file_id, file_name, retries=3):
    """Delete a single file with retries."""
    for attempt in range(retries):
        try:
            service.files().delete(fileId=file_id).execute()
            print(f"Deleted file: {file_name} ({file_id})")
            return True
        except HttpError as e:
            if attempt < retries - 1:
                print(f"Error deleting {file_name}: {e}. Retrying in 2 seconds... ({attempt + 1}/{retries})")
                time.sleep(2)
            else:
                print(f"Failed to delete {file_name} after {retries} attempts.")
                return False

def delete_files_by_filter(service, target_date, mime_type):
    """Delete files based on the creation date and mime type."""
    query = f"mimeType='{mime_type}' and createdTime > '{target_date}'"
    page_token = None
    
    while True:
        try:
            results = service.files().list(q=query, spaces='drive', fields="nextPageToken, files(id, name)", pageToken=page_token).execute()
            items = results.get('files', [])
            
            if not items:
                print('No more files found.')
                break
            else:
                for item in items:
                    delete_file(service, item['id'], item['name'])  # Retry logic is handled here
            
            # Handle pagination
            page_token = results.get('nextPageToken', None)
            if not page_token:
                break
        except HttpError as e:
            print(f"An error occurred while listing files: {e}. Retrying after 5 seconds...")
            time.sleep(5)  # Wait before retrying the list operation

def main():
    # Authenticate and get the service
    service = authenticate()

    # Example filter (YYYY-MM-DD)
    target_date = '2024-09-13T00:00:00Z'  # Adjust the target date (ISO 8601 format)
    mime_type = 'image/jpeg'  # Example mime type (e.g., application/pdf, image/png)
    
    delete_files_by_filter(service, target_date, mime_type)

if __name__ == '__main__':
    main()
