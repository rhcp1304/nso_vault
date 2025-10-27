import os
import re
import pickle
import tempfile
import base64
from datetime import datetime, timedelta

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from pptx import Presentation

# --- Configuration ---
# BASE_DIR should point to the project root (e.g., nso_vault/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'bdstorage_credentials.json')
TOKEN_FILE_PATH = os.path.join(BASE_DIR, 'token.pickle')

# UPDATED SCOPES: Includes both Drive and Gmail read-only
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/gmail.readonly'
]


# --- Google API Authentication ---

def authenticate_google_services():
    """Authenticates for both Drive and Gmail APIs using a single flow."""
    creds = None
    if os.path.exists(TOKEN_FILE_PATH):
        try:
            with open(TOKEN_FILE_PATH, 'rb') as token:
                creds = pickle.load(token)
            print("Loaded API credentials from token file.")
        except Exception:
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Credentials expired, refreshing...")
            creds.refresh(Request())
        else:
            print(f"Initiating new authentication flow using {CREDENTIALS_FILE}...")
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"Credentials file not found at {CREDENTIALS_FILE}. Please ensure it's there.")
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"Error during OAuth flow: {e}")
                return None, None
        try:
            with open(TOKEN_FILE_PATH, 'wb') as token:
                pickle.dump(creds, token)
            print(f"API credentials saved to {TOKEN_FILE_PATH}.")
        except Exception as e:
            print(f"Failed to save API token: {e}")

    try:
        drive_service = build('drive', 'v3', credentials=creds)
        gmail_service = build('gmail', 'v1', credentials=creds)
        return drive_service, gmail_service
    except Exception as e:
        print(f"Error building API services: {e}")
        return None, None


# --- Gmail Specific Functions ---

def get_messages_with_ppt(service, user_id='me'):
    """
    Fetches messages from Gmail containing PPT attachments received from
    the start of today (midnight) up to the current moment, sorted
    from OLDEST to NEWEST.
    """
    # Calculate the date for the start of today
    # We use 'after:YYYY/MM/DD' where the date is YESTERDAY to include all of TODAY.
    date_yesterday = datetime.now() - timedelta(days=1)
    date_query = date_yesterday.strftime('%Y/%m/%d')

    # Updated query: will search for messages received after yesterday's midnight.
    query = f'has:attachment filename:pptx after:{date_query}'

    print(f"Searching Gmail with query: '{query}'")

    try:
        messages = []
        page_token = None
        while True:
            # FIX: Removed the unsupported 'orderBy' and 'maxResults' arguments
            response = service.users().messages().list(
                userId=user_id,
                q=query,
                pageToken=page_token
            ).execute()

            messages.extend(response.get('messages', []))
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        # CRITICAL STEP for ASCENDING ORDER:
        # The API naturally returns messages by descending date, so reversing
        # the list gives us oldest-to-newest processing order.
        messages.reverse()

        return messages

    except HttpError as error:
        print(f'An HTTP error occurred while listing messages: {error}')
        return []
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        return []


def download_attachment(service, msg_id, user_id='me'):
    """
    Downloads the first .pptx attachment from a message to a temporary file.
    Returns (temp_file_path, filename, temp_dir) or (None, None, None).
    """
    try:
        message = service.users().messages().get(userId=user_id, id=msg_id).execute()

        for part in message.get('payload', {}).get('parts', []):
            if part.get('filename') and part.get('filename').lower().endswith('.pptx'):
                attachment_id = part.get('body', {}).get('attachmentId')
                if attachment_id:
                    att_data = service.users().messages().attachments().get(
                        userId=user_id, messageId=msg_id, id=attachment_id
                    ).execute()

                    data = att_data.get('data')
                    # Decode base64 URL-safe string
                    file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))

                    # Save to a temporary file
                    temp_dir = tempfile.mkdtemp()
                    temp_file_path = os.path.join(temp_dir, part.get('filename'))
                    with open(temp_file_path, 'wb') as f:
                        f.write(file_data)

                    print(f"Downloaded attachment to {temp_file_path}")
                    return temp_file_path, part.get('filename'), temp_dir

        return None, None, None

    except HttpError as error:
        print(f'An HTTP error occurred during attachment download: {error}')
        return None, None, None
    except Exception as e:
        print(f'An unexpected error occurred during attachment processing: {e}')
        return None, None, None


# --- PPT Processing ---

def get_market_and_zone_name_from_ppt(ppt_path):
    """
    Extracts market and zone names from the first slide of the PPT.
    """
    market_name = None
    zone_name = None
    try:
        prs = Presentation(ppt_path)
        if not prs.slides:
            print("PPT has no slides.")
            return None, None
        first_slide = prs.slides[0]
        slide_text = ""
        for shape in first_slide.shapes:
            if hasattr(shape, "text_frame") and shape.text_frame:
                for paragraph in shape.text_frame.paragraphs:
                    slide_text += paragraph.text + "\n"
        zone_match = re.search(
            r"ZONE\s*:\s*(.*?)(?:\s*STATE|\s*CITY|\s*PIN CODE|$)",
            slide_text,
            re.IGNORECASE | re.DOTALL
        )
        if zone_match:
            zone_name = zone_match.group(1).strip()
            zone_name = re.sub(r'\s*\[Image \d+\]\s*', '', zone_name).strip()
        else:
            print("Could not find 'ZONE : ' on the first slide.")
            return None, None
        if zone_name:
            # New combined pattern to capture both types of market names
            market_pattern_combined = r"(?:^" + re.escape(zone_name) + r"\s*\d_.*?_.*$|^BD-.*$|^Add_.*$)"

            market_match = re.search(
                market_pattern_combined,
                slide_text,
                re.IGNORECASE | re.MULTILINE
            )
            if market_match:
                market_name = market_match.group(0).strip()
                market_name = re.sub(r'\s*\[Image \d+\]\s*', '', market_name).strip()
                print(f"DEBUG: Found market name: {market_name}")
            else:
                print(
                    "Could not find a string starting with '{zone_name}' followed by a space and a digit, with at least two underscores, or a string starting with 'BD-' or 'Add_'.")
        if market_name is None:
            print(f"DEBUG: Extracted slide text:\n---START---\n{slide_text}\n---END---")
        return market_name, zone_name
    except Exception as e:
        print(f"An error occurred while reading the PPT: {e}")
        return None, None


# --- Drive Operations ---

def create_drive_folder(service, folder_name, parent_folder_id):
    """Creates a new folder."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }
    try:
        file = service.files().create(body=file_metadata, fields='id').execute()
        print(f"Folder '{folder_name}' created with ID: {file.get('id')}")
        return file.get('id')
    except HttpError as error:
        print(f"An HTTP error occurred during folder creation: {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during folder creation: {e}")
        return None


def find_or_create_folder(service, folder_name, parent_folder_id):
    """Finds an existing folder or creates a new one."""
    try:
        query = (
            f"name = '{folder_name}' and "
            f"mimeType = 'application/vnd.google-apps.folder' and "
            f"'{parent_folder_id}' in parents and "
            "trashed = false"
        )
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        items = results.get('files', [])
        if items:
            print(f"Found existing folder '{folder_name}' with ID: {items[0]['id']}")
            return items[0]['id']
        else:
            print(f"Folder '{folder_name}' not found, creating it...")
            return create_drive_folder(service, folder_name, parent_folder_id)
    except HttpError as error:
        print(f"An HTTP error occurred while finding/creating folder '{folder_name}': {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while finding/creating folder '{folder_name}': {e}")
        return None


def upload_file_to_drive(service, file_path, parent_folder_id):
    """
    Uploads a file to Drive, replacing an existing file with the same name
    in the same folder if one is found.
    """
    file_name = os.path.basename(file_path)

    # 1. Check for existing file with the same name in the target folder
    existing_file_id = None
    try:
        query = (
            f"name = '{file_name}' and "
            f"mimeType != 'application/vnd.google-apps.folder' and "
            f"'{parent_folder_id}' in parents and "
            "trashed = false"
        )
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        items = results.get('files', [])
        if items:
            existing_file_id = items[0]['id']
            print(f"Found existing file '{file_name}' with ID: {existing_file_id}. Replacing content.")

    except HttpError as error:
        print(f"An HTTP error occurred while searching for existing file: {error}")
        existing_file_id = None  # Proceed to create if search fails

    try:
        media = MediaFileUpload(file_path, resumable=True)

        if existing_file_id:
            # 2. Update/Replace the existing file content
            file = service.files().update(
                fileId=existing_file_id,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' replaced with new version. ID: {file.get('id')}")
        else:
            # 3. Simple upload (if no file was found)
            file_metadata = {
                'name': file_name,
                'parents': [parent_folder_id]
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")

        return file.get('id')

    except HttpError as error:
        print(f"An HTTP error occurred during upload/update: {error}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during upload/update: {e}")
        return None


# --- Main Processing Logic ---

def main_processor(ppt_file_path, parent_folder_id, drive_service):
    """
    Processes the PPT, determines the target folders (Zone/Market),
    and uploads the file, replacing existing ones if necessary.
    """
    print("Starting PPT processing and folder organization...")

    market_name, zone_name = get_market_and_zone_name_from_ppt(ppt_file_path)
    if not market_name:
        return {'error': 'Could not extract market name. Folder not created and PPT not uploaded.'}

    print(f"Extracted Market Name: {market_name}")
    print(f"Extracted Zone Name: {zone_name}")

    target_parent_for_market = parent_folder_id
    if zone_name:
        zone_folder_id = find_or_create_folder(drive_service, zone_name, parent_folder_id)
        if zone_folder_id:
            target_parent_for_market = zone_folder_id
        else:
            print("Failed to find or create Zone folder. Market folder will be created directly under the main parent.")

    market_folder_id = find_or_create_folder(drive_service, market_name, target_parent_for_market)
    if not market_folder_id:
        return {'error': f"Failed to create Market folder '{market_name}'. PPT file not uploaded."}

    uploaded_file_id = upload_file_to_drive(drive_service, ppt_file_path, market_folder_id)
    if uploaded_file_id:
        print("PPT file uploaded/replaced and placed in the correct folder.")
        return {'message': 'File uploaded and organized successfully!', 'file_id': uploaded_file_id}
    else:
        print("Failed to upload/replace PPT file to the drive.")
        return {'error': 'Failed to upload/replace PPT file to the drive.'}


# --- Django Views ---

def trigger_email_check_page(request):
    """
    Renders the HTML form to trigger the email check.
    """
    return render(request, 'drive_uploader/email_trigger.html')


@csrf_exempt
@require_http_methods(["POST"])
def check_email_and_upload(request):
    """
    Checks Gmail for PPT attachments received since midnight today and processes them.
    """
    parent_folder_id = request.POST.get('parent_folder_id')

    if not parent_folder_id:
        return JsonResponse({'error': 'Missing parent folder ID.'}, status=400)

    # 1. Authenticate
    drive_service, gmail_service = authenticate_google_services()
    if not drive_service or not gmail_service:
        return JsonResponse({'error': 'Google API authentication failed. Check credentials/token.'}, status=500)

    # 2. Fetch messages from the start of today up to the current time
    messages = get_messages_with_ppt(gmail_service)

    if not messages:
        # Message reflects the "today only" logic
        return JsonResponse({'message': 'No PPT attachments found in email received today.'}, status=200)

    all_results = []

    # 3. Process each message (this is now in ascending date order)
    for message in messages:
        msg_id = message['id']
        temp_file_path, filename, temp_dir = download_attachment(gmail_service, msg_id)

        if temp_file_path:
            # Run the main processor logic
            result = main_processor(temp_file_path, parent_folder_id, drive_service)
            result['source_email_id'] = msg_id
            result['source_filename'] = filename

            # 4. Clean up the temporary file and directory
            try:
                os.remove(temp_file_path)
                os.rmdir(temp_dir)
            except Exception as e:
                print(f"Warning: Failed to cleanup temp files: {e}")

            all_results.append(result)
        else:
            all_results.append({
                'error': f'Could not download a PPT attachment from message ID: {msg_id}',
                'source_email_id': msg_id
            })

    return JsonResponse({'message': f'Email processing complete. {len(messages)} emails checked. Results attached.',
                         'results': all_results}, status=200)