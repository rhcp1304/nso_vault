import os
import re
import pickle
import io
import tempfile
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from pptx import Presentation
from pytubefix import YouTube, exceptions as pytube_exceptions
from google.oauth2.credentials import Credentials

# =================================================================================
# === Helper Functions and Classes ===
# =================================================================================

# --- Configuration Constants ---
SCOPES_VIDEO = ['https://www.googleapis.com/auth/drive']
TOKEN_FILE_VIDEO = 'token.pickle'  # Changed to .pickle for consistency
CREDENTIALS_FILE_VIDEO = 'bdstorage_credentials.json'
API_SERVICE_NAME = 'drive'
API_VERSION = 'v3'


# --- Custom Logger/Style Class ---
class Style:
    def SUCCESS(self, msg): return f"\033[92mSUCCESS: {msg}\033[0m"

    def ERROR(self, msg): return f"\033[91mERROR: {msg}\033[0m"

    def WARNING(self, msg): return f"\033[93mWARNING: {msg}\033[0m"

    def INFO(self, msg): return f"{msg}"


# --- DriveHelper Class ---
class DriveHelper:
    def __init__(self):
        self.style = Style()

    def _log(self, message, style_func=None):
        if style_func:
            print(style_func(message))
        else:
            print(message)

    def get_authenticated_drive_service(self):
        creds = None
        token_path = os.path.join(os.getcwd(), TOKEN_FILE_VIDEO)
        credentials_path = os.path.join(os.getcwd(), CREDENTIALS_FILE_VIDEO)

        # 1. Load credentials from the pickle file if it exists
        if os.path.exists(token_path):
            self._log(f"Loading credentials from {TOKEN_FILE_VIDEO}...")
            with open(token_path, 'rb') as token:
                try:
                    creds = pickle.load(token)
                except Exception as e:
                    self._log(f"Error loading {TOKEN_FILE_VIDEO}: {e}. Re-authenticating...",
                              style_func=self.style.ERROR)

        # 2. Check if credentials are valid
        if not creds or not creds.valid:
            # If expired, attempt to refresh
            if creds and creds.expired and creds.refresh_token:
                self._log("Refreshing expired credentials...")
                try:
                    creds.refresh(Request())
                except Exception as e:
                    self._log(f"Error refreshing token: {e}. Re-authenticating...", style_func=self.style.ERROR)
                    creds = None  # Force a new authentication if refresh fails

            # If no valid credentials (or if refresh failed), perform the full flow
            if not creds:
                self._log(f"No valid credentials found. Initiating authentication flow (check your browser)...")
                if not os.path.exists(credentials_path):
                    self._log(
                        f"ERROR: '{CREDENTIALS_FILE_VIDEO}' not found at '{credentials_path}'. Please ensure it's in your project root.",
                        style_func=self.style.ERROR)
                    return None
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES_VIDEO)
                creds = flow.run_local_server(port=0)

            # 3. Save the new/refreshed credentials to the pickle file
            self._log(f"Saving new credentials to {TOKEN_FILE_VIDEO}...")
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)

        self._log("Authentication successful.", style_func=self.style.SUCCESS)
        return build(API_SERVICE_NAME, API_VERSION, credentials=creds)

    def find_pptx_in_drive_folder(self, service, folder_id: str):
        self._log(f"Searching for a PPTX file in folder ID '{folder_id}'...")
        query = f"'{folder_id}' in parents and mimeType = 'application/vnd.openxmlformats-officedocument.presentationml.presentation' and trashed = false"
        try:
            results = service.files().list(q=query, spaces='drive', fields='files(id, name, mimeType)').execute()
            items = results.get('files', [])
            if not items:
                self._log(f"No PPTX file found in folder '{folder_id}'.", style_func=self.style.WARNING)
                return None, None
            else:
                if len(items) > 1:
                    self._log(
                        f"WARNING: Found {len(items)} PPTX files. Using the first one found: '{items[0]['name']}'.",
                        style_func=self.style.WARNING)
                return items[0]['id'], items[0]['name']
        except HttpError as error:
            self._log(f"An error occurred while searching for PPTX: {error}", style_func=self.style.ERROR)
            return None, None

    def download_file_from_drive(self, service, file_id: str, destination_path: str):
        try:
            request = service.files().get_media(fileId=file_id)
            with open(destination_path, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
            return True, "video/mp4"
        except HttpError as error:
            self._log(f"An error occurred during file download from Drive (ID: {file_id}): {error}",
                      style_func=self.style.ERROR)
            return False, None
        except Exception as e:
            self._log(f"An unexpected error occurred during file download (ID: {file_id}): {e}",
                      style_func=self.style.ERROR)
            return False, None

    def download_youtube_video(self, youtube_url: str, destination_path: str):
        try:
            self._log(f"Downloading highest resolution video-only stream from: {youtube_url}")
            yt = YouTube(youtube_url)
            video_stream = yt.streams.filter(progressive=False, only_video=True).order_by('resolution').desc().first()
            if video_stream:
                self._log(f"Found video stream with resolution: {video_stream.resolution}")
                video_stream.download(output_path=os.path.dirname(destination_path),
                                      filename=os.path.basename(destination_path))
                return True, "video/mp4"
            else:
                self._log(f"No suitable video-only stream found for {youtube_url}", style_func=self.style.ERROR)
                return False, None
        except pytube_exceptions.VideoUnavailable:
            self._log(f"Error: The YouTube video at '{youtube_url}' is unavailable.", style_func=self.style.ERROR)
            return False, None
        except Exception as e:
            self._log(f"An error occurred while downloading YouTube video '{youtube_url}': {e}",
                      style_func=self.style.ERROR)
            return False, None

    def upload_file_to_drive(self, service, file_name: str, file_path: str, mime_type: str, parent_folder_id: str):
        file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        try:
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            return file.get('id')
        except HttpError as error:
            self._log(f"An error occurred during file upload ({file_name}): {error}", style_func=self.style.ERROR)
            return None
        except Exception as e:
            self._log(f"An unexpected error occurred during file upload ({file_name}): {e}",
                      style_func=self.style.ERROR)
            return None

    def extract_all_potential_links_from_last_slide(self, pptx_file_path: str) -> list[dict]:
        if not os.path.exists(pptx_file_path) or not pptx_file_path.lower().endswith('.pptx'):
            self._log(f"Error: Invalid PPTX file path '{pptx_file_path}'", style_func=self.style.ERROR)
            return []
        found_links_with_names = []
        url_pattern = re.compile(r'https?://[^\s\]\)\}>"]+')
        try:
            prs = Presentation(pptx_file_path)
            if not prs.slides:
                self._log("No slides found in presentation.")
                return []
            last_slide = prs.slides[-1]
            self._log(f"Analyzing the last slide (Slide {len(prs.slides)}) for all potential links...")

            def get_text_from_cell(cell):
                if cell.text_frame:
                    return " ".join("".join([run.text for run in p.runs]) for p in cell.text_frame.paragraphs).strip()
                return ""

            def find_urls_in_text_content(text_frame_obj, associated_name=None):
                for paragraph in text_frame_obj.paragraphs:
                    full_text = "".join([run.text for run in paragraph.runs])
                    for match in url_pattern.finditer(full_text):
                        found_links_with_names.append({'name': associated_name, 'link': match.group(0).strip()})
                    for run in paragraph.runs:
                        if run.hyperlink.address:
                            found_links_with_names.append({'name': associated_name, 'link': run.hyperlink.address})

            for shape in last_slide.shapes:
                if hasattr(shape, 'action') and shape.action.hyperlink:
                    found_links_with_names.append({'name': None, 'link': shape.action.hyperlink.address})
                if shape.has_text_frame:
                    find_urls_in_text_content(shape.text_frame)
                if shape.has_table:
                    table = shape.table
                    name_col_idx = -1
                    if table.rows:
                        header_row = table.rows[0]
                        for i, cell in enumerate(header_row.cells):
                            cell_text = get_text_from_cell(cell).lower().strip()
                            if "name" in cell_text or "store name" in cell_text:
                                name_col_idx = i
                                break
                    for row_idx, row in enumerate(table.rows):
                        if row_idx == 0: continue
                        current_row_name = None
                        if name_col_idx != -1 and name_col_idx < len(row.cells):
                            current_row_name = get_text_from_cell(row.cells[name_col_idx])
                        for cell in row.cells:
                            if cell.text_frame:
                                find_urls_in_text_content(cell.text_frame, associated_name=current_row_name)
                if hasattr(shape, 'image') and hasattr(shape.image, 'hyperlink') and shape.image.hyperlink.address:
                    found_links_with_names.append({'name': None, 'link': shape.image.hyperlink.address})
            unique_links_with_names = {}
            for item in found_links_with_names:
                link, name = item['link'], item['name']
                if link not in unique_links_with_names or (name and not unique_links_with_names[link]['name']):
                    unique_links_with_names[link] = {'name': name, 'link': link}
            return list(unique_links_with_names.values())
        except Exception as e:
            self._log(f"An error occurred while extracting links: {e}", style_func=self.style.ERROR)
            return []

    def get_market_name_prefix(self, pptx_file_path: str) -> str:
        """
        Extracts the market name from the first slide of a PPTX file.
        The market name is identified by a pattern: 'ZONE : [zone name]' followed by
        a line starting with the zone name, a digit, and two underscores.
        """
        if not os.path.exists(pptx_file_path):
            self._log(f"Error: PPTX file not found locally at '{pptx_file_path}'", style_func=self.style.ERROR)
            return ""
        try:
            prs = Presentation(pptx_file_path)
            if not prs.slides:
                self._log("PPT has no slides.", style_func=self.style.WARNING)
                return ""
            first_slide = prs.slides[0]
            slide_text = ""
            for shape in first_slide.shapes:
                if hasattr(shape, "text_frame") and shape.text_frame:
                    for paragraph in shape.text_frame.paragraphs:
                        slide_text += paragraph.text + "\n"

            # 1. Search for the ZONE name
            zone_match = re.search(
                r"ZONE\s*:\s*(.*?)(?:\s*STATE|\s*CITY|\s*PIN CODE|$)",
                slide_text,
                re.IGNORECASE | re.DOTALL
            )
            if not zone_match:
                self._log("Could not find 'ZONE : ' on the first slide.", style_func=self.style.WARNING)
                return ""

            zone_name = zone_match.group(1).strip()
            zone_name = re.sub(r'\s*\[Image \d+\]\s*', '', zone_name).strip()

            if not zone_name:
                self._log("Found 'ZONE : ' but the zone name was empty.", style_func=self.style.WARNING)
                return ""

            # 2. Use the zone name to find the market name
            market_pattern = r"^" + re.escape(zone_name) + r"\s*\d_.*?_.*$"
            market_match = re.search(
                market_pattern,
                slide_text,
                re.IGNORECASE | re.MULTILINE
            )

            if market_match:
                full_market_name = market_match.group(0).strip()
                full_market_name = re.sub(r'\s*\[Image \d+\]\s*', '', full_market_name).strip()

                # Apply the original logic to extract the prefix from the found market name
                if '_' in full_market_name:
                    prefix = full_market_name.rsplit('_', 1)[1].strip()
                    self._log(f"Extracted market name prefix: '{prefix}'", style_func=self.style.INFO)
                    return prefix
                else:
                    self._log(f"No underscore found in market name. Using full value: '{full_market_name}'",
                              style_func=self.style.WARNING)
                    return full_market_name
            else:
                self._log(
                    f"Could not find a string starting with '{zone_name}' followed by a digit and two underscores.",
                    style_func=self.style.WARNING)
                return ""
        except Exception as e:
            self._log(f"An error occurred while extracting market name prefix: {e}", style_func=self.style.ERROR)
            return ""

    def check_file_exists(self, service, file_name: str, folder_id: str):
        query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
        try:
            results = service.files().list(q=query, fields='files(id)').execute()
            items = results.get('files', [])
            return len(items) > 0
        except HttpError as error:
            self._log(f"An error occurred while checking for file existence: {error}", style_func=self.style.ERROR)
            return False


# --- Main Processing Function ---
def process_video_links_internal(google_drive_folder_id, temp_download_dir):
    drive_helper = DriveHelper()
    style = drive_helper.style
    print(style.SUCCESS(f"Starting process for Google Drive folder ID: {google_drive_folder_id}"))
    if not os.path.exists(temp_download_dir):
        os.makedirs(temp_download_dir)
        print(f"Created temporary download directory: '{temp_download_dir}'")
    else:
        print(f"Using existing temporary download directory: '{temp_download_dir}'")
    service = drive_helper.get_authenticated_drive_service()
    if not service:
        print(style.ERROR("Failed to authenticate with Google Drive API."))
        return
    pptx_drive_id, pptx_file_name = drive_helper.find_pptx_in_drive_folder(service, google_drive_folder_id)
    if not pptx_drive_id:
        print(style.ERROR(f"No PPTX file found in folder '{google_drive_folder_id}'."))
        return
    local_pptx_path = os.path.join(temp_download_dir, pptx_file_name)
    print(f"Downloading PPTX '{pptx_file_name}' (ID: {pptx_drive_id})...")
    if not drive_helper.download_file_from_drive(service, pptx_drive_id, local_pptx_path):
        print(style.ERROR(f"Failed to download PPTX: {pptx_file_name}"))
        return
    print(style.SUCCESS(f"PPTX downloaded to: {local_pptx_path}"))
    market_name_prefix_raw = drive_helper.get_market_name_prefix(local_pptx_path)

    # First, perform the regular expression substitution
    cleaned_name = re.sub(r'[\\/:*?\"<>|]', '', market_name_prefix_raw).strip()

    # Then, use the result in the f-string
    prefix_for_filename = f"{cleaned_name} " if market_name_prefix_raw else ""

    # prefix_for_filename = f"{re.sub(r'[\\/:*?"<>|]', '', market_name_prefix_raw).strip()} " if market_name_prefix_raw else ""
    print(
        f"Using market name prefix: '{prefix_for_filename}'" if prefix_for_filename else "No valid market name prefix found.")
    print("Extracting potential video links and associated names from PPTX...")
    extracted_links_with_names = drive_helper.extract_all_potential_links_from_last_slide(local_pptx_path)
    print(f"Found {len(extracted_links_with_names)} potential links.")
    google_drive_file_id_pattern = re.compile(r'drive\.google\.com/(?:file/d/|uc\?id=)([a-zA-Z0-9_-]+)')
    youtube_pattern = re.compile(r'(?:youtube\.com/watch\?v=|youtu\.be/)([\w-]+)')
    processed_links = set()
    for item in extracted_links_with_names:
        link, suggested_name = item['link'], item['name']
        if link in processed_links:
            continue
        processed_links.add(link)
        drive_match = google_drive_file_id_pattern.search(link)
        youtube_match = youtube_pattern.search(link)
        video_mime_type = "video/mp4"
        video_downloaded = False
        local_video_path = None
        final_video_name = None
        if drive_match:
            video_drive_id = drive_match.group(1)
            print(f"\n--- Detected Google Drive video link: {link} (ID: {video_drive_id}) ---")
            try:
                file_metadata = service.files().get(fileId=video_drive_id, fields='name,mimeType').execute()
                original_video_name_from_drive = file_metadata.get('name', f"unknown_video_{video_drive_id}")
                base_name_for_file = re.sub(r'[\\/:*?"<>|]', '', (
                        suggested_name or os.path.splitext(original_video_name_from_drive)[0])).strip()
                ext_from_drive = os.path.splitext(original_video_name_from_drive)[1]
                final_video_name = f"{prefix_for_filename}{base_name_for_file}{ext_from_drive}"

                # Check for existing file before proceeding
                if drive_helper.check_file_exists(service, final_video_name, google_drive_folder_id):
                    print(style.WARNING(
                        f"File '{final_video_name}' already exists in the folder. Skipping download and upload."))
                    continue

                local_video_path = os.path.join(temp_download_dir, final_video_name)
                print(f"Downloading '{final_video_name}'...")
                video_downloaded, _ = drive_helper.download_file_from_drive(service, video_drive_id, local_video_path)
            except HttpError as api_error:
                print(style.ERROR(f"Drive API error for link {link}: {api_error}"))
                continue
        elif youtube_match:
            print(f"\n--- Detected YouTube video link: {link} ---")

            # The corrected line to remove the YouTube ID from the final name
            base_name = re.sub(r'[\\/:*?"<>|]', '', (suggested_name or 'youtube_video')).strip()
            final_video_name = f"{prefix_for_filename}{base_name}.mp4"

            # Check for existing file before proceeding
            if drive_helper.check_file_exists(service, final_video_name, google_drive_folder_id):
                print(style.WARNING(
                    f"File '{final_video_name}' already exists in the folder. Skipping download and upload."))
                continue

            local_video_path = os.path.join(temp_download_dir, final_video_name)
            print(f"Downloading '{final_video_name}'...")
            video_downloaded, _ = drive_helper.download_youtube_video(link, local_video_path)
        else:
            print(f"Skipping unsupported link: {link}")
            continue

        if video_downloaded and local_video_path:
            # The local filename might still have a number (e.g., "_1")
            # to prevent pytube from overwriting. We handle this after the download.

            # Get the actual filename that was downloaded by pytube
            downloaded_file_name = os.path.basename(local_video_path)

            print(style.SUCCESS(f"Successfully downloaded to: {local_video_path}"))

            if os.path.getsize(local_video_path) < 1024:
                print(style.WARNING(f"WARNING: Downloaded video is very small. Skipping upload."))
                os.remove(local_video_path)
                continue

            print(f"Uploading '{final_video_name}' to Google Drive...")
            uploaded_file_id = drive_helper.upload_file_to_drive(service, final_video_name, local_video_path,
                                                                 video_mime_type, google_drive_folder_id)
            if uploaded_file_id:
                print(style.SUCCESS(f"Successfully uploaded: {final_video_name} (ID: {uploaded_file_id})"))
                os.remove(local_video_path)
                print(f"Cleaned up local video file: {local_video_path}")
            else:
                print(style.ERROR(f"Failed to upload '{final_video_name}'."))
        else:
            print(style.ERROR(f"Failed to download video from {link}"))
    if os.path.exists(local_pptx_path):
        os.remove(local_pptx_path)
        print(f"Cleaned up temporary PPTX file: {local_pptx_path}")
    if os.path.exists(temp_download_dir):
        try:
            for file_name in os.listdir(temp_download_dir):
                file_path = os.path.join(temp_download_dir, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            if not os.listdir(temp_download_dir):
                os.rmdir(temp_download_dir)
                print(f"Cleaned up temporary directory: {temp_download_dir}")
        except OSError as e:
            print(style.WARNING(f"Could not remove directory '{temp_download_dir}': {e}"))
    print(style.SUCCESS("Process completed."))
