import os
import tempfile
import shutil
import traceback
import re
from celery import shared_task
from googleapiclient.errors import HttpError

from uploader.utils import (
    authenticate_google_drive,
    extract_all_potential_links_from_last_slide,
    download_file_from_drive,
    download_youtube_video,
    check_file_exists,
    upload_file_to_drive,
)


@shared_task(bind=True)
def process_videos_from_ppt_links(self, ppt_file_id: str, target_folder_id: str):
    """
    Celery task that downloads the PPT, extracts video links,
    downloads the videos, and uploads them to the specified folder.
    """
    print(f"Starting video processing task for PPT ID: {ppt_file_id} in folder: {target_folder_id}")

    drive_service = authenticate_google_drive()
    if not drive_service:
        print("Failed to authenticate Google Drive. Task aborted.")
        return {'status': 'error', 'message': 'Authentication failed.'}

    temp_dir = tempfile.mkdtemp()
    temp_ppt_path = os.path.join(temp_dir, f"{ppt_file_id}.pptx")

    try:
        # Step 1: Download the PPT file
        print(f"Downloading PPT from Drive (ID: {ppt_file_id})...")
        download_success, _ = download_file_from_drive(drive_service, ppt_file_id, temp_ppt_path)
        if not download_success:
            print("Failed to download PPT file from Drive. Aborting task.")
            return {'status': 'error', 'message': 'Failed to download PPT.'}

        # Step 2: Extract video links from the downloaded PPT
        print("Extracting potential video links and associated names from PPTX...")
        extracted_links_with_names = extract_all_potential_links_from_last_slide(temp_ppt_path)
        print(f"Found {len(extracted_links_with_names)} potential links.")

        # Step 3: Download and upload videos
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

            video_downloaded = False
            local_video_path = None
            final_video_name = None

            if drive_match:
                video_drive_id = drive_match.group(1)
                print(f"\n--- Detected Google Drive video link: {link} (ID: {video_drive_id}) ---")
                try:
                    file_metadata = drive_service.files().get(fileId=video_drive_id, fields='name,mimeType').execute()
                    original_video_name_from_drive = file_metadata.get('name', f"unknown_video_{video_drive_id}")
                    base_name_for_file = re.sub(r'[\\/:*?"<>|]', '', (
                            suggested_name or os.path.splitext(original_video_name_from_drive)[0])).strip()
                    ext_from_drive = os.path.splitext(original_video_name_from_drive)[1]
                    final_video_name = f"{base_name_for_file}{ext_from_drive}"
                    if check_file_exists(drive_service, final_video_name, target_folder_id):
                        print(f"File '{final_video_name}' already exists in the folder. Skipping.")
                        continue
                    local_video_path = os.path.join(temp_dir, final_video_name)
                    print(f"Downloading '{final_video_name}'...")
                    video_downloaded, _ = download_file_from_drive(drive_service, video_drive_id, local_video_path)
                except HttpError as api_error:
                    print(f"Drive API error for link {link}: {api_error}")
                    continue
            elif youtube_match:
                print(f"\n--- Detected YouTube video link: {link} ---")
                base_name = re.sub(r'[\\/:*?"<>|]', '', (suggested_name or 'youtube_video')).strip()
                final_video_name = f"{base_name}.mp4"
                if check_file_exists(drive_service, final_video_name, target_folder_id):
                    print(f"File '{final_video_name}' already exists in the folder. Skipping.")
                    continue
                local_video_path = os.path.join(temp_dir, final_video_name)
                print(f"Downloading '{final_video_name}'...")
                video_downloaded, _ = download_youtube_video(link, local_video_path)
            else:
                print(f"Skipping unsupported link: {link}")
                continue

            if video_downloaded and local_video_path and os.path.exists(local_video_path):
                if os.path.getsize(local_video_path) < 1024:
                    print(f"WARNING: Downloaded video is very small. Skipping upload.")
                    os.remove(local_video_path)
                    continue

                print(f"Uploading '{final_video_name}' to Google Drive...")
                uploaded_file_id = upload_file_to_drive(drive_service, local_video_path, target_folder_id)
                if uploaded_file_id:
                    print(f"Successfully uploaded: {final_video_name} (ID: {uploaded_file_id})")
                    os.remove(local_video_path)
                    print(f"Cleaned up local video file: {local_video_path}")
                else:
                    print(f"Failed to upload '{final_video_name}'.")
            else:
                print(f"Failed to download video from {link}")

        print("Video processing task finished successfully.")
        return {'status': 'success', 'message': 'Video processing completed.'}

    except Exception as e:
        print(f"An unexpected error occurred during video processing task: {e}")
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}
    finally:
        shutil.rmtree(temp_dir)
