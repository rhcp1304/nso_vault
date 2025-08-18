# video_processor/tasks.py

from celery import shared_task
# Import the main processing function from the new services.py file
from .services import process_video_links_internal

@shared_task
def process_video_task(google_drive_folder_id):
    """
    Celery task to handle video processing.
    """
    try:
        temp_download_dir = 'temp_drive_downloads'
        process_video_links_internal(google_drive_folder_id, temp_download_dir)
        return {"status": "success", "message": "Video processing completed."}
    except Exception as e:
        return {"status": "error", "message": str(e)}