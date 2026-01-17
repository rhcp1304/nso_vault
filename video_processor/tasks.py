# video_processor/tasks.py

from celery import shared_task
import os
import shutil
from .services import process_video_links_internal, run_recursive_video_automation


# --- 1. EXISTING FUNCTIONALITY ---
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


# --- 2. NEW AUTOMATION FUNCTIONALITY (FIXED & PERSISTENT) ---
@shared_task(bind=True)
def autonomous_recursive_run_task(self, root_folder_id):
    """
    Automated recursive run that stays alive upon completion.
    """
    temp_download_dir = 'temp_drive_downloads'

    try:
        # This calls the recursive logic
        run_recursive_video_automation(root_folder_id, temp_download_dir)
        print(f"✅ Scan completed for root: {root_folder_id}")
    except Exception as e:
        print(f"❌ CRITICAL ERROR during automation: {e}")
    finally:
        # Cleanup downloads to save VM disk space for other projects
        if os.path.exists(temp_download_dir):
            shutil.rmtree(temp_download_dir)

        print("💡 Task finished. Worker remains ACTIVE and listening for new tasks.")