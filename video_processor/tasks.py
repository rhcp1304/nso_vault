# video_processor/tasks.py

from celery import shared_task
import os
import shutil
import subprocess
from django.conf import settings
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


# --- 2. NEW AUTOMATION FUNCTIONALITY (FIXED) ---
@shared_task(bind=True)
def autonomous_recursive_run_task(self, root_folder_id):
    """
    Automated recursive run that stops ONLY the celery service on completion.
    """
    temp_download_dir = 'temp_drive_downloads'

    try:
        run_recursive_video_automation(root_folder_id, temp_download_dir)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        # Final cleanup: Remove downloaded files to save disk space for other projects
        if os.path.exists(temp_download_dir):
            shutil.rmtree(temp_download_dir)

        # STOP ONLY NSO SERVICE
        # This kills the NSO worker but leaves the VM and other apps ALIVE.
        print("🛑 Task finished. Stopping NSO Celery worker...")
        os.system("sudo systemctl stop celery")