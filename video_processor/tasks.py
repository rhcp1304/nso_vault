# video_processor/tasks.py

from celery import shared_task
import os
import shutil
from django.conf import settings
from .services import process_video_links_internal, run_recursive_video_automation


# --- 1. EXISTING FUNCTIONALITY (RESTORED NAME) ---
@shared_task
def process_video_task(google_drive_folder_id):
    """
    Celery task to handle video processing.
    """
    try:
        # RESTORED: Using your exact original folder name
        temp_download_dir = 'temp_drive_downloads'
        process_video_links_internal(google_drive_folder_id, temp_download_dir)
        return {"status": "success", "message": "Video processing completed."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# --- 2. NEW AUTOMATION FUNCTIONALITY ---
@shared_task(bind=True)
def autonomous_recursive_run_task(self, root_folder_id):
    """
    Automated recursive run that shuts down the VM on completion.
    """
    # Using the same name for automation to respect your gitignore
    temp_download_dir = 'temp_drive_downloads'

    try:
        run_recursive_video_automation(root_folder_id, temp_download_dir)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        # Final cleanup before shutdown
        if os.path.exists(temp_download_dir):
            shutil.rmtree(temp_download_dir)

        # SELF-DESTRUCT
        print("🔌 Task finished. Shutting down system...")
        os.system("sudo shutdown -h now")