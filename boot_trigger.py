import os
import sys
import django

# 1. Setup Django environment
# This allows the script to use your models and Celery tasks
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nso_vault.settings')
django.setup()

try:
    # Import your new recursive task
    from video_processor.tasks import autonomous_recursive_run_task
except ImportError as e:
    print(f"❌ Error: Could not import autonomous_recursive_run_task. {e}")
    sys.exit(1)

# =========================================================
# 2. CONFIGURATION: CHANGE THIS ID EVERY WEEK FOR NOW
# =========================================================
ROOT_FOLDER_ID = "1LylXLQFmMQ0I6YrNZMdjfWts9BzsNb_r"


# =========================================================

def main():
    if not ROOT_FOLDER_ID or "Your_Actual_Drive" in ROOT_FOLDER_ID:
        print("⚠️  No valid ROOT_FOLDER_ID set. Script exiting.")
        sys.exit(1)

    print(f"🚀 Starting Autonomous Video Processing...")
    print(f"📂 Target Root Folder: {ROOT_FOLDER_ID}")

    # .delay() pushes the task to Redis for the Celery worker to pick up
    task = autonomous_recursive_run_task.delay(ROOT_FOLDER_ID)

    print(f"✅ Task queued successfully (ID: {task.id})")
    print("🔌 VM will process files and shut down automatically when finished.")


if __name__ == "__main__":
    main()