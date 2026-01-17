import os
import sys
import django

# 1. Setup Django environment
# This allows the script to use your models and Celery tasks
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nso_vault.settings')
django.setup()

try:
    # Import your recursive task
    from video_processor.tasks import autonomous_recursive_run_task
except ImportError as e:
    print(f"❌ Error: Could not import autonomous_recursive_run_task. {e}")
    sys.exit(1)

# =========================================================
# 2. CONFIGURATION: CHANGE THIS ID EVERY WEEK
# =========================================================
ROOT_FOLDER_ID = "1LylXLQFmMQ0I6YrNZMdjfWts9BzsNb_r"


# =========================================================

def main():
    # Validation
    if not ROOT_FOLDER_ID or "Your_Actual_Drive" in ROOT_FOLDER_ID:
        print("⚠️  No valid ROOT_FOLDER_ID set. Script exiting.")
        sys.exit(1)

    print(f"🚀 Starting Autonomous Video Processing...")
    print(f"📂 Target Root Folder: {ROOT_FOLDER_ID}")

    try:
        # .delay() pushes the task to Redis for the Celery worker to pick up
        task = autonomous_recursive_run_task.delay(ROOT_FOLDER_ID)

        print(f"✅ Task queued successfully (ID: {task.id})")
        print("ℹ️  The NSO Celery worker will now process all folders recursively.")
        print("ℹ️  VM will remain ONLINE for your other projects.")
        print("📊 Monitor progress with: screen -r nso_worker")

    except Exception as e:
        print(f"❌ Failed to queue task: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()