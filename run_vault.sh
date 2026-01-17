#!/bin/bash

# --- CONFIGURATION ---
PROJECT_DIR="/home/lk/nso_vault"
LOG_FILE="$PROJECT_DIR/nso_worker.log"
VENV_PATH="$PROJECT_DIR/venv/bin/activate"

# 1. Check for Folder ID argument
if [ -z "$1" ]; then
    echo "❌ ERROR: No Folder ID provided."
    echo "Usage: runvault [FOLDER_ID]"
    exit 1
fi

NEW_ID=$1

# 2. Enter directory and clean up
cd $PROJECT_DIR
echo "🧹 Cleaning up old Celery processes..."
pkill -9 -f celery 2>/dev/null

# --- CLEAN OLD LOGS ---
cat /dev/null > "$LOG_FILE"

# 3. Update the Python Trigger Script
# This swaps the folder_id value in boot_trigger.py
echo "📝 Updating Root Folder ID to: $NEW_ID"
sed -i "s/folder_id = .*/folder_id = '$NEW_ID'/" boot_trigger.py

# 4. Start the Worker
echo "⚙️ Starting fresh Celery worker..."
source $VENV_PATH
nohup celery -A nso_vault worker --loglevel=info --concurrency=1 > $LOG_FILE 2>&1 &
disown

# 5. Small pause to let worker connect to Redis
sleep 2

# 6. Fire the Trigger
echo "🚀 Firing the boot trigger..."
python3 boot_trigger.py

echo "------------------------------------------------"
echo "✅ SYSTEM ACTIVE"
echo "Folder ID: $NEW_ID"
echo "Log File:  $LOG_FILE"
echo "------------------------------------------------"
echo "Current Status:"
ps aux | grep celery | grep -v grep
echo "------------------------------------------------"
echo "To watch live: tail -f nso_worker.log"