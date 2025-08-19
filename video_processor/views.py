import os
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .tasks import process_videos_from_ppt_links  # Import the new Celery task

# =================================================================================
# === Django Views for Video Processor ===
# =================================================================================

def video_processor_page(request):
    """Renders the HTML form for the video processing."""
    return render(request, 'video_processor/index.html')

@csrf_exempt
@require_POST
def start_video_processing(request):
    """
    Handles the POST request from the front-end and starts the video
    processing in a Celery background task.
    """
    market_folder_id = request.POST.get('market_folder_id')
    ppt_file_id = request.POST.get('ppt_file_id')

    if not market_folder_id or not ppt_file_id:
        return JsonResponse({'status': 'error', 'message': 'Missing market folder ID or PPT file ID.'}, status=400)

    # Run the processing logic in a new Celery task
    task = process_videos_from_ppt_links.delay(ppt_file_id, market_folder_id)

    return JsonResponse({
        'status': 'success',
        'message': 'Video processing started in the background.',
        'task_id': task.id
    })
