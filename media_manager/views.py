import os
import shutil
import tempfile
from django.shortcuts import render
from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

# Import our new services
from . import services
from .tasks import process_video_task  # Celery task


# --- Pages ---
def webm_to_mp4_page(request): return render(request, 'video_converter/index.html')


def upload_page(request): return render(request, 'uploader/index.html')


def video_processor_page(request): return render(request, 'video_processor/index.html')


# --- Actions ---

@csrf_exempt
@require_POST
def process_webm_to_mp4(request):
    uploaded_file = request.FILES.get('webm_file')
    temp_dir = tempfile.mkdtemp()
    in_path = os.path.join(temp_dir, uploaded_file.name)
    out_name = f"{os.path.splitext(uploaded_file.name)[0]}.mp4"
    out_path = os.path.join(temp_dir, out_name)

    with open(in_path, 'wb+') as f:
        for chunk in uploaded_file.chunks(): f.write(chunk)

    success, msg = services.convert_video_to_mp4(in_path, out_path)
    if success:
        return FileResponse(open(out_path, 'rb'), content_type='video/mp4')
    return JsonResponse({'error': msg}, status=500)


@csrf_exempt
@require_POST
def process_upload(request):
    uploaded_file = request.FILES.get('ppt_file')
    parent_id = request.POST.get('parent_folder_id')
    temp_dir = tempfile.mkdtemp()
    temp_path = os.path.join(temp_dir, uploaded_file.name)

    try:
        with open(temp_path, 'wb+') as f:
            for chunk in uploaded_file.chunks(): f.write(chunk)

        market, zone = services.extract_ppt_metadata(temp_path)
        if not market: return JsonResponse({'error': 'Metadata extraction failed'}, status=400)

        drive = services.get_drive_service()
        target_id = services.find_or_create_folder(drive, zone, parent_id) if zone else parent_id
        market_id = services.find_or_create_folder(drive, market, target_id)

        # Upload
        media = services.MediaFileUpload(temp_path, resumable=True)
        drive.files().create(body={'name': uploaded_file.name, 'parents': [market_id]}, media_body=media).execute()

        return JsonResponse({'message': 'Success'})
    finally:
        shutil.rmtree(temp_dir)


@csrf_exempt
@require_POST
def start_video_processing(request):
    drive_id = request.POST.get('google_drive_folder_id')
    task = process_video_task.delay(drive_id)
    return JsonResponse({'task_id': task.id})