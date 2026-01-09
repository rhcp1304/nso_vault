import os
import shutil
from django.shortcuts import render
from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

# Import the service functions
from .services import handle_video_conversion_lifecycle


def webm_to_mp4_page(request):
    """Renders the upload form."""
    return render(request, 'video_converter/index.html')


@csrf_exempt
@require_POST
def process_webm_to_mp4(request):
    """Handles request and streams the converted file back."""
    uploaded_file = request.FILES.get('webm_file')

    if not uploaded_file or not uploaded_file.name.lower().endswith('.webm'):
        return JsonResponse({'status': 'error', 'message': 'Invalid file type.'}, status=400)

    try:
        result, temp_dir, output_path, file_name = handle_video_conversion_lifecycle(uploaded_file)

        if result['status'] == 'success' and os.path.exists(output_path):
            # Create the response
            file_handle = open(output_path, 'rb')
            response = FileResponse(file_handle, content_type='video/mp4')
            response['Content-Disposition'] = f'attachment; filename="{file_name}"'

            # Use the 'close' callback to ensure the temp directory is deleted
            # only AFTER the file is finished streaming.
            def cleanup():
                file_handle.close()
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)

            response.close_callback = cleanup
            return response

        return JsonResponse(result, status=500)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': f'Server Error: {str(e)}'}, status=500)