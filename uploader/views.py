import os
import tempfile
import shutil
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .utils import main_processor


# --- Django Views ---
def upload_page(request):
    """
    Renders the HTML form for the file upload.
    """
    return render(request, 'uploader/index.html')


@csrf_exempt
@require_POST
def process_upload(request):
    """
    Handles the POST request from the form, processes the file,
    and returns a JSON response with the new folder ID.
    """
    try:
        uploaded_file = request.FILES.get('ppt_file')
        parent_folder_id = request.POST.get('parent_folder_id')

        if not uploaded_file or not parent_folder_id:
            return JsonResponse({'error': 'Missing file or parent folder ID.'}, status=400)

        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, uploaded_file.name)

        with open(temp_file_path, 'wb+') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)

        # Call the new main processor from the utility file
        result = main_processor(temp_file_path, parent_folder_id)

        # Clean up the temporary directory
        shutil.rmtree(temp_dir)

        if 'error' in result:
            return JsonResponse({'status': 'error', 'message': result['error']}, status=500)

        # Return the folder ID for the front-end to use
        return JsonResponse({
            'status': 'success',
            'message': 'File uploaded and organized successfully!',
            'market_folder_id': result['market_folder_id']
        }, status=200)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
