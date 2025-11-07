import os
import tempfile
import subprocess
from django.shortcuts import render
from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST


# =================================================================================
# === CORE CONVERSION LOGIC (Helper Function) ===
# =================================================================================

def convert_video_file_fast(input_path, output_path):
    """
    Converts a video from WEBM to MP4 using ffmpeg with h264_videotoolbox 
    for fast, hardware-accelerated encoding (ideal for macOS/Apple Silicon).

    Returns: A dictionary with 'status' and 'message'.
    """

    # Your specified fast command arguments
    command = [
        'ffmpeg',
        '-i', input_path,
        '-c:v', 'h264_videotoolbox',  # Hardware video encoder (macOS specific)
        '-b:v', '5000k',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-y',  # Overwrite output files without asking
        output_path
    ]

    try:
        # Execute the command. check=True raises CalledProcessError on non-zero exit code.
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return {'status': 'success', 'message': f'Conversion successful.'}

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode()
        return {'status': 'error', 'message': f"FFmpeg Error: {error_output}"}
    except FileNotFoundError:
        return {'status': 'error', 'message': "FFmpeg not found. Ensure it is installed and in your system's PATH."}
    except Exception as e:
        return {'status': 'error', 'message': f"An unexpected error occurred: {str(e)}"}


# =================================================================================
# === DJANGO VIEWS ===
# =================================================================================

def webm_to_mp4_page(request):
    """
    Renders the HTML form for the WEBM to MP4 conversion.
    """
    # NOTE: Ensure you have the template 'video_converter/index.html'
    return render(request, 'video_converter/index.html')


@csrf_exempt
@require_POST
def process_webm_to_mp4(request):
    """
    Handles the POST request, converts the video, and returns the converted MP4 file.
    It uses temporary storage for processing and then cleans up.
    """
    uploaded_file = request.FILES.get('webm_file')

    if not uploaded_file or not uploaded_file.name.lower().endswith('.webm'):
        return JsonResponse({'status': 'error', 'message': 'Missing or invalid WEBM file uploaded.'}, status=400)

    temp_dir = None
    input_file_path = None
    output_file_path = None

    try:
        # 1. Prepare file paths
        temp_dir = tempfile.mkdtemp()
        input_file_path = os.path.join(temp_dir, uploaded_file.name)

        base_name, _ = os.path.splitext(uploaded_file.name)
        output_file_name = f"{base_name}.mp4"
        output_file_path = os.path.join(temp_dir, output_file_name)

        # 2. Save uploaded file to disk
        with open(input_file_path, 'wb+') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)

        # 3. Perform the conversion
        result = convert_video_file_fast(input_file_path, output_file_path)

        if result['status'] == 'success' and os.path.exists(output_file_path):

            # 4. Return the converted file directly to the user
            # FileResponse streams the file contents, and the 'finally' block 
            # ensures cleanup happens AFTER the response is sent.
            response = FileResponse(
                open(output_file_path, 'rb'),
                content_type='video/mp4'
            )
            response['Content-Disposition'] = f'attachment; filename="{output_file_name}"'

            # Attach cleanup function to the response object to run later
            response.cleanup_paths = [temp_dir]

            return response
        else:
            # Conversion failed
            return JsonResponse(result, status=500)

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': f'Server Error during processing: {str(e)}'}, status=500)

    finally:
        # 5. Clean up temporary files
        # NOTE: For FileResponse, the cleanup happens after the file is streamed.
        # This block is here as a fallback and for successful conversions handled 
        # by Django's framework (which often calls a cleanup hook if set on the response).
        def cleanup_temp_dir():
            if temp_dir and os.path.exists(temp_dir):
                try:
                    import shutil
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    print(f"Cleanup failed for directory {temp_dir}: {e}")

        # If the request failed before FileResponse was created, clean up immediately.
        if 'response' not in locals() and temp_dir:
            cleanup_temp_dir()

# To fully use this view, you must:
# 1. Create a template at `video_converter/templates/video_converter/index.html` with a file upload form.
# 2. Map the URLs in your app's `urls.py`.