import subprocess
import os
import shutil
import tempfile


def convert_video_file_fast(input_path, output_path):
    """
    Converts a video from WEBM to MP4 using ffmpeg with h264_videotoolbox.
    Returns: A dictionary with 'status' and 'message'.
    """
    command = [
        'ffmpeg',
        '-i', input_path,
        '-c:v', 'h264_videotoolbox',
        '-b:v', '5000k',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-y',
        output_path
    ]

    try:
        subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return {'status': 'success', 'message': 'Conversion successful.'}

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode()
        return {'status': 'error', 'message': f"FFmpeg Error: {error_output}"}
    except FileNotFoundError:
        return {'status': 'error', 'message': "FFmpeg not found in system PATH."}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


def handle_video_conversion_lifecycle(uploaded_file):
    """
    Handles the file system operations: creating temp files,
    triggering conversion, and returning the path.
    """
    temp_dir = tempfile.mkdtemp()
    input_file_path = os.path.join(temp_dir, uploaded_file.name)

    base_name, _ = os.path.splitext(uploaded_file.name)
    output_file_name = f"{base_name}.mp4"
    output_file_path = os.path.join(temp_dir, output_file_name)

    # Save uploaded file
    with open(input_file_path, 'wb+') as f:
        for chunk in uploaded_file.chunks():
            f.write(chunk)

    result = convert_video_file_fast(input_file_path, output_file_path)

    return result, temp_dir, output_file_path, output_file_name