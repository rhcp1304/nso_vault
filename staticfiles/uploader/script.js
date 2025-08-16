document.addEventListener('DOMContentLoaded', () => {
    const uploadForm = document.getElementById('uploadForm');
    const pptFile = document.getElementById('pptFile');
    const parentFolderId = document.getElementById('parentFolderId');
    const uploadBtn = document.getElementById('uploadBtn');
    const buttonText = document.getElementById('buttonText');
    const spinner = document.querySelector('.spinner');
    const statusDiv = document.getElementById('status');

    uploadForm.addEventListener('submit', async (e) => {
        e.preventDefault();

        const file = pptFile.files[0];
        const folderId = parentFolderId.value.trim();

        if (!file || !folderId) {
            updateStatus('Please select a file and enter a valid folder ID.', 'error');
            return;
        }

        uploadBtn.disabled = true;
        buttonText.textContent = 'Uploading...';
        spinner.hidden = false;
        statusDiv.hidden = true;

        const formData = new FormData();
        formData.append('ppt_file', file);
        formData.append('parent_folder_id', folderId);

        try {
            const response = await fetch('/uploader/upload/', {
                method: 'POST',
                body: formData,
            });

            const result = await response.json();

            if (response.ok) {
                updateStatus(result.message || 'File uploaded and organized successfully!', 'success');
            } else {
                updateStatus(`Upload failed: ${result.error || 'An unknown error occurred.'}`, 'error');
            }
        } catch (error) {
            console.error('Fetch error:', error);
            updateStatus(`An unexpected error occurred: ${error.message}.`, 'error');
        } finally {
            uploadBtn.disabled = false;
            buttonText.textContent = 'Upload and Organize';
            spinner.hidden = true;
        }
    });

    function updateStatus(message, type) {
        statusDiv.textContent = message;
        statusDiv.className = `status-message ${type}`;
        statusDiv.hidden = false;
    }
});