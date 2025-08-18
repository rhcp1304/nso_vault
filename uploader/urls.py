# your_project/urls.py

from django.contrib import admin
from django.urls import path, include
from uploader import views as uploader_views

urlpatterns = [
    path('admin/', admin.site.urls),

    # URLs for the PPT Uploader App
    path('', uploader_views.upload_page, name='upload_page'),
    path('upload/', uploader_views.process_upload, name='process_upload'),
]