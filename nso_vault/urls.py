from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('video_processor/', include('video_processor.urls')),
    path('drive-upload', include('drive_uploader.urls')),
]