from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('uploader/', include('uploader.urls')),
    path('video_processor/', include('video_processor.urls')),  # This is the new line

]