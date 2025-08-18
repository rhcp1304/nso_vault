# video_processor/urls.py

from django.urls import path
from . import views

urlpatterns = [
    path('', views.video_processor_page, name='video_processor_page'),
    path('start/', views.start_video_processing, name='start_video_processing'),
]