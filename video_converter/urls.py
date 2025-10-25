from django.urls import path
from . import views

urlpatterns = [
    path('', views.webm_to_mp4_page, name='webm_to_mp4_page'),
    path('process/', views.process_webm_to_mp4, name='process_webm_to_mp4'),
]