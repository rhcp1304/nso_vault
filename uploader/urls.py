from django.urls import path
from . import views

urlpatterns = [
    path('', views.upload_page, name='upload_page'),
    path('upload/', views.process_upload, name='process_upload'),
]