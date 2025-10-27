from django.urls import path
from . import views

urlpatterns = [
    # Maps the root URL to the trigger page
    path('', views.trigger_email_check_page, name='trigger_page'),
    # Maps the POST action to the processing function
    path('check-email/', views.check_email_and_upload, name='check_email'),
]