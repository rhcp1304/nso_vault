import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nso_vault.settings')
app = Celery('nso_vault')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()