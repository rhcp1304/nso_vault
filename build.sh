#!/usr/bin/env bash
set -o errexit  # exit on error

python -m pip install -r requirements.txt
python manage.py collectstatic --noinput