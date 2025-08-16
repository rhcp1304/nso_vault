                                                        #!/usr/bin/env bash
set -o errexit

/usr/local/bin/python3 -m pip install -r requirements.txt
/usr/local/bin/python3 manage.py collectstatic --noinput