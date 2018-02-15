web: gunicorn wsgi:app
worker: celery -E -A virasana.views.celery worker --loglevel=info