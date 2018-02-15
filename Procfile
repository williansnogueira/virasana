web: gunicorn wsgi:app
worker: celery -E -A virasana.workers.raspadir.celery worker --loglevel=info