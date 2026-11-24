from celery import Celery

from .config import settings

# api imports this too (only to send tasks by name), so keep it light - tasks live in worker.py
celery_app = Celery("mockcomms", broker=settings.broker_url, include=["app.worker"])

celery_app.conf.update(
    task_ignore_result=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    beat_schedule={
        "flush-calls-to-postgres": {
            "task": "app.worker.persist_dirty_calls",
            "schedule": settings.persist_every,
        },
    },
)
