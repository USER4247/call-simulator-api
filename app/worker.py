"""
Celery tasks. Two jobs:
  - persist_dirty_calls: runs every few secs (beat), copies changed calls from redis -> postgres
  - upload_recording: fired once a call hits COMPLETED
Plain sync code here, celery isnt async anyway.
"""

import logging

import psycopg
import redis

from . import storage
from .calls import COMPLETED, DIRTY, call_key, events_key
from .celery_app import celery_app
from .config import settings

log = logging.getLogger(__name__)
r = redis.Redis.from_url(settings.redis_url, decode_responses=True)

UPSERT = """
insert into calls (id, api_key_id, from_number, to_number, metadata, state,
                   created_at, updated_at, answered_at, ended_at, duration_seconds, recording_url)
values (%(id)s::uuid, %(api_key_id)s, %(from_number)s, %(to_number)s, %(metadata)s::jsonb, %(state)s,
        %(created_at)s::timestamptz, %(updated_at)s::timestamptz, %(answered_at)s::timestamptz,
        %(ended_at)s::timestamptz, %(duration_seconds)s::numeric, %(recording_url)s)
on conflict (id) do update set
    state = excluded.state,
    updated_at = excluded.updated_at,
    answered_at = coalesce(excluded.answered_at, calls.answered_at),
    ended_at = coalesce(excluded.ended_at, calls.ended_at),
    duration_seconds = coalesce(excluded.duration_seconds, calls.duration_seconds),
    recording_url = coalesce(excluded.recording_url, calls.recording_url)
"""


def _row(h):
    opt = lambda k: h.get(k) or None  # noqa
    return {
        "id": h["id"],
        "api_key_id": int(h["api_key_id"]),
        "from_number": h["from_number"],
        "to_number": h["to_number"],
        "metadata": h.get("metadata") or "{}",
        "state": h["state"],
        "created_at": h["created_at"],
        "updated_at": h["updated_at"],
        "answered_at": opt("answered_at"),
        "ended_at": opt("ended_at"),
        "duration_seconds": opt("duration_seconds"),
        "recording_url": opt("recording_url"),
    }


@celery_app.task(name="app.worker.persist_dirty_calls")
def persist_dirty_calls():
    ids = r.spop(DIRTY, 500)
    if not ids:
        return 0

    snaps = []
    for cid in ids:
        h = r.hgetall(call_key(cid))
        if h.get("id"):
            snaps.append(h)

    if snaps:
        try:
            with psycopg.connect(settings.database_url) as conn:
                with conn.cursor() as cur:
                    cur.executemany(UPSERT, [_row(h) for h in snaps])
        except Exception:
            # put them back so the next run picks them up
            r.sadd(DIRTY, *ids)
            raise

    # finished + has its recording = nothing will change anymore. let redis forget it
    # after a while, GET /calls falls back to postgres after that
    for h in snaps:
        if h["state"] == COMPLETED and h.get("recording_url"):
            # r.delete(call_key(h["id"]), events_key(h["id"]))
            # ^ was deleting straight away but then a GET right after completion goes to pg for no reason
            r.expire(call_key(h["id"]), settings.done_call_ttl)
            r.expire(events_key(h["id"]), settings.done_call_ttl)

    log.info("flushed %d calls to postgres", len(snaps))
    return len(snaps)


@celery_app.task(name="app.worker.upload_recording", bind=True, max_retries=5)
def upload_recording(self, cid):
    try:
        url = storage.store_recording(cid)
    except Exception as exc:
        if self.request.retries >= self.max_retries:
            r.incr("metrics:uploads_failed")
            log.error("giving up on recording for %s", cid)
            raise
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)

    if r.exists(call_key(cid)):
        r.hset(call_key(cid), "recording_url", url)
        r.sadd(DIRTY, cid)

    # also write it straight to postgres. if the row isnt there yet (flush hasnt run)
    # this updates 0 rows and thats fine, the upsert above carries the url over
    with psycopg.connect(settings.database_url) as conn:
        conn.execute("update calls set recording_url = %s where id = %s::uuid", (url, cid))

    r.incr("metrics:uploads_completed")
    log.info("recording uploaded for %s -> %s", cid, url)
    return url
