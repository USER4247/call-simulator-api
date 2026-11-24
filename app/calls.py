"""
Call state lives in redis. One hash per call + a list of every event it went through
(the list is what lets a late websocket client catch up on what it missed).

Flow is always QUEUED -> RINGING -> ANSWERED/UNANSWERED -> COMPLETED
"""

import asyncio
import json
import random
import time
from datetime import datetime, timezone

from .config import settings

QUEUED = "QUEUED"
RINGING = "RINGING"
ANSWERED = "ANSWERED"
UNANSWERED = "UNANSWERED"
COMPLETED = "COMPLETED"

ACTIVE = "calls:active"  # zset, global active calls (for /metrics)
DIRTY = "calls:dirty"  # set of call ids that changed since the last flush to postgres


def call_key(cid):
    return f"call:{cid}"


def events_key(cid):
    return f"call:{cid}:events"


def channel(cid):
    return f"call-events:{cid}"


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def _event(cid, state, seq, ts):
    return json.dumps({"call_id": cid, "state": state, "seq": seq, "at": ts})


async def create_call(r, cid, api_key_id, frm, to, metadata):
    ts = utcnow()
    now = time.time()
    sec = int(now)
    ev = _event(cid, QUEUED, 1, ts)

    async with r.pipeline(transaction=True) as p:
        p.hset(call_key(cid), mapping={
            "id": cid,
            "api_key_id": str(api_key_id),
            "from_number": frm,
            "to_number": to,
            "metadata": json.dumps(metadata or {}),
            "state": QUEUED,
            "seq": 1,
            "created_at": ts,
            "updated_at": ts,
        })
        p.rpush(events_key(cid), ev)
        p.publish(channel(cid), ev)
        p.sadd(DIRTY, cid)
        p.zadd(ACTIVE, {cid: now + settings.max_call_secs})
        p.incr("metrics:calls_total")
        p.incr(f"metrics:cps:{sec}")
        p.expire(f"metrics:cps:{sec}", 5)
        await p.execute()


async def set_state(r, cid, state, extra=None):
    ts = utcnow()
    seq = await r.hincrby(call_key(cid), "seq", 1)

    fields = {"state": state, "updated_at": ts}
    if state == ANSWERED:
        fields["answered_at"] = ts
    elif state == COMPLETED:
        fields["ended_at"] = ts
    if extra:
        fields.update(extra)

    ev = _event(cid, state, seq, ts)
    async with r.pipeline(transaction=True) as p:
        p.hset(call_key(cid), mapping=fields)
        p.rpush(events_key(cid), ev)
        p.publish(channel(cid), ev)
        p.sadd(DIRTY, cid)
        await p.execute()


def _wait(rng):
    return asyncio.sleep(random.uniform(*rng))


async def simulate(r, cid):
    """walks one call through its states. runs as a background task inside the api"""
    await _wait(settings.queued_secs)
    await set_state(r, cid, RINGING)
    await _wait(settings.ring_secs)

    if random.random() < settings.answer_prob:
        await set_state(r, cid, ANSWERED)
        talk = random.uniform(*settings.talk_secs)
        await asyncio.sleep(talk)
        await set_state(r, cid, COMPLETED, {"duration_seconds": f"{talk:.2f}"})
    else:
        await set_state(r, cid, UNANSWERED)
        # await asyncio.sleep(0.5)  # felt weird having a gap here, nobody is on the line anyway
        await set_state(r, cid, COMPLETED, {"duration_seconds": "0"})


# ---- shaping stuff for the api response ----

def _opt(v):
    return v or None


def from_redis(h):
    return {
        "call_id": h["id"],
        "from": h["from_number"],
        "to": h["to_number"],
        "metadata": json.loads(h.get("metadata") or "{}"),
        "state": h["state"],
        "created_at": h["created_at"],
        "updated_at": h["updated_at"],
        "answered_at": _opt(h.get("answered_at")),
        "ended_at": _opt(h.get("ended_at")),
        "duration_seconds": float(h["duration_seconds"]) if h.get("duration_seconds") else None,
        "recording_url": _opt(h.get("recording_url")),
    }


def from_row(row):
    iso = lambda d: d.isoformat() if d else None  # noqa
    return {
        "call_id": str(row["id"]),
        "from": row["from_number"],
        "to": row["to_number"],
        "metadata": row["metadata"],
        "state": row["state"],
        "created_at": iso(row["created_at"]),
        "updated_at": iso(row["updated_at"]),
        "answered_at": iso(row["answered_at"]),
        "ended_at": iso(row["ended_at"]),
        "duration_seconds": float(row["duration_seconds"]) if row["duration_seconds"] is not None else None,
        "recording_url": row["recording_url"],
    }
