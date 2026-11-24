import json

import pytest

from app import calls
from app.config import settings


@pytest.fixture
def instant(monkeypatch):
    for name in ("queued_secs", "ring_secs", "talk_secs"):
        monkeypatch.setattr(settings, name, (0, 0))


async def _states(r, cid):
    return [json.loads(e)["state"] for e in await r.lrange(calls.events_key(cid), 0, -1)]


async def test_answered_path(fake_redis, instant, monkeypatch):
    monkeypatch.setattr(settings, "answer_prob", 1.0)
    await calls.create_call(fake_redis, "c1", 1, "+100", "+200", {"x": 1})
    await calls.simulate(fake_redis, "c1")

    assert await _states(fake_redis, "c1") == ["QUEUED", "RINGING", "ANSWERED", "COMPLETED"]
    h = await fake_redis.hgetall(calls.call_key("c1"))
    assert h["answered_at"] and h["ended_at"]
    assert await fake_redis.sismember(calls.DIRTY, "c1")


async def test_unanswered_path(fake_redis, instant, monkeypatch):
    monkeypatch.setattr(settings, "answer_prob", 0.0)
    await calls.create_call(fake_redis, "c2", 1, "+100", "+200", None)
    await calls.simulate(fake_redis, "c2")

    assert await _states(fake_redis, "c2") == ["QUEUED", "RINGING", "UNANSWERED", "COMPLETED"]
    out = calls.from_redis(await fake_redis.hgetall(calls.call_key("c2")))
    assert out["duration_seconds"] == 0
    assert out["answered_at"] is None


async def test_seq_goes_up_by_one(fake_redis, instant):
    await calls.create_call(fake_redis, "c3", 1, "a", "b", None)
    await calls.simulate(fake_redis, "c3")
    seqs = [json.loads(e)["seq"] for e in await fake_redis.lrange(calls.events_key("c3"), 0, -1)]
    assert seqs == list(range(1, len(seqs) + 1))
