import asyncio

import httpx
import pytest

from app import limiter as limiter_mod
from app import main
from app.auth import ApiKey, require_api_key
from app.config import settings
from app.limiter import Limiter

KEY = ApiKey(1, "test", max_concurrent=3, max_cps=100)


@pytest.fixture
async def client(fake_redis, monkeypatch):
    # long ring so calls stay "active" during the test
    monkeypatch.setattr(settings, "ring_secs", (30, 30))
    # dont actually talk to celery
    sent = []

    async def fake_send(cid):
        sent.append(cid)

    monkeypatch.setattr(main, "send_upload_job", fake_send)

    # ASGITransport doesnt run lifespan, so wire state up by hand (no postgres needed)
    main.app.state.redis = fake_redis
    main.app.state.limiter = Limiter(fake_redis, 60)
    main.app.state.running = set()
    main.app.dependency_overrides[require_api_key] = lambda: KEY

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

    for t in list(main.app.state.running):
        t.cancel()
    await asyncio.gather(*main.app.state.running, return_exceptions=True)
    main.app.dependency_overrides.clear()


async def test_needs_auth(client):
    main.app.dependency_overrides.clear()
    res = await client.post("/calls", json={"from": "1", "to": "2"})
    assert res.status_code == 401
    assert "error" in res.json()


async def test_create_and_get(client):
    res = await client.post("/calls", json={"from": "+911", "to": "+912", "metadata": {"a": 1}})
    assert res.status_code == 201
    body = res.json()
    assert body["state"] == "QUEUED"
    assert body["websocket_url"] == f"ws://test/ws/calls/{body['call_id']}"

    got = (await client.get(f"/calls/{body['call_id']}")).json()
    assert got["from"] == "+911"
    assert got["metadata"] == {"a": 1}


async def test_concurrent_limit_returns_429(client):
    for _ in range(3):
        assert (await client.post("/calls", json={"from": "1", "to": "2"})).status_code == 201
    res = await client.post("/calls", json={"from": "1", "to": "2"})
    assert res.status_code == 429
    assert res.json() == {"error": "Rate limit exceeded"}


async def test_cps_limit_returns_429(client, monkeypatch):
    main.app.dependency_overrides[require_api_key] = lambda: ApiKey(1, "t", 100, 2)
    monkeypatch.setattr(limiter_mod.time, "time", lambda: 5000.0)  # freeze the second
    codes = [(await client.post("/calls", json={"from": "1", "to": "2"})).status_code for _ in range(3)]
    assert codes == [201, 201, 429]


async def test_cant_see_other_keys_calls(client):
    cid = (await client.post("/calls", json={"from": "1", "to": "2"})).json()["call_id"]
    main.app.dependency_overrides[require_api_key] = lambda: ApiKey(2, "other", 3, 2)
    assert (await client.get(f"/calls/{cid}")).status_code == 404


async def test_bad_body(client):
    res = await client.post("/calls", json={"from": "1"})
    assert res.status_code == 422
    assert res.json()["error"] == "Invalid request"


async def test_metrics(client):
    await client.post("/calls", json={"from": "1", "to": "2"})
    m = (await client.get("/metrics")).json()
    assert m["calls_total"] == 1
    assert m["calls_active"] == 1

    prom = (await client.get("/metrics/prometheus")).text
    assert "mockcomms_calls_total 1" in prom
