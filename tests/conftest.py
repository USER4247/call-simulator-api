import fakeredis
import pytest


@pytest.fixture
async def fake_redis():
    r = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield r
    await r.flushall()
    await r.aclose()
