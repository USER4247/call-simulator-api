from app.auth import ApiKey
from app.limiter import Limiter


async def test_concurrent_limit(fake_redis):
    lim = Limiter(fake_redis, hold_secs=60)
    key = ApiKey(1, "t", max_concurrent=3, max_cps=100)

    for i in range(3):
        assert await lim.acquire(key, f"c{i}", now=1000) is None
    assert await lim.acquire(key, "c3", now=1000) == "concurrent"

    # one call ends -> room for one more
    await lim.release(1, "c0")
    assert await lim.acquire(key, "c3", now=1000) is None


async def test_cps_limit_resets_next_second(fake_redis):
    lim = Limiter(fake_redis, hold_secs=60)
    key = ApiKey(1, "t", max_concurrent=100, max_cps=2)

    assert await lim.acquire(key, "a", now=1000.1) is None
    assert await lim.acquire(key, "b", now=1000.5) is None
    assert await lim.acquire(key, "c", now=1000.9) == "cps"
    assert await lim.acquire(key, "c", now=1001.0) is None


async def test_stuck_calls_dont_hold_slots_forever(fake_redis):
    lim = Limiter(fake_redis, hold_secs=60)
    key = ApiKey(1, "t", max_concurrent=1, max_cps=100)

    assert await lim.acquire(key, "stuck", now=1000) is None
    assert await lim.acquire(key, "next", now=1030) == "concurrent"
    # 60s later the stuck one is dropped
    assert await lim.acquire(key, "next", now=1061) is None


async def test_keys_are_seperate(fake_redis):
    lim = Limiter(fake_redis, hold_secs=60)
    a = ApiKey(1, "a", max_concurrent=1, max_cps=100)
    b = ApiKey(2, "b", max_concurrent=1, max_cps=100)

    assert await lim.acquire(a, "x", now=1000) is None
    assert await lim.acquire(b, "y", now=1000) is None
