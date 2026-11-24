import time

# Both checks have to happen atomically otherwise two requests landing at the same time
# can both see "2 active" and both get in. Lua runs as one op in redis so that solves it,
# tried doing it with a pipeline first but thats not enough becuase the check needs the read result
#
# async with self.r.pipeline() as p:
#     p.zcard(active_key(key.id))
#     p.incr(cps_key)
#     n_active, n_cps = await p.execute()
#
# active calls = sorted set, score is when the slot expires (safety net, see MAX_CALL_SECONDS)
# cps = plain counter per key per second, expires on its own
_LUA = """
local active = KEYS[1]
local cps = KEYS[2]
local now = tonumber(ARGV[1])
local max_conc = tonumber(ARGV[2])
local max_cps = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', active, '-inf', now)
if redis.call('ZCARD', active) >= max_conc then
  return 1
end

local n = redis.call('INCR', cps)
if n == 1 then
  redis.call('EXPIRE', cps, 2)
end
if n > max_cps then
  return 2
end

redis.call('ZADD', active, now + tonumber(ARGV[5]), ARGV[4])
return 0
"""

_REASONS = {0: None, 1: "concurrent", 2: "cps"}


def active_key(api_key_id):
    return f"limits:active:{api_key_id}"


class Limiter:
    def __init__(self, redis, hold_secs):
        self.r = redis
        self.hold_secs = hold_secs
        self._script = redis.register_script(_LUA)

    async def acquire(self, key, call_id, now=None):
        """returns None if the call can go ahead, otherwise 'concurrent' or 'cps'"""
        now = now if now is not None else time.time()
        res = await self._script(
            keys=[active_key(key.id), f"limits:cps:{key.id}:{int(now)}"],
            args=[now, key.max_concurrent, key.max_cps, call_id, self.hold_secs],
        )
        return _REASONS[int(res)]

    async def release(self, api_key_id, call_id):
        await self.r.zrem(active_key(api_key_id), call_id)
