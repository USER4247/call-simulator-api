import time
from dataclasses import dataclass

from fastapi import Header, HTTPException

from . import db


@dataclass(frozen=True)
class ApiKey:
    id: int
    name: str
    max_concurrent: int
    max_cps: int


# tiny in-process cache so we arent hitting postgres on every single request.
# 10s means a limit change in the db shows up quickly enough
_CACHE_TTL = 10.0
_cache = {}


async def lookup(token):
    hit = _cache.get(token)
    if hit and time.monotonic() - hit[0] < _CACHE_TTL:
        return hit[1]

    async with db.pool.connection() as conn:
        cur = await conn.execute(
            "select id, name, max_concurrent, max_cps from api_keys where key = %s and enabled",
            (token,),
        )
        row = await cur.fetchone()

    if not row:
        # dont cache misses, someone spamming random keys would just grow the dict
        _cache.pop(token, None)
        return None

    key = ApiKey(*row)
    _cache[token] = (time.monotonic(), key)
    return key


def parse_bearer(header_val):
    if not header_val:
        return None
    scheme, _, token = header_val.partition(" ")
    token = token.strip()
    if scheme.lower() != "bearer" or not token:
        return None
    return token


async def require_api_key(authorization: str | None = Header(default=None)) -> ApiKey:
    token = parse_bearer(authorization)
    if token is None:
        raise HTTPException(401, "Missing or malformed Authorization header")
    key = await lookup(token)
    if key is None:
        raise HTTPException(401, "Invalid API key")
    return key
