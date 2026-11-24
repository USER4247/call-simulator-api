import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import calls, db
from .auth import ApiKey, lookup, parse_bearer, require_api_key
from .celery_app import celery_app
from .config import settings
from .limiter import Limiter

log = logging.getLogger("mockcomms")
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app):
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.limiter = Limiter(app.state.redis, settings.max_call_secs)
    app.state.running = set()  # keep refs to call tasks or asyncio may GC them mid-call
    await db.pool.open(wait=True, timeout=30)
    try:
        yield
    finally:
        for t in list(app.state.running):
            t.cancel()
        await asyncio.gather(*app.state.running, return_exceptions=True)
        await db.pool.close()
        await app.state.redis.aclose()


app = FastAPI(title="Mock Comms Service", lifespan=lifespan)


# every error comes back as {"error": "..."} so its the same shape as the rate limit one
@app.exception_handler(StarletteHTTPException)
async def _http_err(request, exc):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code, headers=getattr(exc, "headers", None))


@app.exception_handler(RequestValidationError)
async def _validation_err(request, exc):
    return JSONResponse({"error": "Invalid request", "details": jsonable_encoder(exc.errors())}, status_code=422)


class NewCall(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from", min_length=1, max_length=32)
    to: str = Field(min_length=1, max_length=32)
    metadata: dict[str, Any] | None = None


async def send_upload_job(cid):
    # send_task is blocking network io to the broker, dont want it on the event loop
    await asyncio.to_thread(celery_app.send_task, "app.worker.upload_recording", args=[cid])


async def run_call(app, api_key_id, cid):
    r = app.state.redis
    finished = False
    try:
        await calls.simulate(r, cid)
        finished = True
    except Exception:
        log.exception("call %s blew up during simulation", cid)
    finally:
        # free the slot whatever happened. suppress so a redis hiccup here doesnt hide the real error
        with suppress(Exception):
            await app.state.limiter.release(api_key_id, cid)
            await r.zrem(calls.ACTIVE, cid)

    if finished:
        try:
            await send_upload_job(cid)
        except Exception:
            log.exception("couldnt queue recording upload for %s", cid)


async def load_call(app, cid, api_key_id):
    h = await app.state.redis.hgetall(calls.call_key(cid))
    if h.get("id"):
        # someone elses call -> same as not found, dont leak that it exists
        return calls.from_redis(h) if h.get("api_key_id") == str(api_key_id) else None

    # not in redis anymore (older finished call), fall back to postgres
    async with db.pool.connection() as conn:
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute("select * from calls where id = %s and api_key_id = %s", (cid, api_key_id))
        row = await cur.fetchone()
    return calls.from_row(row) if row else None


def ws_url_for(request, cid):
    scheme = "wss" if request.url.scheme == "https" else "ws"
    return f"{scheme}://{request.url.netloc}/ws/calls/{cid}"


@app.post("/calls", status_code=201)
async def create_call(body: NewCall, request: Request, key: ApiKey = Depends(require_api_key)):
    app_ = request.app
    r = app_.state.redis
    cid = str(uuid.uuid4())

    blocked = await app_.state.limiter.acquire(key, cid)
    if blocked:
        await r.incr("metrics:rate_limited_total")
        return JSONResponse(
            {"error": "Rate limit exceeded"},
            status_code=429,
            headers={"X-RateLimit-Reason": blocked},
        )

    try:
        await calls.create_call(r, cid, key.id, body.from_, body.to, body.metadata)
    except Exception:
        await app_.state.limiter.release(key.id, cid)
        raise

    task = asyncio.create_task(run_call(app_, key.id, cid))
    app_.state.running.add(task)
    task.add_done_callback(app_.state.running.discard)

    return {"call_id": cid, "state": calls.QUEUED, "websocket_url": ws_url_for(request, cid)}


@app.get("/calls/{call_id}")
async def get_call(call_id: uuid.UUID, request: Request, key: ApiKey = Depends(require_api_key)):
    call = await load_call(request.app, str(call_id), key.id)
    if call is None:
        raise HTTPException(404, "Call not found")
    return call


@app.get("/recordings/{call_id}.mp3")
async def get_recording(call_id: uuid.UUID, request: Request, key: ApiKey = Depends(require_api_key)):
    # serves the file out of the mock bucket volume
    call = await load_call(request.app, str(call_id), key.id)
    path = Path(settings.bucket_dir) / "recordings" / f"{call_id}.mp3"
    if call is None or not path.exists():
        raise HTTPException(404, "Recording not found")
    return FileResponse(path, media_type="audio/mpeg")


async def collect_metrics(r):
    now = time.time()
    sec = int(now)
    await r.zremrangebyscore(calls.ACTIVE, "-inf", now)
    total, cps, up_ok, up_fail, limited = await r.mget(
        "metrics:calls_total",
        f"metrics:cps:{sec - 1}",  # last *full* second, the current one is still counting
        "metrics:uploads_completed",
        "metrics:uploads_failed",
        "metrics:rate_limited_total",
    )
    n = lambda v: int(v) if v else 0  # noqa
    return {
        "calls_total": n(total),
        "calls_active": await r.zcard(calls.ACTIVE),
        "cps": n(cps),
        "uploads_completed": n(up_ok),
        "uploads_failed": n(up_fail),
        "rate_limited_total": n(limited),
    }


@app.get("/metrics")
async def metrics(request: Request, key: ApiKey = Depends(require_api_key)):
    return await collect_metrics(request.app.state.redis)


_PROM = [
    # name, type, help
    ("calls_total", "counter", "Calls created since start"),
    ("calls_active", "gauge", "Calls not completed yet"),
    ("cps", "gauge", "New calls in the last full second"),
    ("uploads_completed", "counter", "Recording uploads that finished"),
    ("uploads_failed", "counter", "Recording uploads that gave up after retries"),
    ("rate_limited_total", "counter", "POST /calls rejected by the rate limiter"),
]


@app.get("/metrics/prometheus", response_class=PlainTextResponse)
async def metrics_prom(request: Request, key: ApiKey = Depends(require_api_key)):
    m = await collect_metrics(request.app.state.redis)
    out = []
    for name, typ, help_ in _PROM:
        out.append(f"# HELP mockcomms_{name} {help_}")
        out.append(f"# TYPE mockcomms_{name} {typ}")
        out.append(f"mockcomms_{name} {m[name]}")
    return PlainTextResponse("\n".join(out) + "\n", media_type="text/plain; version=0.0.4")


@app.websocket("/ws/calls/{call_id}")
async def call_ws(ws: WebSocket, call_id: str):
    # browsers cant set headers on a websocket so ?api_key= works too.
    # accept first and then close with a code, otherwise the client just sees a 403 and no reason recieved
    token = parse_bearer(ws.headers.get("authorization")) or ws.query_params.get("api_key")
    key = await lookup(token) if token else None

    await ws.accept()
    if key is None:
        await ws.close(code=4401, reason="Invalid API key")
        return
    try:
        cid = str(uuid.UUID(call_id))
    except ValueError:
        await ws.close(code=4404, reason="Call not found")
        return

    r = ws.app.state.redis
    pubsub = r.pubsub()
    # subscribe BEFORE reading history. otherwise an event can slip in between the two
    # and get lost. duplicates are fine, we skip them by seq
    await pubsub.subscribe(calls.channel(cid))
    try:
        call = await load_call(ws.app, cid, key.id)
        if call is None:
            await ws.close(code=4404, reason="Call not found")
            return

        history = await r.lrange(calls.events_key(cid), 0, -1)
        if not history:
            # already aged out of redis, only thing we have is the final row
            await ws.send_json({"call_id": cid, "state": call["state"], "seq": None, "at": call["updated_at"]})
            await ws.close()
            return

        last = 0
        for raw in history:
            ev = json.loads(raw)
            await ws.send_json(ev)
            last = ev["seq"]
            if ev["state"] == calls.COMPLETED:
                await ws.close()
                return

        async with asyncio.timeout(settings.max_call_secs):
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                ev = json.loads(msg["data"])
                if ev["seq"] <= last:
                    continue
                await ws.send_json(ev)
                last = ev["seq"]
                if ev["state"] == calls.COMPLETED:
                    break
        await ws.close()
    except (WebSocketDisconnect, RuntimeError):
        pass  # client left
    except TimeoutError:
        with suppress(Exception):
            await ws.close(code=1011, reason="call never finished")
    finally:
        with suppress(Exception):
            await pubsub.unsubscribe()
            await pubsub.aclose()
