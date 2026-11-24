# Mock Comms Service

Fake calling platform. You POST a call, it goes through QUEUED -> RINGING -> ANSWERED/UNANSWERED -> COMPLETED on its own with random timings, and you can watch that live over a websocket. When a call finishes a worker "uploads" a recording and puts the url on the call.

Stack: FastAPI (async), Redis for live state + rate limiting, Postgres for persistence, Celery for the background jobs. Everything runs with docker compose.

## Running it

```
docker compose up --build
```

That brings up postgres, redis, a one-shot `migrate` container (creates the tables + 2 dev keys), the api on :8000 and the celery worker.

Dev keys from the migration:

| key | concurrent | cps |
|---|---|---|
| `dev-key-1` | 3 | 2 |
| `dev-key-2` | 20 | 10 |

Limits live in the `api_keys` table so you can change them / add keys with plain sql, the api picks changes up within ~10s:

```
docker compose exec db psql -U calls -c "insert into api_keys (key, name, max_concurrent, max_cps) values ('my-key', 'me', 5, 3);"
```

## API

Every request needs `Authorization: Bearer <key>`. Errors always look like `{"error": "..."}`.

**POST /calls**

```
curl -X POST localhost:8000/calls \
  -H "Authorization: Bearer dev-key-1" -H "Content-Type: application/json" \
  -d '{"from": "+14155550100", "to": "+14155550199", "metadata": {"campaign": "test"}}'
```
```json
{"call_id": "3f0c...", "state": "QUEUED", "websocket_url": "ws://localhost:8000/ws/calls/3f0c..."}
```

Over the limit (either one) you get `429 {"error": "Rate limit exceeded"}`. There's an `X-RateLimit-Reason` header (`concurrent` or `cps`) if you want to know which one.

**WS /ws/calls/{id}**

Connect with the same bearer header, or `?api_key=` since browsers can't set headers on websockets. You get every state change as json:

```json
{"call_id": "3f0c...", "state": "RINGING", "seq": 2, "at": "2026-11-24T08:31:02.118+00:00"}
```

If you connect late you still get the earlier events first (they're replayed from redis), then live ones. Socket closes after COMPLETED.

```
npx wscat -c "ws://localhost:8000/ws/calls/<id>?api_key=dev-key-1"
```

**GET /calls/{id}** - current state, timestamps, duration, recording_url (null until the upload is done). You only see your own key's calls.

**GET /metrics** - json: calls_total, calls_active, cps (last full second), uploads_completed, uploads_failed, rate_limited_total.

**GET /metrics/prometheus** - same numbers in prometheus text format.

**GET /recordings/{id}.mp3** - the uploaded mock recording (local bucket mode).

There's also a script that does all of the above in one go against a running stack:

```
pip install httpx websockets
python scripts/smoke_test.py
```

## How it works

**Call state** is a redis hash per call plus a list of its events. Each state change does hset + rpush to the event list + publish on a pubsub channel in one MULTI, and marks the call "dirty". The websocket handler subscribes to the channel first, then replays the list, and skips anything with a seq it already sent. That ordering is on purpose, other way round you can lose an event that lands in between.

The state machine itself runs as an asyncio task inside the api process (just sleeps between transitions). Since all state + events go through redis, it doesn't matter which api instance the websocket hits.

**Rate limiting** is one lua script so the check and the reserve are atomic. Concurrent calls are a sorted set per key, score = expiry time. The expiry is only there so a call that never finishes (api crashed mid call) doesn't eat a slot forever. CPS is a fixed 1 second window counter.

**Persistence**: celery beat runs every 5s, pops the dirty ids and upserts those calls into postgres. GET /calls reads redis first, postgres if redis doesn't have it anymore. Finished calls that already have their recording get a 1h TTL in redis after being flushed.

**Recording upload**: when a call hits COMPLETED the api queues `upload_recording` (send_task in a thread so the event loop isn't blocked). The worker copies `assets/mock_recording.mp3` into the bucket, sets `recording_url` on the call in redis + postgres, retries with backoff if it fails.

The "bucket" is a docker volume shared by the worker and api (task said S3 or a mock bucket, went with the mock so everything runs offline with no cloud account). Swapping in S3 would only touch `app/storage.py`.

**Migrations**: `migrations/*.sql`, applied in order by `python -m app.migrate` (tracked in `schema_migrations`, advisory lock so parallel starts don't collide). Didn't feel like alembic was worth it for two tables.

## Config

All env vars, see `app/config.py`. The useful ones:

| var | default | |
|---|---|---|
| QUEUED_SECONDS | 0.5,1.5 | min,max time in QUEUED |
| RING_SECONDS | 2,5 | |
| TALK_SECONDS | 3,8 | only for answered calls |
| ANSWER_PROBABILITY | 0.6 | |
| PERSIST_INTERVAL_SECONDS | 5 | redis -> postgres flush |
| MAX_CALL_SECONDS | 120 | safety expiry for concurrency slots |
| LOCAL_BUCKET_DIR | /data/bucket | where recordings go |

## Tests

```
pip install -r requirements-dev.txt
pytest
```

Uses fakeredis (with lua) so no services needed. Covers the limiter (concurrent, cps, stuck call expiry), both call paths, and the api endpoints incl. the 429s.

## Layout

```
app/
  main.py        routes + websocket
  calls.py       call state in redis + the simulation
  limiter.py     the lua rate limiter
  auth.py        bearer key lookup (cached 10s)
  worker.py      celery tasks: flush to postgres, upload recording
  storage.py     mock bucket
  migrate.py     tiny sql migration runner
migrations/      schema
scripts/         smoke test
tests/
```

## Things I'd do differently with more time

- If an api instance dies mid call, that call just stops where it was (its slot frees itself after MAX_CALL_SECONDS but the state stays stuck). Fix would be moving the state machine into the worker using delayed tasks, or a sweeper that finishes orphaned calls.
- Fixed window cps lets through up to 2x the limit right on a second boundary. Sliding window would fix it, didn't seem worth it here.
- API keys are stored plain in the db. Should be hashed.
- calls_active in /metrics is global, not per key.
- Beat runs inside the worker (`-B`). Fine for one worker, needs to be its own container if you scale workers.
