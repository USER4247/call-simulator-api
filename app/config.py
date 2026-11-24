import os
from pathlib import Path


def _env(name, default):
    return os.getenv(name, default)


def _range(name, default):
    # "min,max" -> (min, max). e.g RING_SECONDS=2,5
    lo, hi = (float(x) for x in _env(name, default).split(","))
    return lo, hi


class Settings:
    def __init__(self):
        self.redis_url = _env("REDIS_URL", "redis://localhost:6379/0")
        self.broker_url = _env("CELERY_BROKER_URL", "redis://localhost:6379/1")
        self.database_url = _env("DATABASE_URL", "postgresql://calls:calls@localhost:5432/calls")

        # worker has no request object so it needs this to build the recording url
        self.public_base_url = _env("PUBLIC_BASE_URL", "http://localhost:8000").rstrip("/")

        # timings for the fake call, random value picked inside each range (seconds)
        self.queued_secs = _range("QUEUED_SECONDS", "0.5,1.5")
        self.ring_secs = _range("RING_SECONDS", "2,5")
        self.talk_secs = _range("TALK_SECONDS", "3,8")
        self.answer_prob = float(_env("ANSWER_PROBABILITY", "0.6"))

        # if the api crashes mid call the call would hold a concurrency slot forever.
        # after this many secs we just let it go
        self.max_call_secs = int(_env("MAX_CALL_SECONDS", "120"))

        self.persist_every = float(_env("PERSIST_INTERVAL_SECONDS", "5"))
        self.done_call_ttl = int(_env("COMPLETED_CALL_TTL_SECONDS", "3600"))

        self.bucket_dir = _env("LOCAL_BUCKET_DIR", "/data/bucket")

        self.mock_mp3 = _env(
            "MOCK_RECORDING_PATH",
            str(Path(__file__).resolve().parent.parent / "assets" / "mock_recording.mp3"),
        )


settings = Settings()
