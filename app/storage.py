import os
import shutil
from pathlib import Path

from .config import settings


def store_recording(cid):
    """copies the mock mp3 into the bucket, returns the url to it.

    "bucket" = a docker volume shared by worker + api, api serves it at /recordings/..
    task said s3 *or* mock bucket, went with mock so the whole thing runs offline
    """
    obj = f"recordings/{cid}.mp3"
    dest = Path(settings.bucket_dir) / obj
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(".part")
    shutil.copyfile(settings.mock_mp3, tmp)
    os.replace(tmp, dest)  # so the api never serves a half written file
    return f"{settings.public_base_url}/{obj}"
