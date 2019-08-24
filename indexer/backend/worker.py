import io
import os
import sys
import time

from PIL import Image
from redis import Redis
import requests
from rq import Connection, Worker

from ..structures import QueuedImage, IndexedImage
from ..snowflake import generate_snowflake
from ..index import compute_image_hash, exists_in_index

REDIS = None
WORKER_ID = None


def download_image(url):
    with io.BytesIO() as bio:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()

        for chunk in resp.iter_content(chunk_size=128):
            bio.write(chunk)

        img = Image.open(bio)
        img.load()

        return img


def process_queued_image(queued_image):
    global REDIS, WORKER_ID

    img = download_image(queued_image.source_url)
    imhash = compute_image_hash(img)

    if exists_in_index(REDIS, imhash, min_threshold=24):
        return

    img_id = generate_snowflake(REDIS, 0, WORKER_ID)

    indexed_img = IndexedImage.from_queued_image(img_id, imhash, queued_image)
    indexed_img.save_to_index(REDIS)

    print(
        "Processed: {}#{} ==> img_id:{}".format(
            indexed_img.source_site, indexed_img.source_id, indexed_img.img_id
        )
    )

    time.sleep(0.5)  # ratelimit to avoid hitting source servers too hard


def main():
    global REDIS, WORKER_ID

    redis_url = sys.argv[1]

    REDIS = Redis.from_url(redis_url)
    WORKER_ID = int(sys.argv[2])

    with Connection(REDIS):
        worker = Worker(
            ["backend-index"], name="backend-{:d}-{:d}".format(WORKER_ID, os.getpid())
        )
        worker.work()
