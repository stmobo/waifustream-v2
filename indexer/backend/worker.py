import io
import os
import os.path as osp
import sys
import time

from PIL import Image
from redis import Redis
import requests
from rq import Connection, Worker

from ..structures import QueuedImage, IndexedImage
from ..snowflake import generate_snowflake
from ..index import compute_image_hash, search_index

REDIS = None
APP_REDIS = None
WORKER_ID = None
IMAGE_CACHE_DIR = None


def download_image(url):
    bio = io.BytesIO()
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    for chunk in resp.iter_content(chunk_size=128):
        bio.write(chunk)

    img = Image.open(bio)
    img.load()

    return img, bio


def process_queued_image(queued_image):
    global REDIS, APP_REDIS, IMAGE_CACHE_DIR, WORKER_ID

    if REDIS.sismember(
        "index:sites:" + queued_image.source_site + ":source_ids",
        queued_image.source_id,
    ):
        return

    img, bio = download_image(queued_image.source_url)
    imhash = compute_image_hash(img)

    results = search_index(REDIS, imhash, min_threshold=24)

    if len(results) > 0:
        h = results[0][0]
        imhash_key = b"imhash:" + h

        img_id = REDIS.get(imhash_key)
        img_id = int(img_id.decode("utf-8"))

        indexed_img = IndexedImage.from_queued_image(img_id, h, queued_image)
        indexed_img.save_duplicate_info(REDIS)
    else:
        img_id = generate_snowflake(REDIS, 0, WORKER_ID)
        indexed_img = IndexedImage.from_queued_image(img_id, imhash, queued_image)
        indexed_img.save_to_index(REDIS)

    path = osp.join(IMAGE_CACHE_DIR, indexed_img.cache_filename)

    if not osp.isfile(path):
        with open(path, "wb") as f:
            bio.seek(0)
            f.write(bio.read())
        APP_REDIS.zadd("img_cache:live", {path: int(time.time() * 1000)})

    bio.close()

    print(
        "Processed: {}#{} ==> img_id:{}".format(
            indexed_img.source_site, indexed_img.source_id, indexed_img.img_id
        )
    )

    time.sleep(0.5)  # ratelimit to avoid hitting source servers too hard


def cache_saved_image(img_id):
    global REDIS, APP_REDIS, IMAGE_CACHE_DIR

    indexed_image = IndexedImage.load_from_index(REDIS, img_id)
    path = osp.join(IMAGE_CACHE_DIR, indexed_image.cache_filename)
    if osp.isfile(path):
        return

    with open(path, "wb") as f:
        resp = requests.get(indexed_image.source_url, stream=True)
        resp.raise_for_status()

        for chunk in resp.iter_content(chunk_size=128):
            f.write(chunk)

    APP_REDIS.zadd("img_cache:live", {path: int(time.time() * 1000)})


def main():
    global REDIS, APP_REDIS, WORKER_ID, IMAGE_CACHE_DIR

    redis_url = sys.argv[1]

    REDIS = Redis.from_url(redis_url)
    APP_REDIS = Redis.from_url(sys.argv[2])
    IMAGE_CACHE_DIR = sys.argv[3]
    WORKER_ID = int(sys.argv[4])

    with Connection(REDIS):
        worker = Worker(
            ["backend-index"], name="backend-{:d}-{:d}".format(WORKER_ID, os.getpid())
        )
        worker.work()
