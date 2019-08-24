import io
import sys

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


def process_queued_image(queued_image, min_threshold):
    global REDIS, WORKER_ID

    img = download_image(queued_image.source_url)
    imhash = compute_image_hash(img)

    if exists_in_index(REDIS, imhash, min_threshold=min_threshold):
        return

    img_id = generate_snowflake(REDIS, 0, WORKER_ID)

    indexed_img = IndexedImage.from_queued_image(img_id, imhash, queued_image)
    indexed_img.save_to_index(REDIS)


def main():
    global REDIS, WORKER_ID

    redis_url = sys.argv[1]

    REDIS = Redis(redis_url)
    WORKER_ID = int(sys.argv[2])

    with Connection(REDIS):
        worker = Worker(["backend-index"])
        worker.work()
