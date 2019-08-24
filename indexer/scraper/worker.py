import sys
import os

from redis import Redis
from rq import Connection, Worker

from .danbooru import index_character as index_character_danbooru
from .gelbooru import index_character as index_character_gelbooru


REDIS = None
WORKER_ID = None


def do_indexing_crawl(site, character):
    global REDIS

    if site == "danbooru":
        index_character_danbooru(REDIS, character)
    elif site == "gelbooru":
        index_character_gelbooru(REDIS, character)


def main():
    global REDIS, WORKER_ID

    redis_url = sys.argv[1]

    REDIS = Redis.from_url(redis_url)
    WORKER_ID = int(sys.argv[2])

    with Connection(REDIS):
        worker = Worker(
            ["scraper"], name="scraper-{:d}-{:d}".format(WORKER_ID, os.getpid())
        )
        worker.work()
