import sys
import os

from redis import Redis
from rq import Connection, Worker

from .danbooru import ops as danbooru_ops
from .gelbooru import ops as gelbooru_ops


REDIS = None
WORKER_ID = None

site_ops = {"danbooru": danbooru_ops, "gelbooru": gelbooru_ops}


def do_indexing_crawl(site, character):
    global REDIS

    site_ops[site]["index"](REDIS, character)


def do_associate_character(site, character, tags):
    global REDIS

    site_ops[site]["associate"](REDIS, character, tags)


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
