import sys

from redis import Redis
from rq import Connection, Worker

from .danbooru import index_character as index_character_danbooru


REDIS = None
WORKER_ID = None


def do_indexing_crawl(site, character):
    global REDIS

    if site == "danbooru":
        index_character_danbooru(REDIS, character)


def main():
    global REDIS, WORKER_ID

    redis_url = sys.argv[1]

    REDIS = Redis.from_url(redis_url)
    WORKER_ID = int(sys.argv[2])

    with Connection(REDIS):
        worker = Worker(["scraper"], name="scraper-{:d}".format(WORKER_ID))
        worker.work()
