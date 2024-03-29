import sys

from redis import Redis
from rq import Queue


def main():
    redis_url = sys.argv[1]
    site = sys.argv[2]
    character = sys.argv[3]

    redis = Redis.from_url(redis_url)

    q = Queue("scraper", connection=redis)
    q.enqueue(
        "indexer.scraper.worker.do_indexing_crawl", site, character, job_timeout="6h"
    )
    print("Enqueued scraping job for " + site)


if __name__ == "__main__":
    main()
