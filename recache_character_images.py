import sys

from redis import Redis
from rq import Queue


def main():
    redis_url = sys.argv[1]
    character = sys.argv[2]

    redis = Redis.from_url(redis_url)

    q = Queue("backend-index", connection=redis)

    for img_id, _ in redis.zscan_iter("index:characters:" + character):
        img_id = img_id.decode("utf-8")
        q.enqueue(
            "indexer.backend.worker.cache_saved_image", img_id, job_timeout="6h"
        )

        print("Queued recache job for {}".format(img_id))


if __name__ == "__main__":
    main()
