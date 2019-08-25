import sys

from redis import Redis
from rq import Queue

from indexer.scraper import danbooru, gelbooru


def main():
    redis_url = sys.argv[1]
    redis = Redis.from_url(redis_url)

    for tag_id, _ in redis.zscan_iter("index:tags:all"):
        tag_id = tag_id.decode("utf-8")

        print("Processing tag: " + tag_id)
        tag, source_site = tag_id.rsplit("@", 1)

        data = {}

        for img_id, score in redis.zscan_iter("index:tags:" + source_site + ":" + tag):
            img_id = int(img_id.decode("utf-8"))
            data[img_id] = int(score)

        redis.zadd("index:tags:merged:" + tag, data)
        redis.zadd("index:tags:merged:" + tag_id, data)


if __name__ == "__main__":
    main()
