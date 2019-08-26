import sys

import attr
from redis import Redis
from rq import Queue


def main():
    redis_url = sys.argv[1]
    from_name = sys.argv[2]
    to_name = sys.argv[3]

    redis = Redis.from_url(redis_url)

    redis.zrem("index:characters", from_name)
    redis.zadd("index:characters", {from_name: "0"})
    redis.rename("index:characters:" + from_name, "index:characters:" + to_name)

    print("Performed global rename...")

    for img_id, _ in redis.zscan_iter("index:characters:" + to_name):
        img_id = img_id.decode("utf-8")

        redis_key = "index:image:" + img_id

        tr = redis.pipeline()

        tr.srem(redis_key + ":characters", from_name)
        tr.sadd(redis_key + ":characters", to_name)

        tr.execute()

        print("Processed image " + img_id + "...")


if __name__ == "__main__":
    main()
