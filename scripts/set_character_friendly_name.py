import sys

import attr
from redis import Redis


def main():
    redis_url = sys.argv[1]
    basename = sys.argv[2]
    friendly_name = sys.argv[3]

    redis = Redis.from_url(redis_url)
    redis.set("character:" + basename + ":name", friendly_name)


if __name__ == "__main__":
    main()
