import sys

from redis import Redis
from rq import Queue

from indexer.scraper.danbooru import associate_character_tag


def main():
    redis_url = sys.argv[1]
    character = sys.argv[2]
    tag = sys.argv[3]

    redis = Redis.from_url(redis_url)

    associate_character_tag(redis, character, tag)
    print("Associated character: {} with danbooru tag {}".format(character, tag))


if __name__ == "__main__":
    main()
