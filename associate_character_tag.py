import sys

from redis import Redis
from rq import Queue

from indexer.scraper import danbooru, gelbooru


def main():
    redis_url = sys.argv[1]
    site = sys.argv[2]
    character = sys.argv[3]
    tag = sys.argv[4]

    redis = Redis.from_url(redis_url)

    if site == "danbooru":
        danbooru.associate_character_tag(redis, character, tag)
        print("Associated character: {} with danbooru tag {}".format(character, tag))
    elif site == "gelbooru":
        gelbooru.associate_character_tag(redis, character, tag)
        print("Associated character: {} with gelbooru tag {}".format(character, tag))


if __name__ == "__main__":
    main()
