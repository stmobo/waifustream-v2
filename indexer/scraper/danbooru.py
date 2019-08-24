import io
from pathlib import Path
import time

import requests
import attr
from PIL import Image
import numpy as np
from rq import Queue

from ..structures import QueuedImage

base_url = "https://danbooru.donmai.us"
ratings = {"s": "safe", "q": "questionable", "e": "explicit"}
exclude_tags = ["loli", "shota", "bestiality", "guro", "shadman"]


def danbooru_post_to_queued_image(normalized_characters, data):
    tags = data["tag_string"].split()

    url = None
    if "file_url" in data:
        url = data["file_url"]
    elif "large_file_url" in data:
        url = data["large_file_url"]
    elif "preview_file_url" in data:
        url = data["preview_file_url"]

    return QueuedImage(
        source_site="danbooru",
        source_id=data["id"],
        source_url=url,
        sfw_rating=ratings[data["rating"]],
        characters=normalized_characters,
        authors=data["tag_string_artist"].split(),
        source_tags=tags,
    )


def construct_search_endpoint(page, tags, start_id):
    endpoint = "/posts.json?page={}&limit=200".format(page)
    tags = list(tags)

    if start_id is not None:
        if len(tags) >= 2:
            tags = list(tags[:1])

        tags.append("id%3A%3C" + str(start_id))

    if len(tags) > 0:
        endpoint += "&tags={}".format(
            "+".join(map(lambda s: str(s).lower().strip(), tags))
        )

    return base_url + endpoint


def search_api(tags, start_id=None):
    if len(tags) > 2:
        raise ValueError("Cannot search for more than two tags at a time")

    if start_id is not None:
        start_id = int(start_id)

    page = 0
    n_tries = 0

    while page < 1000:
        time.sleep(0.5)

        if n_tries > 5:
            print("Giving up.")
            return

        print("[search] tags: {} - page {}".format(" ".join(tags), page))
        response = requests.get(construct_search_endpoint(page, tags, start_id))

        if response.status_code < 200 or response.status_code > 299:
            print(
                "    Got error response code {} when retrieving {} page {}".format(
                    str(response.status_code), " ".join(tags), page
                )
            )
            n_tries += 1
            continue

        data = response.json()

        if not isinstance(data, list):
            print("    Got weird response: " + str(data))
            n_tries += 1
            continue

        if len(data) == 0:
            return

        page += 1
        n_tries = 0

        ids = list(int(d["id"]) for d in data)
        last_id = min(ids)

        if start_id is not None and last_id > start_id:
            continue

        for d in data:
            ts = d["tag_string"].split()

            if any(t in exclude_tags for t in ts):
                continue

            yield d


def associate_character_tag(redis, normalized_character, character_tag):
    tr = redis.pipeline()

    tr.sadd("danbooru:characters", normalized_character)
    tr.set("danbooru:characters:" + normalized_character, character_tag)

    tr.execute()


def index_character(redis, normalized_character):
    character_tag = redis.get("danbooru:characters:" + normalized_character)
    if character_tag is None:
        return

    character_tag = character_tag.decode("utf-8")

    queue = Queue("backend-index", connection=redis)
    for post_data in search_api([character_tag]):
        queue_data = danbooru_post_to_queued_image((normalized_character,), post_data)

        # check URL filetype:
        if queue_data.source_url is None:
            continue

        splits = queue_data.source_url.rsplit(".", maxsplit=1)

        if len(splits) == 2:
            if splits[1] not in ["png", "jpeg", "jpg", "gif"]:
                continue
        else:
            continue

        queue.enqueue("indexer.backend.worker.process_queued_image", queue_data)
        print("Danbooru: Enqueued post {} for indexing".format(queue_data.source_id))
