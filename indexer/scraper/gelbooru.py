import io
from pathlib import Path
import time

from bs4 import BeautifulSoup
import requests
import attr
import numpy as np
from rq import Queue

from ..structures import QueuedImage

base_url = "https://gelbooru.com/"
ratings = {"s": "safe", "q": "questionable", "e": "explicit"}
exclude_tags = ["loli", "shota", "bestiality", "guro", "shadman"]


def gelbooru_post_to_queued_image(normalized_characters, data):
    tags = data["tags"].split()

    resp = requests.get(
        "https://gelbooru.com/index.php?page=post&s=view&id=" + str(data["id"])
    )
    soup = BeautifulSoup(resp.text)
    artists = []

    for artist_li in soup.find_all(
        "li", attrs={"class": "tag-type-artist"}, recursive=True
    ):
        for a_elem in artist_li.find_all("a", recursive=True):
            text = "".join(str(t) for t in a_elem.stripped_strings)

            if text != "?" and text in tags:
                artists.append(text)

    return QueuedImage(
        source_site="gelbooru",
        source_id=data["id"],
        source_url=data["file_url"],
        source_original=data.get("source", ""),
        sfw_rating=ratings[data["rating"]],
        characters=normalized_characters,
        authors=artists,
        source_tags=tags,
    )


def construct_search_endpoint(page, tags):
    endpoint = "/index.php?page=dapi&s=post&q=index&json=1&pid={:d}".format(page)
    tags = list(tags)

    if len(tags) > 0:
        endpoint += "&tags={}".format(
            "+".join(map(lambda s: str(s).lower().strip(), tags))
        )

    return base_url + endpoint


def search_api(tags):
    page = 0
    n_tries = 0

    while page < 1000:
        time.sleep(0.5)

        if n_tries > 5:
            print("Giving up.")
            return

        print("[search] tags: {} - page {}".format(" ".join(tags), page))
        response = requests.get(construct_search_endpoint(page, tags))

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

        for d in data:
            ts = d["tags"].split()

            if any(t in exclude_tags for t in ts):
                continue

            yield d


def associate_character_tag(redis, normalized_character, character_tag):
    tr = redis.pipeline()

    tr.sadd("gelbooru:characters", normalized_character)
    tr.set("gelbooru:characters:" + normalized_character, character_tag)

    tr.execute()


def index_character(redis, normalized_character):
    character_tags = redis.get("gelbooru:characters:" + normalized_character)
    if character_tags is None:
        return

    character_tags = character_tags.decode("utf-8")
    character_tags = character_tags.split(",")

    queue = Queue("backend-index", connection=redis)
    for post_data in search_api(character_tags):
        if redis.sismember("index:sites:danbooru:source_ids", post_data["id"]):
            continue

        queue_data = gelbooru_post_to_queued_image((normalized_character,), post_data)

        # check URL filetype:
        if queue_data.source_url is None:
            continue

        # pylint: disable=no-member
        splits = queue_data.source_url.rsplit(".", maxsplit=1)

        if len(splits) == 2:
            if splits[1] not in ["png", "jpeg", "jpg", "gif"]:
                continue
        else:
            continue

        queue.enqueue("indexer.backend.worker.process_queued_image", queue_data)
        print("Gelbooru: Enqueued post {} for indexing".format(queue_data.source_id))
