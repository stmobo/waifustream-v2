import base64
import os
import os.path as osp
import time

import aiohttp
import aioredis
import attr
from redis import Redis
from rq import Queue
from sanic import Sanic, exceptions, response

from .img_cache import load_indexed_image
from indexer.structures import IndexedImage

app = Sanic(load_env="WAIFUSTREAM_")
app.config.update(
    {
        "PROXIES_COUNT": 1,
        "IMAGE_CACHE_DIR": "",
        "IMAGE_CACHE_TTL": 14 * 24 * 3600,
        "REDIS_URL": "redis://localhost:6380",
        "INDEX_DB": 0,
        "APP_DB": 1,
    }
)

app.index_redis = None
app.app_redis = None
app.scraper_queue = None

image_types = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
}


@app.listener("before_server_start")
async def init(app, loop):
    app.index_redis = await aioredis.create_redis(
        app.config["REDIS_URL"], db=int(app.config["INDEX_DB"])
    )

    app.app_redis = await aioredis.create_redis(
        app.config["REDIS_URL"], db=int(app.config["APP_DB"])
    )

    app.sync_redis = Redis.from_url(app.config["REDIS_URL"], db=app.config["INDEX_DB"])
    app.scraper_queue = Queue("scraper", connection=app.sync_redis)


@app.listener("after_server_stop")
async def teardown(app, loop):
    app.redis.close()

    await app.redis.wait_closed()


@app.route("/images/<img_id:int>")
async def get_index_data(request, img_id):
    try:
        indexed_image = await IndexedImage.load_from_index_async(
            app.index_redis, img_id
        )
    except KeyError:
        raise exceptions.NotFound("No image " + img_id + " in index")

    data = attr.asdict(indexed_image)
    data["imhash"] = base64.b64encode(data["imhash"])

    return response.json(data)


@app.route("/images/<img_id:int>/image")
async def get_image_route(request, img_id):
    try:
        indexed_image = await IndexedImage.load_from_index_async(
            app.index_redis, img_id
        )
    except KeyError:
        raise exceptions.NotFound("No image " + img_id + " in index")

    img_path = await load_indexed_image(app.app_redis, indexed_image)
    _, ext = osp.splitext(img_path)

    return await response.file_stream(img_path, mime_type=image_types[ext[1:]])


@app.route("/characters/<character:string>")
async def get_character_images_route(request, character):
    page = 0
    count = 100
    start_index = page * count

    if not (await app.index_redis.exists("index:characters:" + character) > 0):
        raise exceptions.NotFound("No character " + character + " found in index")

    data = await app.index_redis.zrange(
        "index:characters:" + character,
        start_index,
        start_index + count,
        encoding="utf-8",
    )
    return response.json(list(map(int, data)))


@app.route("/characters/<character:string>", methods=["POST"])
async def associate_characters_route(request, character):
    if request.json is None or not isinstance(request.json, dict):
        raise exceptions.InvalidUsage("Must send JSON dictionary payload")

    for site, tags in request.json.items():
        if isinstance(tags, list):
            tags = ",".join(tags)

        app.scraper_queue.enqueue(
            "indexer.scraper.worker.do_associate_character", site, character, tags
        )

    return response.text("", status=204)


@app.route("/characters/<character:string>/update", methods=["POST"])
async def index_characters_route(request, character):
    if request.json is None or not isinstance(request.json, list):
        raise exceptions.InvalidUsage("Must send JSON list payload")

    for site in request.json:
        if isinstance(tags, list):
            tags = ",".join(tags)

        app.scraper_queue.enqueue(
            "indexer.scraper.worker.do_indexing_crawl", site, character
        )

    return response.text("", status=202)

def main():
    app.run(host="0.0.0.0", port=8090)


if __name__ == "__main__":
    main()