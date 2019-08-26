import base64
import hashlib
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
from .management import bp as management_bp

app = Sanic(load_env="WAIFUSTREAM_")
app.blueprint(management_bp)
app.config.update(
    {
        "PROXIES_COUNT": 1,
        # "IMAGE_CACHE_DIR": "",
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

    app.http_session = aiohttp.ClientSession()


@app.listener("after_server_stop")
async def teardown(app, loop):
    app.index_redis.close()
    app.app_redis.close()

    await app.index_redis.wait_closed()
    await app.app_redis.wait_closed()

    await app.http_session.close()


@app.route("/images/<img_id:int>")
async def get_index_data(request, img_id):
    try:
        indexed_image = await IndexedImage.load_from_index_async(
            app.index_redis, img_id
        )
    except KeyError as e:
        if e.args[0].startswith("No image"):
            raise exceptions.NotFound("No image " + str(img_id) + " in index")
        else:
            raise e

    data = {
        "img_id": str(indexed_image.img_id),
        "imhash": base64.b64encode(indexed_image.imhash),
        "cache_path": "image/" + indexed_image.cache_filename,
    }
    data.update(attr.asdict(indexed_image.queued_img_data))

    return response.json(data)


@app.route("/image/<img_id:int>")
async def get_image_route(request, img_id):
    try:
        indexed_image = await IndexedImage.load_from_index_async(
            app.index_redis, img_id
        )
    except KeyError as e:
        if e.args[0].startswith("No image"):
            raise exceptions.NotFound("No image " + str(img_id) + " in index")
        else:
            raise e

    img_path = await load_indexed_image(app, indexed_image)
    _, ext = osp.splitext(img_path)

    return await response.file_stream(img_path, mime_type=image_types[ext[1:]])


@app.route("/characters")
async def get_all_characters_route(request):
    characters = await app.index_redis.zrange(
        "index:characters", 0, -1, encoding="utf-8"
    )
    return response.json(characters)


@app.route("/characters/<character:string>")
async def get_character_images_route(request, character):
    page = 0
    count = 100

    if "page" in request.args:
        try:
            page = int(request.args["page"][0])
        except ValueError:
            raise exceptions.InvalidUsage("Page argument must be an integer")

    if "count" in request.args:
        try:
            count = int(request.args["count"][0])
        except ValueError:
            raise exceptions.InvalidUsage("Count argument must be an integer")

    start_index = page * count

    if not (await app.index_redis.exists("index:characters:" + character) > 0):
        raise exceptions.NotFound("No character " + character + " found in index")

    filter_sets = []

    if "tag" in request.args:
        filter_sets.extend("index:tags:merged:" + t for t in request.args["tag"])

    if "author" in request.args:
        filter_sets.extend("index:authors:" + a for a in request.args["author"])

    if "site" in request.args:
        filter_sets.extend("index:sites:" + s for s in request.args["site"])

    if "rating" in request.args:
        filter_sets.extend("index:rating:" + r for r in request.args["rating"])

    if len(filter_sets) > 0:
        filter_sets.insert(0, "index:characters:" + character)

        h = hashlib.sha1()
        for s in filter_sets:
            h.update(s.encode("utf-8"))
        dest_key = "tmp_query:" + h.hexdigest()

        if not (await app.index_redis.exists(dest_key)):
            await app.index_redis.zinterstore(dest_key, *filter_sets, aggregate="max")

        await app.index_redis.expire(dest_key, 15 * 60)

        total = await app.index_redis.zcard(dest_key)
        ids = await app.index_redis.zrange(
            dest_key, start_index, start_index + count, encoding="utf-8"
        )
    else:
        total = await app.index_redis.zcard("index:characters:" + character)
        ids = await app.index_redis.zrange(
            "index:characters:" + character,
            start_index,
            start_index + count,
            encoding="utf-8",
        )

    resp = []
    for img_id in ids:
        indexed_image = await IndexedImage.load_from_index_async(
            app.index_redis, int(img_id)
        )

        index_data = {
            "img_id": str(indexed_image.img_id),
            "imhash": base64.b64encode(indexed_image.imhash),
            "cache_path": "image/" + indexed_image.cache_filename,
        }
        index_data.update(attr.asdict(indexed_image.queued_img_data))

        resp.append(index_data)

    return response.json(resp, headers={"X-Total-Items": total})


def main():
    app.run(host="0.0.0.0", port=8090, workers=os.cpu_count())


if __name__ == "__main__":
    main()
