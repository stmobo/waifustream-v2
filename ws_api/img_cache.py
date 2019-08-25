import io
import os.path as osp
import hashlib
import time

import aiofiles

ALLOWED_FILE_TYPES = ("png", "jpg", "jpeg", "gif")


async def download_image(app, filename, url):
    async with app.http_session.get(url) as resp:
        if resp.status < 200 or resp.status > 299:
            raise OSError("Got error " + str(resp.status) + " when fetching " + url)

        cur_ts = int(time.time() * 1000)
        out_path = osp.join(app.config["IMAGE_CACHE_DIR"], filename)

        async with aiofiles.open(out_path, "wb") as f:
            while True:
                chunk = await resp.content.read(128)
                if not chunk:
                    break
                await f.write(chunk)

        await app.app_redis.zadd("img_cache:live", cur_ts, out_path)

        return out_path


async def load_indexed_image(app, indexed_image):
    img_id = str(indexed_image.img_id)
    url = indexed_image.source_url

    _, ext = osp.splitext(url)
    if len(ext) <= 1:
        raise ValueError("Could not detect filetype of " + url)

    ext = ext[1:].lower()
    if ext not in ALLOWED_FILE_TYPES:
        raise ValueError("Invalid filetype " + ext + " for URL " + url)

    filename = img_id + "." + ext
    path = osp.join(app.config["IMAGE_CACHE_DIR"], filename)
    if osp.isfile(path):
        return path

    path = await download_image(app, filename, url)

    return path

