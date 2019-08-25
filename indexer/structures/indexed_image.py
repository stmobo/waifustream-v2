import attr
import numpy as np

from ..snowflake import get_timestamp
from .queued_image import QueuedImage


def _cvt_imhash(h):
    if isinstance(h, np.ndarray):
        return h.tobytes()
    elif h is None:
        return None
    else:
        return bytes(h)


@attr.s(slots=True)
class IndexedImage(object):
    """Represents an image that is in the index.
    """

    # snowflake ID
    img_id: int = attr.ib(converter=int)

    # img hash
    imhash: bytes = attr.ib(converter=_cvt_imhash)

    queued_img_data: QueuedImage = attr.ib()

    @property
    def imhash_ndarray(self):
        """ndarray: The image hash for this entry, as a uint8 `ndarray`.
        """
        return np.frombuffer(self.imhash, dtype=np.uint8)

    def __getattr__(self, name):
        return getattr(self.queued_img_data, name)

    @classmethod
    def load_from_index(cls, redis, img_id):
        redis_key = "index:image:" + str(img_id)
        exists = redis.exists(redis_key)

        if not exists:
            raise KeyError("No image " + str(img_id) + " exists in index")

        ret_data = redis.hgetall(redis_key)
        data = {}

        for key, value in ret_data.items():
            data[key.decode("utf-8")] = value

        characters = redis.smembers(redis_key + ":characters")
        authors = redis.smembers(redis_key + ":authors")
        source_tags = redis.smembers(redis_key + ":source_tags")

        queued_img_data = QueuedImage.from_redis_data(
            data, characters, authors, source_tags
        )

        return cls(
            img_id=img_id, imhash=data["imhash"], queued_img_data=queued_img_data
        )

    @classmethod
    async def load_from_index_async(cls, aredis, img_id):
        redis_key = "index:image:" + str(img_id)
        exists = await aredis.exists(redis_key)

        if not exists:
            raise KeyError("No image " + str(img_id) + " exists in index")

        ret_data = await aredis.hgetall(redis_key)
        data = {}

        for key, value in ret_data.items():
            data[key.decode("utf-8")] = value

        characters = await aredis.smembers(redis_key + ":characters", encoding="utf-8")
        authors = await aredis.smembers(redis_key + ":authors", encoding="utf-8")
        source_tags = await aredis.smembers(
            redis_key + ":source_tags", encoding="utf-8"
        )

        queued_img_data = QueuedImage.from_redis_data(
            data, characters, authors, source_tags
        )

        return cls(
            img_id=img_id, imhash=data["imhash"], queued_img_data=queued_img_data
        )

    @classmethod
    def from_queued_image(cls, img_id, img_hash, queued_image):
        return cls(img_id=img_id, imhash=img_hash, queued_img_data=queued_image)

    def save_duplicate_info(self, redis):
        redis_key = "index:image:" + str(self.img_id)

        tr = redis.pipeline()

        # pylint: disable=no-member
        tr.sadd(
            redis_key + ":aliases",
            "{}#{}".format(
                self.queued_img_data.source_site, self.queued_img_data.source_id
            ),
        )

        tr.sadd("index:sites:" + self.queued_img_data.source_site, self.img_id)
        tr.sadd(
            "index:sites:" + self.queued_img_data.source_site + ":source_ids",
            self.source_id,
        )

        tr.execute()

    def save_to_index(self, redis):
        redis_key = "index:image:" + str(self.img_id)
        imhash_key = b"imhash:" + self.imhash

        d = {"imhash": self.imhash}
        d.update(
            attr.asdict(
                self.queued_img_data,
                filter=lambda attr, value: attr
                not in ("characters", "authors", "source_tags"),
            )
        )

        del d["characters"]
        del d["authors"]
        del d["source_tags"]

        tr = redis.pipeline()

        tr.set(imhash_key, self.img_id)
        tr.sadd("index:images", self.img_id)

        tr.delete(redis_key)
        tr.hmset(redis_key, d)

        # pylint: disable=no-member
        tr.sadd(
            redis_key + ":aliases",
            "{}#{}".format(
                self.queued_img_data.source_site, self.queued_img_data.source_id
            ),
        )

        tr.delete(redis_key + ":characters")

        if len(self.queued_img_data.characters) > 0:
            tr.sadd(redis_key + ":characters", *self.queued_img_data.characters)

        tr.delete(redis_key + ":authors")

        if len(self.queued_img_data.authors) > 0:
            tr.sadd(redis_key + ":authors", *self.queued_img_data.authors)

        tr.delete(redis_key + ":source_tags")

        if len(self.queued_img_data.source_tags) > 0:
            tr.sadd(redis_key + ":source_tags", *self.queued_img_data.source_tags)

        ts = get_timestamp(self.img_id)

        tr.sadd("index:sites:" + self.queued_img_data.source_site, self.img_id)
        tr.sadd(
            "index:sites:" + self.queued_img_data.source_site + ":source_ids",
            self.source_id,
        )

        tr.sadd("index:rating:" + self.queued_img_data.sfw_rating, self.img_id)

        tr.zadd(
            "index:characters",
            dict((ch, "0") for ch in self.queued_img_data.characters),
        )

        if len(self.queued_img_data.authors) > 0:
            tr.zadd(
                "index:authors", dict((au, "0") for au in self.queued_img_data.authors)
            )

        if len(self.queued_img_data.source_tags) > 0:
            tr.zadd(
                "index:tags:all",
                dict(
                    (tag + "@" + self.queued_img_data.source_site, "0")
                    for tag in self.queued_img_data.source_tags
                ),
            )

            tr.zadd(
                "index:tags:" + self.queued_img_data.source_site,
                dict((tag, "0") for tag in self.queued_img_data.source_tags),
            )

        for character in self.queued_img_data.characters:
            if len(character) == 0:
                continue

            tr.zadd("index:characters:" + character, {self.img_id: ts})

        for author in self.queued_img_data.authors:
            if len(author) == 0:
                continue

            tr.zadd("index:authors:" + author, {self.img_id: ts})

        for tag in self.queued_img_data.source_tags:
            if len(tag) == 0:
                continue

            tr.zadd(
                "index:tags:" + self.queued_img_data.source_site + ":" + tag,
                {self.img_id: ts},
            )

            tr.zadd(
                "index:tags:merged:" + tag + "@" + self.queued_img_data.source_site,
                {self.img_id: ts},
            )

            tr.zadd("index:tags:merged:" + tag, {self.img_id: ts})

        tr.execute()

