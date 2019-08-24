import attr
import numpy as np

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

        redis_data = redis.hgetall(redis_key)

        characters = redis.smembers(redis_key + ":characters")
        authors = redis.smembers(redis_key + ":authors")
        source_tags = redis.smembers(redis_key + ":source_tags")

        queued_img_data = QueuedImage.from_redis_data(
            redis_data, characters, authors, source_tags
        )

        return cls(
            img_id=img_id, imhash=redis_data["imhash"], queued_img_data=queued_img_data
        )

    @classmethod
    def from_queued_image(cls, img_id, img_hash, queued_image):
        return cls(img_id=img_id, imhash=img_hash, queued_img_data=queued_image)

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
        tr.delete(redis_key + ":characters")

        if len(self.queued_img_data.characters) > 0:
            tr.sadd(redis_key + ":characters", *self.queued_img_data.characters)

        tr.delete(redis_key + ":authors")
        
        if len(self.queued_img_data.authors) > 0:
            tr.sadd(redis_key + ":authors", *self.queued_img_data.authors)

        tr.delete(redis_key + ":source_tags")

        if len(self.queued_img_data.source_tags) > 0:
            tr.sadd(redis_key + ":source_tags", *self.queued_img_data.source_tags)

        tr.sadd("index:sites:" + self.queued_img_data.source_site, self.img_id)
        tr.sadd(
            "index:sites:" + self.queued_img_data.source_site + ":source_ids",
            self.source_id,
        )

        tr.sadd("index:rating:" + self.queued_img_data.sfw_rating, self.img_id)

        for character in self.queued_img_data.characters:
            tr.sadd("index:characters", character)
            tr.sadd("index:characters:" + character, self.img_id)

        for author in self.queued_img_data.authors:
            tr.sadd("index:authors", author)
            tr.sadd("index:authors:" + author, self.img_id)

        for tag in self.queued_img_data.source_tags:
            tr.sadd("index:tags:" + self.queued_img_data.source_site, tag)
            tr.sadd(
                "index:tags:" + self.queued_img_data.source_site + ":" + tag,
                self.img_id,
            )

        tr.execute()

