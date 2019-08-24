import attr


def convert_redis_sequence(l):
    def _cvt(x):
        try:
            return x.decode("utf-8")
        except AttributeError:
            return str(x)

    return tuple(map(_cvt, l))


@attr.s(frozen=True, slots=True)
class QueuedImage(object):
    """Represents an image that is queued to be indexed by the indexer backend.
    """

    source_site: str = attr.ib(converter=str)
    source_id: str = attr.ib(converter=str)
    source_url: str = attr.ib(converter=str)
    characters: tuple = attr.ib(converter=convert_redis_sequence)
    sfw_rating: str = attr.ib(converter=str)
    authors: tuple = attr.ib(converter=convert_redis_sequence)
    source_tags: tuple = attr.ib(converter=convert_redis_sequence)

    @classmethod
    def from_redis_data(cls, redis_data, characters, authors, source_tags):
        return cls(
            source_site=redis_data["source_site"].decode("utf-8"),
            source_id=redis_data["source_id"].decode("utf-8"),
            source_url=redis_data["source_url"].decode("utf-8"),
            sfw_rating=redis_data["sfw_rating"].decode("utf-8"),
            characters=characters,
            authors=authors,
            source_tags=source_tags,
        )
