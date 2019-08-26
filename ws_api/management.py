from sanic import Blueprint, exceptions, response

from .utils import authorized


bp = Blueprint("management")


@bp.route("/user/whoami")
@authorized()
async def whoami_route(request):
    return response.text(request["username"])


@bp.route("/characters/<character:string>", methods=["POST"])
@authorized()
async def associate_characters_route(request, character):
    if request.json is None or not isinstance(request.json, dict):
        raise exceptions.InvalidUsage("Must send JSON dictionary payload")

    for site, tags in request.json.items():
        if isinstance(tags, list):
            tags = ",".join(tags)

        request.app.scraper_queue.enqueue(
            "indexer.scraper.worker.do_associate_character", site, character, tags
        )

    return response.text("", status=204)


@bp.route("/characters/<character:string>/update", methods=["POST"])
@authorized()
async def index_characters_route(request, character):
    if request.json is None or not isinstance(request.json, list):
        raise exceptions.InvalidUsage("Must send JSON list payload")

    for site in request.json:
        request.app.scraper_queue.enqueue(
            "indexer.scraper.worker.do_indexing_crawl",
            site,
            character,
            job_timeout="6h",
        )

    return response.text("", status=202)
