import base64
import binascii
from functools import wraps
import hashlib
import secrets

from sanic import response, exceptions


def authorized():
    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            redis = request.app.app_redis

            try:
                auth_header = request.headers["Authorization"]
            except KeyError:
                raise exceptions.Unauthorized("Authorization header required")

            try:
                method, credentials = auth_header.split(" ", 1)
                if method != "Basic":
                    raise exceptions.Unauthorized("Authorization method not supported")

                data = base64.b64decode(credentials).decode("utf-8")
                username, password = data.split(":", 1)
            except (ValueError, binascii.Error):
                raise exceptions.Unauthorized("Invalid authorization header")

            pw_data = await redis.hgetall("auth:user:" + username)
            try:
                if pw_data is None:
                    raise exceptions.Unauthorized("Invalid username / password")

                salt = pw_data[b"salt"]
                pw_hash = pw_data[b"hash"]
            except KeyError:
                raise exceptions.Unauthorized("Invalid username / password")

            test_pw_hash = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, 100000
            )
            if not secrets.compare_digest(pw_hash, test_pw_hash):
                raise exceptions.Unauthorized("Invalid username / password")

        return decorated_function

    return decorator
