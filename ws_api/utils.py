import base64
import binascii
from functools import wraps
import hashlib
import secrets

from itsdangerous import BadSignature, SignatureExpired
from sanic import response, exceptions


def authorized():
    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            auth_cookie = request.cookies.get("ws_session")
            if auth_cookie is not None:
                try:
                    auth_cookie = base64.b64decode(auth_cookie)
                    username = request.app.signer.unsign(auth_cookie)
                except SignatureExpired:
                    del request.cookies["ws_session"]
                    raise exceptions.Unauthorized("Authorization cookie expired")
                except BadSignature:
                    del request.cookies["ws_session"]
                    raise exceptions.Unauthorized("Bad authorization cookie")
            else:
                redis = request.app.app_redis

                try:
                    auth_header = request.headers["Authorization"]
                except KeyError:
                    raise exceptions.Unauthorized("Authorization header required")

                try:
                    method, credentials = auth_header.split(" ", 1)
                    if method != "Basic":
                        raise exceptions.Unauthorized(
                            "Authorization method not supported"
                        )

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

            request["username"] = username
            return await f(request, *args, **kwargs)

        return decorated_function

    return decorator
