import hashlib
import secrets
import sys

from redis import Redis


def main():
    redis_url = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]

    redis = Redis.from_url(redis_url)

    salt = secrets.token_bytes(8)
    pw_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100000)

    redis.hmset("auth:user:"+username, {
        "salt": salt,
        "hash": pw_hash
    })

    print("Created new user {}".format(username))


if __name__ == "__main__":
    main()
