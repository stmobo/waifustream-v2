import time

EPOCH = 1543536000000


def generate_snowflake(redis, group_id, worker_id):
    snowflake_key = "last_snowflake:" + str(group_id) + ":" + str(worker_id)

    last_snowflake = redis.get(snowflake_key)

    cur_ts = int(time.time() * 1000) - EPOCH
    cur_seq = 0

    if last_snowflake is not None:
        last_snowflake = int(last_snowflake.decode("utf-8"))
        last_ts = last_snowflake >> 22
        last_seq = last_snowflake & 0xFFF

        if cur_ts < last_ts:
            # time is going backwards?
            print("Time went backwards (from {} to {})".format(last_ts, cur_ts))
            time.sleep((last_ts - cur_ts) * 1000)
            return generate_snowflake(redis, group_id, worker_id)

        if last_ts == cur_ts:
            cur_seq = (last_seq + 1) & 0xFFF
            if cur_seq <= last_seq:
                print("Sequence overrun")
                time.sleep(0.010)
                return generate_snowflake(redis, group_id, worker_id)

    snowflake = (
        ((cur_ts & 0x3FFFFFFFFFF) << 22)
        | ((group_id & 0x1F) << 17)
        | ((worker_id & 0x1F) << 12)
        | (cur_seq & 0xFFF)
    )

    redis.set(snowflake_key, snowflake, px=1)

    return snowflake


def get_timestamp(snowflake):
    return (snowflake >> 22) + EPOCH
