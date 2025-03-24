from data.repository.calls.helpers import generateTwoMonthsDateRange
from data.repository.stats_dash.dialpad_calls import countDialpadCallsByDateRange
from datetime import datetime
import json, pandas as pd, redis

def updateRedisKeys(allCalls: list[dict], uniqueCalls: list[dict], redisKey: str) -> None:
    """ Updates Redis keys with given DataFrame.

    Parameters
        - allCalls {list[dict]} the data for all Dialpad calls.
        - uniqueCalls {list[dict]} the data for unique Dialpad calls.
        - redisKey {str} the Redis key to update.
    """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Defines expiration time and formatted data for Redis key.
    allCallsJson = json.dumps(obj=allCalls, default=str)
    uniqueCallsJson = json.dumps(obj=uniqueCalls, default=str)

    # Set Redis key with 10 hours expiration time.
    #redisCli.set(name=redisKey, value=data, ex=expirationTime)
    redisCli.hset(
        name=redisKey,
        mapping={
            "allCalls": allCallsJson,
            "uniqueCalls": uniqueCallsJson
        }
    )

    # Close Redis connection.
    redisCli.close()

def updateTwoMonthsRedisKeys():
    # Defines date ranges and Redis keys.
    today = datetime.today().date()
    dateRange = generateTwoMonthsDateRange(today)
    firstDayLastMonth = dateRange[0]["start"]
    currentDay = dateRange[1]["end"]
    dateRange.append({
        "start": firstDayLastMonth,
        "end": currentDay
    })
    redisKeys = [
        "DialpadCallsPreviousMonth",
        "DialpadCallsCurrentMonth",
        "DialpadCallsTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRange):
        allCalls, uniqueCalls = countDialpadCallsByDateRange(val["start"], val["end"])
        updateRedisKeys(allCalls, uniqueCalls, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
