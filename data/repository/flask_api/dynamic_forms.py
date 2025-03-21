from data.repository.calls.helpers import generateTwoMonthsDateRange
from service.dynamic_form import generateDynamicFormDf
from datetime import datetime
import json, pandas as pd, redis

def updateRedisKeys(rawData: pd.DataFrame, redisKey: str) -> None:
    """ Updates Redis keys with the last date of Receipts Payroll table.

    Parameters
        - data {pandas.DataFrame} the data to update.
        - redisKey {str} the Redis key to update.
    """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Defines expiration time and formatted data for Redis key.
    expirationTime = 60*60*10
    data = json.dumps(obj=rawData, default=str)

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=data, ex=expirationTime)

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
        "DynamicFormsLastMonth",
        "DynamicFormsCurrentMonth",
        "DynamicFormsTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRange):
        dynamicFormDf = generateDynamicFormDf(val["start"], val["end"])
        updateRedisKeys(dynamicFormDf, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
