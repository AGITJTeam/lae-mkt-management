import json, redis
from datetime import datetime

from data.repository.calls.helpers import generateTwoMonthsDateRange
from service.dynamic_form import generateDynamicFormDf

def updateRedisPreviousRecords():
    today = datetime.today().date()
    dateRange = generateTwoMonthsDateRange(today)
    redisKeys = ["DynamicFormsLastMonth", "DynamicFormsCurrentMonth", "DynamicFormsTwoMonths"]
    
    firstDayLastMonth = dateRange[0]["start"]
    currentDay = dateRange[1]["end"]
    
    dateRange.append({
        "start": firstDayLastMonth,
        "end": currentDay
    })
    
    for i, val in enumerate(dateRange):
        updateRedisKeys(val["start"], val["end"], redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")

def updateRedisKeys(start: str, end: str, redisKey: str) -> None:
    """ Updates Redis keys with the last date of Receipts Payroll table.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
        - redisKey {str} the Redis key to update.
    """

    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
    dynamicFormDf = generateDynamicFormDf(start, end)
    expirationTime = 60*60*10
    data = json.dumps(obj=dynamicFormDf, default=str)
    redisCli.set(name=redisKey, value=data, ex=expirationTime)
    redisCli.close()