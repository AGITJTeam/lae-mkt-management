from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from data.repository.calls.webquotes_repo import Webquotes
from service.webquotes import generateWebquotesDf
from datetime import datetime
import json, pandas as pd, redis

def updateWebquotesTables(start: str, end: str) -> None:
    """ Updates Webquotes table in db with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    webquotesDf = generateWebquotesDf(start, end)
    postDataframeToDb(
        data=webquotesDf,
        table="webquotes",
        mode="append",
        filename="flask_api.ini"
    )

def updateRedisKeys(rawData: str | list[dict], redisKey: str) -> None:
    """ Updates Redis keys with given DataFrame.
    
    Parameters
        - data {str | list[dict]} the data to update.
        - redisKey {str} the Redis key to update.
    """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Defines expiration time and formatted data for Redis key.
    expirationTime = 60*60*10
    if type(rawData) == list:
        data = json.dumps(obj=rawData, default=str)
        rawData = data

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=rawData, ex=expirationTime)

    # Close Redis connection.
    redisCli.close()

def addWebquotesTodayRecords() -> None:
    """ Adds today's date data to Webquotes table. """

    today = datetime.today().date().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

def updateWebquotesPreviousRecords() -> None:
    """ Generate last and current month date ranges to delete and
    update Webquotes table. """

    # Defines date ranges.
    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]

    # Delete data that will be updated.
    webquotes = Webquotes()
    webquotes.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Webquotes data from {firstDayLastMonth} to {yesterday} deleted...")

    # Generates data for every date range and updates Webquotes table.
    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")

def updateWTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month and last month
    for updating Redis keys for Webquotes endpoint. """

    # Defines date ranges and Redis keys.
    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    redisKeys = [
        "WebquotesPreviousMonth",
        "WebquotesCurrentMonth"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        webquotesDf = generateWebquotesDf(val["start"], val["end"])
        webquotesJson = webquotesDf.to_json(orient="records", date_format="iso")
        updateRedisKeys(webquotesJson, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")

def updateWDTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month and last month
    for updating Redis keys for WebquotesDetails endpoint. """

    # Defines date ranges and Redis keys.
    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    redisKeys = [
        "WebquotesDetailsPreviousMonth",
        "WebquotesDetailsCurrentMonth"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        webquotes = Webquotes()
        webquotesDetailsJson = webquotes.getWebquotesFromDateRange(val["start"], val["end"])
        updateRedisKeys(webquotesDetailsJson, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")

def updateAllWebquotesRedisKey() -> None:
    """ Generate date range from 2024-01-01 to today for updating Redis
    key for Webquotes endpoint. """

    # Defines date range and Redis keys.
    today = datetime.today().date().isoformat()
    start = "2024-01-01"
    redisKey = "AllWebquotes"

    # Generates data for date range.
    webquotes = Webquotes()
    webquotesJson = webquotes.getPartialFromDateRange(start, today)

    # Updates Redis key.
    updateRedisKeys(webquotesJson, redisKey)
    print(f"Redis keys {redisKey} updated...")
