from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from data.repository.calls.webquotes_repo import Webquotes
from service.webquotes import generateWebquotesDf
from datetime import datetime
import json, redis

def updateWebquotesTables(start: str, end: str) -> None:
    """ Updates Webquotes table in db with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    webquotesDf = generateWebquotesDf(start, end)
    postDataframeToDb(data=webquotesDf, table="webquotes", mode="append", filename="flask_api.ini")

def addWebquotesTodayRecords() -> None:
    """ Generates today's date to add to Webquotes table. """

    today = datetime.today().date().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in db with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")

def updateWebquotesPreviousRecords() -> None:
    """ Generate last and current month date ranges to delete and
    update Webquotes table. """

    dateRanges = genWebquotesDateRange()

    webquotes = Webquotes()
    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    webquotes.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Webquotes data from {firstDayLastMonth} to {yesterday} deleted...")

    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

def genWebquotesDateRange() -> list[dict[str, str]] | None:
    """ Generates date range to delete and update Webquotes table.
    
    Returns
        {list[dict[str, str]] | None} date ranges to delete and update
        or None if exception raise.
    """

    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)

    dataAvailable = any(
        not generateWebquotesDf(date["start"], date["end"]).empty
        for date in dateRanges
    )

    if not dataAvailable:
        raise Exception(f"No data from {dateRanges[0]['start']} to {dateRanges[0]['end']} to update.")

    return dateRanges

def updateRedisKeys(start: str, end: str, redisKey: str) -> None:
    """ Updates Redis keys with the last date of Webquotes table.
    
    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
        - redisKey {str} the Redis key to update.
    """
    
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
    webquotesDf = generateWebquotesDf(start, end)
    expirationTime = 60*60*10
    data = json.dumps(obj=webquotesDf, default=str)
    redisCli.set(name=redisKey, value=data, ex=expirationTime)
    redisCli.close()

def updateRedisPreviousRecords() -> None:
    """ Generate last and current month date ranges to update
    Redis keys for Webquotes table. """

    dateRanges = genWebquotesDateRange()
    redisKeys = ["WebquotesLastMonth", "WebquotesCurrentMonth"]

    for i, val in enumerate(dateRanges):
        updateRedisKeys(val["start"], val["end"], redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")