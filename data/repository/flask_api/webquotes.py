from data.repository.calls.helpers import (
    generateOneMonthDateRange,
    generateTwoMonthsDateRange,
    postDataframeToDb
)
from data.repository.calls.webquotes_repo import Webquotes
from service.webquotes import generateWebquotesDf
from datetime import datetime, timedelta
import json, logging, redis

logger = logging.getLogger(__name__)

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

def updateWebquotesPreviousRecords() -> None:
    """ Generate last and current month date ranges to delete and
    update Webquotes table. """

    # Defines date ranges.
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    start, end = generateOneMonthDateRange(yesterday)

    # Delete data that will be updated.
    webquotes = Webquotes()
    webquotes.deleteLastMonthData(start, end)
    logger.info(f"Webquotes data from {start} to {end} deleted...")

    # Generates data for current month and updates Webquotes table.
    updateWebquotesTables(start, end)
    logger.info(f"Webquotes data from {start} to {end} updated...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    logger.info(f"Webquotes data from {start} to {end} added...")

def updateWTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month and last month
    for updating Redis keys for Webquotes endpoint. """

    # Retrieves last date from database.
    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]

    # Defines date ranges.
    dateRanges = generateTwoMonthsDateRange(lastDate)
    firstDayPreviousMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    dateRanges.append({
        "start": firstDayPreviousMonth,
        "end": yesterday
    })

    # Defines Redis keys.
    redisKeys = [
        "WebquotesPreviousMonth",
        "WebquotesCurrentMonth",
        "WebquotesTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        webquotesJson = webquotes.getPartialFromDateRange(val["start"], val["end"])
        updateRedisKeys(webquotesJson, redisKeys[i])
        logger.info(f"Redis keys {redisKeys[i]} updated...")

def updateWDTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month and last month
    for updating Redis keys for WebquotesDetails endpoint. """

    # Retrieves last date from database.
    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]

    # Defines date ranges.
    dateRanges = generateTwoMonthsDateRange(lastDate)
    firstDayPreviousMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    dateRanges.append({
        "start": firstDayPreviousMonth,
        "end": yesterday
    })

    # Defines Redis keys.
    redisKeys = [
        "WebquotesDetailsPreviousMonth",
        "WebquotesDetailsCurrentMonth",
        "WebquotesDetailsTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        webquotesDetailsJson = webquotes.getWebquotesFromDateRange(val["start"], val["end"])
        updateRedisKeys(webquotesDetailsJson, redisKeys[i])
        logger.info(f"Redis keys {redisKeys[i]} updated...")

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
    logger.info(f"Redis keys {redisKey} updated...")
