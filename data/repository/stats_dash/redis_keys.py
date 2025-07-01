from data.repository.stats_dash.dash_final_sales import dashFinalSales
from data.repository.stats_dash.pvc import dashPvc
from data.repository.stats_dash.dash_os import dashOs
from data.repository.stats_dash.top_carriers import topCarriers
from data.repository.stats_dash.out_of_state import outOfState
from datetime import datetime, timedelta
import redis, logging, json

logger = logging.getLogger(__name__)

def updateRedisKeys(redisKey: str, data: dict) -> None:
    """ Updates Redis keys with given dictionary.

    Parameters
        - redisKey {str} the Redis key to update.
        - data {list[dict} the data already formatted.
    """

    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
    redisCli.hset(name=redisKey, mapping=data)
    redisCli.close()

def updateFinalSalesKey() -> None:
    """ Updates 'FinalSalesCurrentMonth' Redis Key for the Final
    Sales Report.
    """

    # Defines dates for calculating Final Sales.
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    startOfWeek = today - timedelta(days=today.weekday())
    lastDayPreviousWeek = startOfWeek - timedelta(days=1)
    firstDayPreviousWeek = lastDayPreviousWeek - timedelta(days=6)

    # Parse dates to string.
    startStr = firstDayPreviousWeek.isoformat()
    endStr = lastDayPreviousWeek.isoformat()
    yesterdayStr = yesterday.isoformat()

    # Calculate Final Sales.
    yesterdayData, lastWeekData = dashFinalSales(
        startStr,
        endStr,
        yesterdayStr
    )

    # Define Redis key and values for Hash type.
    redisKeys = "FinalSalesCurrentMonth"
    hashValues = {
        "yesterdayData": json.dumps(yesterdayData),
        "lastWeekData": json.dumps(lastWeekData)
    }

    # Update Redis key.
    updateRedisKeys(redisKeys, hashValues)
    logger.info("Redis key 'FinalSalesCurrentMonth' updated...")

def updatePvcKey() -> None:
    """ Updates 'PvcCurrentMonth' Redis Key for the Pvc Report. """

    # Calculate Pvc.
    yesterdayData, lastWeekData = dashPvc()

    # Define Redis key and values for Hash type.
    redisKeys = "PvcCurrentMonth"
    hashValues = {
        "yesterdayData": json.dumps(yesterdayData),
        "lastWeekData": json.dumps(lastWeekData)
    }

    # Update Redis key.
    updateRedisKeys(redisKeys, hashValues)
    logger.info("Redis key 'PvcCurrentMonth' updated...")

def updateOnlineSalesKey() -> None:
    """ Updates 'OnlineSalesCurrentMonth' Redis Key for the Online
    Sales Report. """

    # Defines dates for calculating Online sales.
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)

    # Parse dates to string.
    startStr = firstDayCurrentMonth.isoformat()
    endStr = today.isoformat()

    # Calculate Online Sales.
    companySales, totalSums = dashOs(startStr, endStr)

    # Define Redis key and values for Hash type.
    redisKeys = "OnlineSalesCurrentMonth"
    hashValues = {
        "daily_data": json.dumps(companySales),
        "total_data": json.dumps(totalSums)
    }

    # Update Redis key.
    updateRedisKeys(redisKeys, hashValues)
    logger.info("Redis key 'OnlineSalesCurrentMonth' updated...")

def updateTopCarriersKey() -> None:
    """ Updates 'TopCarriersCurrentMonth' Redis Key for the Top
    Carriers Report. """

    # Defines dates for calculating Top Carriers.
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)

    # Parse dates to string.
    startStr = firstDayCurrentMonth.isoformat()
    endStr = today.isoformat()
    

    # Calculate Top Carriers.
    companySales, companySalesOffices, totalSums = topCarriers(startStr, endStr)

    # Define Redis key and values for Hash type.
    redisKeys = "TopCarriersCurrentMonth"
    hashValues = {
        "daily_data": json.dumps(companySales),
        "daily_data_office": json.dumps(companySalesOffices),
        "total_data": json.dumps(totalSums)
    }

    # Update Redis key.
    updateRedisKeys(redisKeys, hashValues)
    logger.info("Redis key 'TopCarriersCurrentMonth' updated...")

def updateOutOfStateKey() -> None:
    """ Updates 'OutOfStateCurrentMonth' Redis Key for the Out Of State Report. """

    # Defines dates for calculating Out Of State.
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    firstDayCurrentMonth = today.replace(day=1)

    # Parse dates to string.
    startStr = firstDayCurrentMonth.isoformat()
    endStr = yesterday.isoformat()

    # Calculate Out Of State.
    dailyData = outOfState(startStr, endStr)

    # Define Redis key and values for Hash type.
    redisKeys = "OutOfStateCurrentMonth"
    hashValues = {
        "daily_data": json.dumps(dailyData)
    }

    # Update Redis key.
    updateRedisKeys(redisKeys, hashValues)
    logger.info("Redis key 'OutOfStateCurrentMonth' updated...")
