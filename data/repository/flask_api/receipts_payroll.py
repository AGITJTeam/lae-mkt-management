from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import datetime
import json, pandas as pd, redis

def updateReceiptsPayrollTable(start: str, end: str) -> None:
    """ Updates Receipts Payroll table in db with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receiptsDf = generateReceiptsPayrollDf(start, end)
    postDataframeToDb(
        data=receiptsDf,
        table="receipts_payroll",
        mode="append",
        filename="flask_api.ini"
    )

def updateRedisKeys(rawData: pd.DataFrame, redisKey: str) -> None:
    """ Updates Redis keys with given DataFrame.
    
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

def addReceiptsPayrollTodayRecords() -> None:
    """ Add today's date data to Receipts Payroll table. """

    today = datetime.today().date().isoformat()
    updateReceiptsPayrollTable(start=today, end=today)
    print(f"Receipts Payroll data from {today} added...")

def updateReceiptsPayrollPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Receipts
    Payroll table. """

    # Defines date ranges.
    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]

    # Delete data that will be updated.
    receipts = ReceiptsPayroll()
    receipts.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Receipts Payroll data from {firstDayLastMonth} to {yesterday} deleted...")

    # Generates data for every date range and updates Webquotes table.
    for date in dateRanges:
        updateReceiptsPayrollTable(date["start"], date["end"])
        print(f"Receipts Payroll data from {date["start"]} to {date["end"]} updated...")

def addReceiptsPayrollSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Receipts Payroll table in a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateReceiptsPayrollTable(start, end)
    print(f"Receipts Payroll data from {start} to {end} added...")

def updateTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month, last month and both
    months for updating Redis keys for ReceiptsPayroll endpoint. """

    # Defines date ranges and Redis keys.
    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    firstDayLastMonth = dateRanges[0]["start"]
    today = dateRanges[1]["end"]
    dateRanges.append({
        "start": firstDayLastMonth,
        "end": today
    })
    redisKeys = [
        "ReceiptsPayrollLastMonth",
        "ReceiptsPayrollCurrentMonth",
        "ReceiptsPayrollTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        receiptsPayrollDf = generateReceiptsPayrollDf(val["start"], val["end"])
        updateRedisKeys(receiptsPayrollDf, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
