from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import (
    generateOneMonthDateRange,
    generateTwoMonthsDateRange,
    postDataframeToDb
)
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
    if type(rawData) == list:
        data = json.dumps(obj=rawData, default=str)
        rawData = data

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=rawData, ex=expirationTime)

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
    date = receiptsPayroll.getLastRecord()[0]["date"]
    lastDate = date.date()
    start, end = generateOneMonthDateRange(lastDate)

    # Delete data that will be updated.
    receipts = ReceiptsPayroll()
    receipts.deleteLastMonthData(start, end)
    print(f"Receipts Payroll data from {start} to {end} deleted...")

    # Generates data for current month and updates Receipts Payroll table.
    updateReceiptsPayrollTable(start, end) 
    print(f"Receipts Payroll data from {start} to {end} updated...")

def addReceiptsPayrollSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Receipts Payroll table in a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateReceiptsPayrollTable(start, end)
    print(f"Receipts Payroll data from {start} to {end} added...")

def updateTwoMonthsRedisKeys() -> None:
    """ Generate date range for current month and last month
    for updating Redis keys for ReceiptsPayroll endpoint. """

    # Retrieves last date from database.
    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    date = lastDate.date()

    # Defines date ranges.
    dateRanges = generateTwoMonthsDateRange(date)
    firstDayPreviousMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    dateRanges.append({
        "start": firstDayPreviousMonth,
        "end": yesterday
    })

    # Defines Redis keys.
    redisKeys = [
        "ReceiptsPayrollPreviousMonth",
        "ReceiptsPayrollCurrentMonth",
        "ReceiptsPayrollTwoMonths"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        receiptsPayrollJson = receiptsPayroll.getBetweenDates(val["start"], val["end"])
        updateRedisKeys(receiptsPayrollJson, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
