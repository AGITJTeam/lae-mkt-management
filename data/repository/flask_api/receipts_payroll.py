from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import datetime
import json, redis

def updateReceiptsPayrollTable(start: str, end: str) -> None:
    """ Updates Receipts Payroll table in db with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receiptsDf = generateReceiptsPayrollDf(start, end)
    postDataframeToDb(data=receiptsDf, table="receipts_payroll", mode="append", filename="flask_api.ini")

def addReceiptsPayrollTodayRecords() -> None:
    """ Generates today's date to add to Receipts Payroll table. """

    today = datetime.today().date().isoformat()
    updateReceiptsPayrollTable(start=today, end=today)
    print(f"Receipts Payroll data from {today} added...")

def addReceiptsPayrollSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Receipts Payroll table in vm with an specific
    date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateReceiptsPayrollTable(start, end)
    print(f"Receipts Payroll data from {start} to {end} added...")

def updateReceiptsPayrollPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Receipts
    Payroll table.
    """

    dateRanges = genReceiptsPayrollDateRange()

    receipts = ReceiptsPayroll()
    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    receipts.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Receipts Payroll data from {firstDayLastMonth} to {yesterday} deleted...")

    for date in dateRanges:
        updateReceiptsPayrollTable(date["start"], date["end"])
        print(f"Receipts Payroll data from {date["start"]} to {date["end"]} updated...")

def genReceiptsPayrollDateRange() -> list[dict[str, str]] | None:
    """ Generates date range to delete and update Receipts Payroll table.
    
    Returns
        {list[dict[str, str]] | None} date ranges to delete and update
        or None if exception raise.
    """

    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)

    dataAvailable = any(
        not generateReceiptsPayrollDf(date["start"], date["end"]).empty
        for date in dateRanges
    )

    if not dataAvailable:
        raise Exception(f"No data from {dateRanges[0]['start']} to {dateRanges[0]['end']} to update.")

    return dateRanges

def updateRedisKeys(start: str, end: str, redisKey: str) -> None:
    """ Updates Redis keys with the last date of Receipts Payroll table.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
        - redisKey {str} the Redis key to update.
    """

    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)
    receiptsPayrollDf = generateReceiptsPayrollDf(start, end)
    expirationTime = 60*60*10
    data = json.dumps(obj=receiptsPayrollDf, default=str)
    redisCli.set(name=redisKey, value=data, ex=expirationTime)
    redisCli.close()

def updateRedisPreviousRecords() -> None:
    """ Generate last and current month date ranges to update
    Redis keys for Receipts Payroll table. """

    dateRanges = genReceiptsPayrollDateRange()
    redisKeys = ["ReceiptsPayrollLastMonth", "ReceiptsPayrollCurrentMonth"]

    for i, val in enumerate(dateRanges):
        updateRedisKeys(val["start"], val["end"], redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
