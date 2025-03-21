from data.repository.calls.helpers import postDataframeToDb
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from service.receipts import generateReceiptsDf
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import timedelta
import json, pandas as pd, redis

def updateReceiptsTable(receiptsPayrollDf: pd.DataFrame) -> None:
    """ Updates Receipts table in db with a date range.

    Parameters
        - receiptsPayrollDf {pandas.DataFrame} Receipts Payroll
        DataFrame.
    """

    receiptsDf = generateReceiptsDf(receiptsPayrollDf)
    postDataframeToDb(
        data=receiptsDf,
        table="receipts",
        mode="append",
        filename="flask_api.ini"
    )

def updateRedisKey() -> None:
    """ Updates Redis keys with given DataFrame. """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Defines date ranges.
    startDate, endDate = genOneMonthDateRange()

    # Generates data for date range.
    receiptsPayroll = ReceiptsPayroll()    
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(startDate, endDate)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)

    # Defines expiration time, redis Key and formatted data.
    expirationTime = 60*60*10
    redisKey = "ReceiptsCurrentMonth"
    data = json.dumps(obj=receiptsPayrollDf, default=str)

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=data, ex=expirationTime)

    # Close Redis connection.
    redisCli.close()

def addReceiptsSpecificRange(start: str, end: str) -> None:
    """ Add data to Receipts table with a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    # Generates data for date range and delete duplicated IdReceiptHdr records.
    receiptsPayrollDf = generateReceiptsPayrollDf(start, end)
    rpNoDuplicates = receiptsPayrollDf.drop_duplicates("id_receipt_hdr")

    # Updated Receipts table.
    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {start} to {end} added...")

def updateReceiptsPreviousRecords() -> None:
    """ Update Receipts yesterday records. """

    # Defines yesterday and today dates.
    receiptsPayroll = ReceiptsPayroll()
    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    todayDate = lastDateFromTable.date()
    yesterdayDate = todayDate - timedelta(days=1)
    today = todayDate.isoformat()
    yesterday = yesterdayDate.isoformat()

    # Generates data from yesterday to today and delete duplicated IdReceiptHdr records.
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=yesterday, end=today)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)

    # Convert data to list.
    receiptsIds = receiptsPayrollDf["id_receipt_hdr"].tolist()

    # Delete data that will be updated.
    receipts = Receipts()
    receipts.deleteByIds(receiptsIds)
    print(f"Receipts data from {yesterday} to {today} deleted...")

    # Updated Receipts table.
    updateReceiptsTable(receiptsPayrollDf)
    print(f"Receipts data from {yesterday} to {today} updated...")

def genOneMonthDateRange() -> tuple[str, str]:
    """ Generates one month date range to delete and/or update Receipts
    table.

    Returns
        {tuple[str, str]} date ranges to delete and/or update.
    """

    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    firstDayCurrentMonth = lastDate.replace(day=1)

    startDate = firstDayCurrentMonth.date().isoformat()
    endDate = lastDate.date().isoformat()

    return startDate, endDate
