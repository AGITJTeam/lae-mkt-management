from data.repository.calls.helpers import postDataframeToDb
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from service.receipts import generateReceiptsDf
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import timedelta
import pandas as pd
import redis, json

def updateReceiptsTable(receiptsPayrollDf: pd.DataFrame) -> None:
    """ Updates Receipts table in db.

    Parameters
        - receiptsPayrollDf {pandas.DataFrame} Receipts Payroll
        DataFrame.
    """

    receiptsDf = generateReceiptsDf(receiptsPayrollDf)
    postDataframeToDb(data=receiptsDf, table="receipts", mode="append", filename="flask_api.ini")

def addReceiptsSpecificRange(start: str, end: str) -> None:
    """ Add data to Receipts table in db with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receiptsPayrollDf = generateReceiptsPayrollDf(start, end)
    rpNoDuplicates = receiptsPayrollDf.drop_duplicates("id_receipt_hdr")

    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {start} to {end} added...")

def updateReceiptsPreviousRecords() -> None:
    """ Update Receipts yesterday records. """

    receiptsPayroll = ReceiptsPayroll()
    receipts = Receipts()

    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    todayDate = lastDateFromTable.date()
    yesterdayDate = todayDate - timedelta(days=1)
    today = todayDate.isoformat()
    yesterday = yesterdayDate.isoformat()

    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=yesterday, end=today)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)
    receiptsIds = receiptsPayrollDf["id_receipt_hdr"].tolist()

    receipts.deleteByIds(receiptsIds)
    print(f"Receipts data from {yesterday} to {today} deleted...")

    updateReceiptsTable(receiptsPayrollDf)
    print(f"Receipts data from {yesterday} to {today} updated...")

def updateRedisKey() -> None:
    """ Updates Redis keys with the last date of Receipts table. """

    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    startDate, endDate = genOneMonthDateRange()
    receiptsPayroll = ReceiptsPayroll()
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=startDate, end=endDate)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)

    expirationTime = 60*60*10
    redisKey = "ReceiptsCurrentMonth"
    data = json.dumps(obj=receiptsPayrollDf, default=str)
    redisCli.set(name=redisKey, value=data, ex=expirationTime)
    redisCli.close()

def genOneMonthDateRange() -> tuple[str, str]:
    """ Generates one month date range to delete and/or update Receipts
    table.

    Returns
        {tuple[str, str]} date ranges to delete and/or update.
    """

    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    firstDayCurrentMonth = lastDate.replace(day=1)

    startDate = firstDayCurrentMonth.isoformat()
    endDate = lastDate.isoformat()

    return startDate, endDate
