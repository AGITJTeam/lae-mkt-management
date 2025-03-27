from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
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

def updateRedisKeys(rawData: pd.DataFrame, redisKey: str) -> None:
    """ Updates Redis keys with given DataFrame.
    
    Parameters
        - data {pandas.DataFrame} the data to update.
        - redisKey {str} the Redis key to update.
    """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Defines expiration time, redis Key and formatted data.
    expirationTime = 60*60*10
    data = json.dumps(obj=rawData, default=str)

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
    """ Update Receipts todays records. """

    # Defines todays date.
    receiptsPayroll = ReceiptsPayroll()
    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    todayDate = lastDateFromTable.date()
    #yesterdayDate = todayDate - timedelta(days=1)
    today = todayDate.isoformat()
    #yesterday = yesterdayDate.isoformat()

    # Generates data from today and delete duplicated IdReceiptHdr records.
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=today, end=today)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)

    # Convert data to list.
    receiptsIds = receiptsPayrollDf["id_receipt_hdr"].tolist()

    # Delete data that will be updated.
    receipts = Receipts()
    receipts.deleteByIds(receiptsIds)
    print(f"Receipts data from {today} to {today} deleted...")

    # Updated Receipts table.
    updateReceiptsTable(receiptsPayrollDf)
    print(f"Receipts data from {today} to {today} updated...")

def updateTwoMonthsRedisKeys() -> None:
    """ Generate date range for current mont and last month
    for updating Redis keys for Webquotes endpoint. """

    # Defines date ranges and Redis keys.
    receiptsPayroll = ReceiptsPayroll()
    lastDate = receiptsPayroll.getLastRecord()[0]["date"]
    date = lastDate.date()
    dateRanges = generateTwoMonthsDateRange(date)
    redisKeys = [
        "ReceiptsPreviousMonth",
        "ReceiptsCurrentMonth"
    ]

    # Generates data for every date range and updates Redis keys.
    for i, val in enumerate(dateRanges):
        receiptsPayrollJson = receiptsPayroll.getBetweenDates(val["start"], val["end"])
        receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
        receiptsPayrollDf.drop_duplicates(subset=["id_receipt_hdr"], inplace=True)
        receiptsJson = receiptsPayrollDf.to_json(orient="records")
        updateRedisKeys(receiptsJson, redisKeys[i])
        print(f"Redis keys {redisKeys[i]} updated...")
