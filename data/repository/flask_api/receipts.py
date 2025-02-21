from data.repository.calls.helpers import postDataframeToDb
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from service.receipts import generateReceiptsDf
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import timedelta
import pandas as pd

def updateReceiptsTable(receiptsPayrollDf: pd.DataFrame) -> None:
    """ Updates Receipts table in db.

    Parameters
        - receiptsPayrollDf {pandas.DataFrame} Receipts Payroll
        DataFrame.
    """

    receiptsDf = generateReceiptsDf(receiptsPayrollDf)
    postDataframeToDb(data=receiptsDf, table="receipts", mode="append", filename="flask_api.ini")

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
