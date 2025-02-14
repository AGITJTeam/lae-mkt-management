from data.repository.calls.helpers import postDataframeToDb
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from service.receipts import generateReceiptsDf
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import datetime
import pandas as pd

def updateReceiptsTable(receiptsPayrollDf: pd.DataFrame) -> None:
    """ Updates Receipts table in db.

    Parameters
        - receiptsPayrollDf {pandas.DataFrame} Receipts Payroll
        DataFrame.
    """

    receiptsDf = generateReceiptsDf(receiptsPayrollDf)
    postDataframeToDb(receiptsDf, "receipts", "append")

def addReceiptsTodayRecords() -> None:
    """ Generates today's date to add to Receipts table. """

    today = datetime.today().date().isoformat()
    receiptsPayrollDf = generateReceiptsPayrollDf(start=today, end=today)
    rpNoDuplicates = receiptsPayrollDf.drop_duplicates("id_receipt_hdr")
    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {today} added...")

def updateReceiptsYesterdayRecords() -> None:
    """ Update Receipts yesterday records. """

    receiptsPayroll = ReceiptsPayroll()
    receipts = Receipts()

    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    date = lastDateFromTable.date().isoformat()
    receiptsJson = receipts.getBetweenDates(start=date, end=date)
    
    if len(receiptsJson) == 0:
        print(f"No data from {date} to update.")
        raise Exception("No data found")
    
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsIds = receiptsDf["id_receipt_hdr"].tolist()
    receipts.deleteByIds(receiptsIds)

    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=date, end=date)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    rpNoDuplicates = receiptsPayrollDf.drop_duplicates("id_receipt_hdr")

    print(f"Receipts data from {date} to {date} deleted...")
    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {date} to {date} updated...")

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
