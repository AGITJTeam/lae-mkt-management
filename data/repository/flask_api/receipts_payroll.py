from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from service.receipts_payroll import generateReceiptsPayrollDf
from datetime import datetime

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

def updateReceiptsPayrollPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Receipts
    Payroll table.
    """

    receipts = ReceiptsPayroll()
    
    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dateRanges = generateTwoMonthsDateRange(lastDate)
    
    dataAvailable = any(
        not generateReceiptsPayrollDf(date["start"], date["end"]).empty
        for date in dateRanges
    )
    
    if not dataAvailable:
        raise Exception(f"No data from {dateRanges[0]['start']} to {dateRanges[0]['end']} to update.")
    
    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    receipts.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Receipts Payroll data from {firstDayLastMonth} to {yesterday} deleted...")

    for date in dateRanges:
        updateReceiptsPayrollTable(date["start"], date["end"])
        print(f"Receipts Payroll data from {date["start"]} to {date["end"]} updated...")

def addReceiptsPayrollSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Receipts Payroll table in vm with an specific
    date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateReceiptsPayrollTable(start, end)
    print(f"Receipts Payroll data from {start} to {end} added...")
