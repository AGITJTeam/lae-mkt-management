from data.repository.calls.customers_repo import Customers
from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from service.customers import transformCustomersDfForLaeData
from service.lae_data import generateLaeData
from service.receipts_payroll import transformReceiptsDfForLaeData
import pandas as pd

def updateLaeDataTables(start: str, end: str) -> None:
    """ Updates Lae Data table in db with a date range.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receipts = ReceiptsPayroll()

    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    print("ReceiptsPayroll table generated..")

    customersDf = getUniqueCustomersDf(receiptsNoDuplicates)
    print("Customers table generated...")
    
    newReceipts = transformReceiptsDfForLaeData(receiptsDf)
    newCustomers = transformCustomersDfForLaeData(customersDf)
    laeData = generateLaeData(newReceipts, newCustomers)
    postDataframeToDb(laeData, "lae_data", "append")
    print("Lae Data table generated and posted...")

def updateLaeDataTablesPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Lae Data table. """

    receiptsPayroll = ReceiptsPayroll()
    lae = LaeData()

    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    
    dateRanges = generateTwoMonthsDateRange(lastDate)
    start = dateRanges[0]["start"]
    end = dateRanges[1]["end"]

    lae.deleteLastMonthData(start, end)
    print(f"Lae data from {start} to {end} deleted...")
    
    for dates in dateRanges:
        updateLaeDataTables(start=dates["start"], end=dates["end"])
        print(f"Lae Data from {dates["start"]} to {dates["end"]} updated...")

def addLaeSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Lae table in db with an specific date range.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateLaeDataTables(start, end)
    print(f"Lae Data from {start} to {end} added...")

def getUniqueCustomersDf(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    """ Gets all data from unique customers.

    Parameteres
        receiptsDf {panda.DataFrame} Receipts Payroll DataFrame.

    """

    customersIds = receiptsDf["customer_id"].tolist()
    customers = Customers()
    customersJson = customers.getByIds(customersIds)
    customerDf = pd.DataFrame(customersJson)
    customersIdsNoDuplicates = customerDf.drop_duplicates("customer_id").reset_index(drop=True)

    return customersIdsNoDuplicates
