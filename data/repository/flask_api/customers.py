from data.repository.calls.customers_repo import Customers
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import generateOneWeekDateRange, postData
from service.customers import generateCustomersDf
import pandas as pd

def updateCustomersTable(receiptsDf: pd.DataFrame) -> None:
    """ Updates Customers table in vm with updated Receipts Payroll table.

    Parameteres
        - receiptsDf {pandas.DataFrame} Receipts Payroll DataFrame.
    """

    customersDf = generateCustomersDf(receiptsDf)
    postData(customersDf, "customers", "append")

def updateCustomersPreviousRecords() -> None:
    """ Generate Customers ids to delete with Receipts DataFrame. """

    receipts = ReceiptsPayroll()
    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dates = generateOneWeekDateRange(lastDate)

    receiptsJson = receipts.getBetweenDates(dates["start"], dates["end"])
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    customersIds = receiptsNoDuplicates["customer_id"].tolist()

    customers = Customers()
    customers.deleteByIds(customersIds)
    print(f"Customers data from {dates["start"]} to {dates["end"]} deleted...")
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {dates["start"]} to {dates["end"]} updated...")

def addCustomersSpecificRange(start: str, end: str) -> None:
    """ Add data to Customers table in vm with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receipts = ReceiptsPayroll()
    customers = Customers()

    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    customersIds = receiptsNoDuplicates["customer_id"].tolist()

    customers.deleteByIds(customersIds)
    print(f"Customers data from {start} to {end} deleted...")
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {start} to {end} added...")
