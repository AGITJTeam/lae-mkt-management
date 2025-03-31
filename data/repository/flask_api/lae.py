from data.repository.calls.customers_repo import Customers
from data.repository.calls.helpers import generateOneMonthDateRange, postDataframeToDb
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from service.customers import transformCustomersDfForLaeData
from service.lae_data import generateLaeData
from service.receipts_payroll import transformReceiptsDfForLaeData
import pandas as pd
from datetime import datetime, timedelta

def updateLaeDataTables(start: str, end: str) -> None:
    """ Updates Lae Data table in db with a date range.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receiptsPayroll = ReceiptsPayroll()
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start, end)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["customer_id"], inplace=True)
    print("ReceiptsPayroll table generated..")

    customersDf = getUniqueCustomersDf(receiptsPayrollDf)
    print("Customers table generated...")

    receipts = transformReceiptsDfForLaeData(receiptsPayrollDf)
    customers = transformCustomersDfForLaeData(customersDf)
    laeData = generateLaeData(receipts, customers)

    postDataframeToDb(data=laeData, table="lae_data", mode="append", filename="flask_api.ini")
    print("Lae Data table generated and posted...")

def getUniqueCustomersDf(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    """ Gets all data from unique customers.

    Parameteres
        receiptsDf {panda.DataFrame} Receipts Payroll DataFrame.

    """

    customers = Customers()

    customersIds = receiptsDf["customer_id"].tolist()
    customersJson = customers.getByIds(customersIds)
    customerDf = pd.DataFrame(customersJson)
    customersIdsNoDuplicates = customerDf.drop_duplicates("customer_id").reset_index(drop=True)

    return customersIdsNoDuplicates

def updateLaeDataTablesPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Lae Data
    table. """

    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    start, end = generateOneMonthDateRange(yesterday)

    lae = LaeData()
    lae.deleteLastMonthData(start, end)
    print(f"Lae data from {start} to {end} deleted...")
    
    updateLaeDataTables(start, end)
    print(f"Lae Data from {start} to {end} updated...")

def addLaeSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Lae table in db with an specific date range.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateLaeDataTables(start, end)
    print(f"Lae Data from {start} to {end} added...")
