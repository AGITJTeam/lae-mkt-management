from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.webquotes_repo import Webquotes
from data.repository.calls.helpers import (
    postData,
    generateStartAndEndDates,
    generateOneWeekDateRange,
    generateTwoMonthsDateRange
)

from service.employees import generateEmployeesDf
from service.receipts_payroll import generateReceiptsPayrollDf, transformReceiptsDfForLaeData
from service.customers import generateCustomersDf, transformCustomersDfForLaeData
from service.lae_data import generateLaeData
from service.webquotes import generateWebquotesDf
from service.policies_details import generatePoliciesDf, deleteColumnWithListValues
from service.vehicles_insured import generateVehiclesDf
from service.policies_dtl import generatePoliciesDtlDf
from service.receipts import generateReceiptsDf

from datetime import datetime
import pandas as pd

def updateEmployeesTable() -> None:
    """ Updates Employees table in vm with Lae Employees endpoint response. """

    employeesDf = generateEmployeesDf()
    postData(employeesDf, "employees", "replace")
    print("Employees table generated and posted...")


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


def updateReceiptsPayrollTable(start: str, end: str) -> None:
    """ Updates Receipts Payroll table in vm with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receiptsDf = generateReceiptsPayrollDf(start, end)
    postData(receiptsDf, "receipts_payroll", "append")

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
    start, end = generateStartAndEndDates(lastDate)
    receipts.deleteLastMonthData(start, end)
    print(f"Receipts Payroll data from {start} to {end} deleted...")

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


def updateLaeDataTables(start: str, end: str) -> None:
    """ Updates Lae Data table in vm with a date range. Call LAE API if
    new records are added, call vm table if previous records are updated.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receipts = ReceiptsPayroll()

    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    print("ReceiptsPayroll table generated..")

    customersDf = getCustomersDfById(receiptsNoDuplicates)
    print("Customers table generated...")
    
    newReceipts = transformReceiptsDfForLaeData(receiptsDf)
    newCustomers = transformCustomersDfForLaeData(customersDf)
    laeData = generateLaeData(newReceipts, newCustomers)
    postData(laeData, "lae_data", "append")
    print("Lae Data table generated and posted...")

def updateLaeDataTablesPreviousRecords() -> None:
    """ Generate last and current month date ranges to update Lae Data table. """

    receiptsPayroll = ReceiptsPayroll()
    lae = LaeData()

    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    
    dateRanges = generateTwoMonthsDateRange(lastDate)
    start, end = generateStartAndEndDates(lastDate)
    lae.deleteLastMonthData(start, end)
    print(f"Lae data from {start} to {end} deleted...")
    
    for dates in dateRanges:
        updateLaeDataTables(start=dates["start"], end=dates["end"])
        print(f"Lae Data from {dates["start"]} to {dates["end"]} updated...")

def addLaeSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Lae table in vm with an specific date range.

    Parameteres
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateLaeDataTables(start, end)
    print(f"Lae Data from {start} to {end} added...")

def getCustomersDfById(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    """ Gets unique customers ids from vm table.

    Parameteres
        receiptsDf {panda.DataFrame} Receipts Payroll DataFrame.

    """

    customersIds = receiptsDf["customer_id"].tolist()
    customers = Customers()
    customersJson = customers.getByIds(customersIds)
    customerDf = pd.DataFrame(customersJson)
    customersIdsNoDuplicates = customerDf.drop_duplicates("customer_id").reset_index(drop=True)

    return customersIdsNoDuplicates


def updateWebquotesTables(start: str, end: str) -> None:
    """ Updates Webquotes table in vm with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    webquotesDf = generateWebquotesDf(start, end)
    postData(webquotesDf, "webquotes", "append")

def addWebquotesTodayRecords() -> None:
    """ Generates today's date to add to Webquotes table. """
    today = datetime.today().date().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

def updateWebquotesPreviousRecords() -> None:
    """ Generate last and current month date ranges to update
    Webquotes table. """

    webquotes = Webquotes()

    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    
    dateRanges = generateTwoMonthsDateRange(lastDate)
    start, end = generateStartAndEndDates(lastDate)
    webquotes.deleteLastMonthData(start, end)
    print(f"Webquotes data from {start} to {end} deleted...")

    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in vm with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")


def updatePoliciesTables(start: str, end: str) -> None:
    """ Updates Policies and all sub-tables in vm.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receipts = ReceiptsPayroll()
    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    print("ReceiptsPayroll table generated..")

    policiesDf = generatePoliciesDf(receiptsNoDuplicates)
    policiesNoDuplicates = policiesDf.drop_duplicates("vehicle_insureds")

    vehiclesInsured = policiesNoDuplicates["vehicle_insureds"]
    vehiclesDf = generateVehiclesDf(vehiclesInsured)
    print("VehiclesInsured table generated")

    policiesDtl = policiesNoDuplicates["policies_dtl"]
    policiesDtlDf = generatePoliciesDtlDf(policiesDtl)
    print("PoliciesDTL table generated...")

    newPoliciesDf = deleteColumnWithListValues(policiesDf)
    print("Policies Details table generated...")

    postData(vehiclesDf, "vehicles_insured", "append")
    postData(policiesDtlDf, "policies_dtl", "append")
    postData(newPoliciesDf, "policies_details", "append")
    print("All tables posted...")


def updateReceiptsTable(receiptsPayrollDf: pd.DataFrame) -> None:
    """ Updates Receipts table in vm.

    Parameters
        - receiptsPayrollDf {pandas.DataFrame} Receipts Payroll
        DataFrame.
    """

    receiptsDf = generateReceiptsDf(receiptsPayrollDf)
    postData(receiptsDf, "receipts", "append")

def addReceiptsTodayRecords() -> None:
    """ Generates today's date to add to Receipts table. """

    today = datetime.today().date().isoformat()
    receiptsPayrollDf = generateReceiptsPayrollDf(start=today, end=today)
    updateReceiptsTable(receiptsPayrollDf)
    print(f"Receipts Payroll data from {today} added...")

def updateReceiptsPreviousRecords() -> None:
    """ Update Receipts previous records. """

    receipts = Receipts()    

    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dates = generateOneWeekDateRange(lastDate)

    receiptsJson = receipts.getBetweenDates(dates["start"], dates["end"])
    receiptsDf = pd.DataFrame(receiptsJson)
    rpNoDuplicates = receiptsDf.drop_duplicates("id_receipt_hdr")
    receiptsIds = rpNoDuplicates["id_receipt_hdr"].tolist()

    receipts.deleteByIds(receiptsIds)
    print(f"Receipts data from {dates["start"]} to {dates["end"]} deleted...")
    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {dates["start"]} to {dates["end"]} updated...")

def addReceiptsSpecificRange(start: str, end: str) -> None:
    """ Add data to Receipts table in vm with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """
    
    receiptsPayrollDf = generateReceiptsPayrollDf(start, end)
    rpNoDuplicates = receiptsPayrollDf.drop_duplicates("id_receipt_hdr")

    updateReceiptsTable(rpNoDuplicates)
    print(f"Receipts data from {start} to {end} added...")
