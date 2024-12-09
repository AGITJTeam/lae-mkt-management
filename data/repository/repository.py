from data.repository.calls.helpers import postData, generateStartAndEndDates, generateTwoMonthsDateRange, generateOneMonthsDateRange
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.webquotes_repo import Webquotes

from service.helpers import getCustomersIds
from service.employees import generateEmployeesDf
from service.receipts_payroll import generateReceiptsPayrollDf, transformReceiptsDfForLaeData
from service.customers import generateCustomersDf, transformCustomersDfForLaeData
from service.lae_data import generateLaeData
from service.webquotes import generateWebquotesDf
from service.policies_details import generatePoliciesDf, deleteColumnWithListValues
from service.vehicles_insured import generateVehiclesDf
from service.policies_dtl import generatePoliciesDtlDf

import pandas as pd
from datetime import date

""" Updates Employees table in vm with Lae Employees endpoint response. """
def updateEmployeesTable() -> None:
    employeesDf = generateEmployeesDf()
    postData(employeesDf, "employees", "replace")
    print("Employees table generated and posted...")


""" Updates Customers table in vm with updated Receipts Payroll table.

Parameteres
    receiptsDf {pandas.DataFrame} Receipts Payroll DataFrame.

"""
def updateCustomersTable(receiptsDf: pd.DataFrame) -> None:
    customersDf = generateCustomersDf(receiptsDf)
    postData(customersDf, "customers", "append")

""" Generate Customers ids to delete with Receipts DataFrame. """
def updateCustomersPreviousRecords() -> None:
    receipts = ReceiptsPayroll()
    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dates = generateOneMonthsDateRange(lastDate)

    receiptsJson = receipts.getBetweenDates(dates["start"], dates["end"])
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    customersIds = receiptsNoDuplicates["customer_id"].tolist()

    customers = Customers()
    customers.deleteCurrentMonthData(customersIds)
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {dates["start"]} to {dates["end"]} updated...")


""" Updates Receipts Payroll table in vm with a date range.

Parameters
    - start {str} beginning of the range.
    - end {str} end of the range.

"""
def updateReceiptsPayrollTable(start: str, end: str) -> None:
    receiptsDf = generateReceiptsPayrollDf(start, end)
    postData(receiptsDf, "receipts_payroll", "append")

""" Generates today's date to add to Receipts Payroll table. """
def addReceiptsTodayRecords() -> None:
    today = date.today().isoformat()
    updateReceiptsPayrollTable(start=today, end=today)
    print(f"Receipts Payroll data from {today} added...")

"""
    Generate last and current month date ranges to update Receipts
    Payroll table.
"""
def updateReceiptsPreviousRecords() -> None:
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

""" Add data to Receipts Payroll table in vm with an specific date range.

Parameters
    - start {str} beginning of the range.
    - end {str} end of the range.

"""
def addReceiptsSpecificDateRange(start: str, end: str) -> None:
    updateReceiptsPayrollTable(start, end)
    print(f"Receipts Payroll data from {start} to {end} added...")


""" Updates Lae Data table in vm with a date range. Call LAE API if
    new records are added, call vm table if previous records are updated.

Parameteres
    - start {str} beginning of the range.
    - end {str} end of the range.
    - updateCustomers {bool} indicates if new records are added.

"""
def updateLaeDataTables(start: str, end: str, updateCustomers: bool) -> None:
    receipts = ReceiptsPayroll()

    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    print("ReceiptsPayroll table generated..")

    if updateCustomers:
        customersDf = generateCustomersDf(receiptsNoDuplicates)
        postData(customersDf, "customers", "append")
        print("Customers table generated and posted...")
    else:
        customersDf = getCustomersDfById(receiptsNoDuplicates)
        print("Customers table generated...")
    
    newReceipts = transformReceiptsDfForLaeData(receiptsDf)
    newCustomers = transformCustomersDfForLaeData(customersDf)
    laeData = generateLaeData(newReceipts, newCustomers)
    postData(laeData, "lae_data", "append")
    print("Lae Data table generated and posted...")

""" Generates today's date to add to Receipts Payroll table. """
def addLaeDataTablesTodayRecords() -> None:
    today = date.today().isoformat()
    updateLaeDataTables(start=today, end=today, updateCustomers=True)
    print(f"Lae data from {today} added...")

""" Generate last and current month date ranges to update Lae Data table. """
def updateLaeDataTablesPreviousRecords() -> None:
    lae = LaeData()

    lastDateFromTable = lae.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    
    dateRanges = generateTwoMonthsDateRange(lastDate)
    start, end = generateStartAndEndDates(lastDate)
    lae.deleteLastMonthData(start, end)
    print(f"Lae data from {start} to {end} deleted...")
    
    for dates in dateRanges:
        updateLaeDataTables(dates["start"], dates["end"], False)
        print(f"Lae Data from {dates["start"]} to {dates["end"]} updated...")

""" Add data to Lae table in vm with an specific date range.

Parameteres
    - start {str} beginning of the range.
    - end {str} end of the range.

"""
def addLaeSpecificDateRange(start: str, end: str) -> None:
    updateLaeDataTables(start, end, True)
    print(f"Lae Data from {start} to {end} added...")

""" Gets unique customers ids from vm table.

Parameteres
    receiptsDf {panda.DataFrame} Receipts Payroll DataFrame.

"""
def getCustomersDfById(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    customersIds = receiptsDf["customer_id"].tolist()
    customers = Customers()
    customersJson = customers.getByIds(customersIds)
    customerDf = pd.DataFrame(customersJson)
    customersIdsNoDuplicates = customerDf.drop_duplicates("customer_id").reset_index(drop=True)

    return customersIdsNoDuplicates


""" Updates Webquotes table in vm with a date range.

Parameters
    - start {str} beginning of the range.
    - end {str} end of the range.

"""
def updateWebquotesTables(start: str, end: str) -> None:
    webquotesDf = generateWebquotesDf(start, end)
    postData(webquotesDf, "webquotes", "append")

""" Generates today's date to add to Webquotes table. """
def addWebquotesTodayRecords() -> None:
    today = date.today().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

""" Generate last and current month date ranges to update Webquotes table. """
def updateWebquotesPreviousRecords() -> None:
    webquotes = Webquotes()

    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    
    dateRanges = generateTwoMonthsDateRange(lastDate)
    start, end = generateStartAndEndDates(lastDate)
    webquotes.deleteLastMonthData(start, end)
    print(f"Webquotes data from {start} to {end} deleted...")

    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

""" Add data to Webquotes table in vm with an specific date range.

Parameters
    - start {str} beginning of the range.
    - end {str} end of the range.

"""
def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")


def updatePoliciesTables(start: str, end: str) -> None:
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
