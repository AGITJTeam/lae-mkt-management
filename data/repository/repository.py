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
from service.logs import generateLogsDf

import pandas as pd
from datetime import date, datetime

def updateEmployeesTable() -> None:
    employeesDf = generateEmployeesDf()
    postData(employeesDf, "employees", "replace")
    print("Employees table generated and posted...")

def updateCustomersTable(receiptsDf: pd.DataFrame) -> None:
    customersDf = generateCustomersDf(receiptsDf)
    postData(customersDf, "customers", "append")

def updateCustomersPreviousRecords() -> None:
    receipts = ReceiptsPayroll()
    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dates = generateOneMonthsDateRange(lastDate)

    receiptsJson = receipts.getBetweenDates(dates["start"], dates["end"])
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    customersIds = getCustomersIds(receiptsNoDuplicates)

    customers = Customers()
    customers.deleteCurrentMonthData(customersIds)
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {dates["start"]} to {dates["end"]} updated...")

def updateReceiptsPayrollTable(start: str, end: str) -> None:
    receiptsDf = generateReceiptsPayrollDf(start, end)
    postData(receiptsDf, "receipts_payroll", "append")

def addReceiptsTodayRecords() -> None:
    today = date.today().isoformat()
    updateReceiptsPayrollTable(start=today, end=today)
    print(f"Receipts Payroll data from {today} added...")

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

def addLaeDataTablesTodayRecords() -> None:
    today = date.today().isoformat()
    updateLaeDataTables(start=today, end=today, updateCustomers=True)
    print(f"Lae data from {today} added...")

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

def getCustomersDfById(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    customersIds = getCustomersIds(receiptsDf)
    customers = Customers()
    customersJson = customers.getByIds(customersIds)
    customerDf = pd.DataFrame(customersJson)
    customersIdsNoDuplicates = customerDf.drop_duplicates("customer_id").reset_index(drop=True)

    return customersIdsNoDuplicates

def updateWebquotesTables(start: str, end: str) -> None:
    webquotesDf = generateWebquotesDf(start, end)
    postData(webquotesDf, "webquotes", "append")

def addWebquotesTodayRecords() -> None:
    today = date.today().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

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

def updatePoliciesTables(start: str, end: str) -> None:
    receipts = ReceiptsPayroll()
    receiptsJson = receipts.getDataBetweenDates(start, end)
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
    
    logs = policiesNoDuplicates["logs"]
    logsDf = generateLogsDf(logs)
    print("Logs table generated...")

    newPoliciesDf = deleteColumnWithListValues(policiesDf)
    print("Policies Details table generated...")

    postData(vehiclesDf, "vehicles_insured", "append")
    postData(policiesDtlDf, "policies_dtl", "append")
    postData(logsDf, "logs", "append")
    postData(newPoliciesDf, "policies_details", "append")
    print("All tables posted...")
