from data.repository.calls.helpers import postData

from service.employees import generateEmployeesDf
from service.receipts_payroll import generateReceiptsPayrollDf, transformReceiptsDfForLaeData
from service.customers import generateCustomersDf, transformCustomersDfForLaeData
from service.lae_data import generateLaeData
from service.policies_details import generatePoliciesDf, deleteColumnWithListValues
from service.vehicles_insured import generateVehiclesDf

def receiptsPayrollDailyUpdate():
    receiptsDf = generateReceiptsPayrollDf()
    postData(receiptsDf, "receipts_payroll", "append")

def generateAllTablesAndPostThem() -> None:
    employeesDf = generateEmployeesDf()
    postData(employeesDf, "employees", "replace")
    print("employees table generated and published...")

    receiptsDf = generateReceiptsPayrollDf()
    postData(receiptsDf, "receipts_payroll", "append")
    print("receipts_payroll table generated and published...")

    customersDf = generateCustomersDf(receiptsDf)
    postData(customersDf, "customers", "append")
    print("customers table generated and published...")

    newReceipts = transformReceiptsDfForLaeData(receiptsDf)
    newCustomers = transformCustomersDfForLaeData(customersDf)
    laeData = generateLaeData(newReceipts, newCustomers)
    postData(laeData, "lae_data", "append")
    print("lae_data table generated and published...")

    policiesDf = generatePoliciesDf(receiptsDf)
    print("policies_details table generated...")

    vehiclesInsured = policiesDf["vehicle_insureds"]
    vehiclesDf = generateVehiclesDf(vehiclesInsured)
    postData(vehiclesDf, "vehicles_insured", "append")
    print("vehicles_insured table generated and published...")

    newPoliciesDf = deleteColumnWithListValues(policiesDf)
    postData(newPoliciesDf, "policies_details", "append")
    print("policies_details table published...")