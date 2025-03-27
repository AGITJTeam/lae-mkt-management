from data.repository.calls.customers_repo import Customers
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import generateOneWeekDateRange, postDataframeToDb
from service.customers import generateCustomersDf
import json, pandas as pd, redis

def updateCustomersTable(receiptsDf: pd.DataFrame) -> None:
    """ Updates Customers table in db with updated Receipts Payroll
    DataFrame. 

    Parameteres
        - receiptsDf {pandas.DataFrame} Receipts Payroll DataFrame.
    """

    customersDf = generateCustomersDf(receiptsDf)
    postDataframeToDb(
        data=customersDf,
        table="customers",
        mode="append",
        filename="flask_api.ini"
    )

def updateRedisKey() -> None:
    """ Updates Redis keys with retrieved customers data. """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Generates all Customers data.
    customers = Customers()
    customersJson = customers.getAllData()

    # Defines expiration time, redis Key and formatted data.
    expirationTime = 60*60*10
    redisKey = "AllCustomers"
    data = json.dumps(obj=customersJson, default=str)

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=data, ex=expirationTime)
    print("AllCustomers Redis key updated...")

    # Close Redis connection.
    redisCli.close()

def addCustomersSpecificRange(start: str, end: str) -> None:
    """ Add data to Customers table with a specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    # Generates data from date range and delete duplicated CustomerId records.
    receipts = ReceiptsPayroll()
    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")

    # Convert data to list.
    customersIds = receiptsNoDuplicates["customer_id"].tolist()

    # Delete data that will be updated.
    customers = Customers()
    customers.deleteByIds(customersIds)

    # Updated Customers table.
    print(f"Customers data from {start} to {end} deleted...")
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {start} to {end} added...")

def updateCustomersPreviousRecords() -> None:
    """ Generates todays customers ids and updates Customers table. """

    # Defines todays date.
    receiptsPayroll = ReceiptsPayroll()
    lastDateFromTable = receiptsPayroll.getLastRecord()[0]["date"]
    todayDate = lastDateFromTable.date()
    todayStr = todayDate.isoformat()

    # Generates data from today and delete duplicated CustomerId records.
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=todayStr, end=todayStr)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["customer_id"], inplace=True)

    # Convert data to list.
    customersIds = receiptsPayrollDf["customer_id"].tolist()

    # Delete data that will be updated.
    customers = Customers()
    customers.deleteByIds(customersIds)
    print(f"Customers data from {todayStr} to {todayStr} deleted...")

    # Updated Customers table.
    updateCustomersTable(receiptsPayrollDf)
    print(f"Customers data from {todayStr} to {todayStr} updated...")
