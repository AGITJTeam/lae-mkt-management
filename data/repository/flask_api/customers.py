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
    """ Generates last week customers ids and updates Customers table. """

    # Defines date ranges.
    receipts = ReceiptsPayroll()
    lastDateFromTable = receipts.getLastRecord()[0]["date"]
    lastDate = lastDateFromTable.date()
    dates = generateOneWeekDateRange(lastDate)

    # Generates data from date range and delete duplicated CustomerId records.
    receiptsJson = receipts.getBetweenDates(dates["start"], dates["end"])
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")

    # Convert data to list.
    customersIds = receiptsNoDuplicates["customer_id"].tolist()

    # Delete data that will be updated.
    customers = Customers()
    customers.deleteByIds(customersIds)

    # Updated Customers table.
    print(f"Customers data from {dates["start"]} to {dates["end"]} deleted...")
    updateCustomersTable(receiptsNoDuplicates)
    print(f"Customers data from {dates["start"]} to {dates["end"]} updated...")
