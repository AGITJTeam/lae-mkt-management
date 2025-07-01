from data.repository.calls.customers_repo import Customers
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.helpers import postDataframeToDb
from service.customers import generateCustomersDf
from datetime import datetime, timedelta
import json, logging, pandas as pd, redis

logger = logging.getLogger(__name__)

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
    logger.info("AllCustomers Redis key updated...")

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
    logger.info(f"Customers data from {start} to {end} deleted...")
    updateCustomersTable(receiptsNoDuplicates)
    logger.info(f"Customers data from {start} to {end} added...")

def updateCustomersPreviousRecords() -> None:
    """ Generates todays customers ids and updates Customers table. """

    # Defines todays date.
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    yesterdayStr = yesterday.isoformat()

    # Generates data from today and delete duplicated CustomerId records.
    receiptsPayroll = ReceiptsPayroll()
    receiptsPayrollJson = receiptsPayroll.getBetweenDates(start=yesterdayStr, end=yesterdayStr)
    receiptsPayrollDf = pd.DataFrame(receiptsPayrollJson)
    receiptsPayrollDf.drop_duplicates(subset=["customer_id"], inplace=True)

    # Convert data to list.
    customersIds = receiptsPayrollDf["customer_id"].tolist()

    # Delete data that will be updated.
    customers = Customers()
    customers.deleteByIds(customersIds)
    logger.info(f"Customers data from {yesterdayStr} to {yesterdayStr} deleted...")

    # Updated Customers table.
    updateCustomersTable(receiptsPayrollDf)
    logger.info(f"Customers data from {yesterdayStr} to {yesterdayStr} updated...")
