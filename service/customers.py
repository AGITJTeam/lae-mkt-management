from service.columnsTransformations import custColumnsToDelete, custNewColumnsNames
from service.helpers import deleteColumns, renameColumns
from data.models.customers_model import CustomerModel
from controllers.controller import getCustomer
import pandas as pd
import time

def generateCustomersDf(receipts: pd.DataFrame) -> pd.DataFrame:
    """ Create Customers DataFrame with API response.

    Parameters
        - receiptsDf {pandas.DataFrame} from which the ids will be obtained.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    customersIds = receipts["customer_id"].tolist()
    customers = getCustomersData(customersIds)
    customersDf = pd.DataFrame(customers)
    customersDf["dateCreated"] = pd.to_datetime(customersDf["dateCreated"])
    customersDf["lastUpdated"] = pd.to_datetime(customersDf["lastUpdated"])
    customersDf["birthDay"] = pd.to_datetime(customersDf["birthDay"])
    renamedCustomersDf = renameColumns(customersDf, custNewColumnsNames)
    
    return renamedCustomersDf

def getCustomersData(ids: list) -> list[CustomerModel]:
    """ Iterate over customers ids and save them in a list.

    Parameters
        - ids {dict.values} ids to iterate.

    Returns
        {list[CustomerModel]} list of customers.
    """

    customers = []

    for id in ids:
        customer = getCustomer(id)

        if not customer:
            continue

        customerModel = CustomerModel(**customer)
        customers.append(customerModel)

        time.sleep(1.0)
    
    return customers

def transformCustomersDfForLaeData(df: pd.DataFrame) -> pd.DataFrame:
    """ Delete and add a column from Customers DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to transform.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    df = df.copy()

    df = deleteColumns(df, custColumnsToDelete)
    df = addPhoneFixColumn(df)

    return df

def addPhoneFixColumn(df: pd.DataFrame) -> pd.DataFrame:
    """ Add conditional phone number column to Customers DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.

    Returns
        {pandas.DataFrame} DataFrame with 1 new columns.
    """

    phoneColumnValues = df["phone"]
    cellPhoneColumnValues = df["cell_phone"]
    phoneFixValues = []

    for i in range(len(cellPhoneColumnValues)):
        if cellPhoneColumnValues[i] == phoneColumnValues[i]:
            phoneFixValues.append(cellPhoneColumnValues[i])
        elif phoneColumnValues[i] == None:
            phoneFixValues.append(cellPhoneColumnValues[i])
        elif cellPhoneColumnValues[i] == None:
            phoneFixValues.append(phoneColumnValues[i])
        elif cellPhoneColumnValues[i] != phoneColumnValues[i]:
            phoneFixValues.append("2 Phones")
        elif cellPhoneColumnValues[i] == None:
            phoneFixValues.append("No Phone")
        else:
            phoneFixValues.append("Fix")
    
    df["phone_fix"] = phoneFixValues

    return df
