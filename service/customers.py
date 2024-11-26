from service.columnsTransformations import custColumnsToDelete, custNewColumnsNames
from service.helpers import deleteColumns, renameColumns, getCustomersIds
from data.models.customers_model import CustomerModel
from controllers.controller import getCustomer
import pandas as pd

""" Create Customers DataFrame with renamed columns with API response.

Parameters
    receiptsDf {DataFrame} from which the ids will be obtained.

Returns
    {DataFrame} resulting DataFrame.

"""
def generateCustomersDf(receipts: pd.DataFrame) -> pd.DataFrame:
    customersIds = getCustomersIds(receipts)
    customers = getCustomersWithId(customersIds)
    customersDf = pd.DataFrame(customers)
    customersDf["dateCreated"] = pd.to_datetime(customersDf["dateCreated"])
    customersDf["lastUpdated"] = pd.to_datetime(customersDf["lastUpdated"])
    customersDf["birthDay"] = pd.to_datetime(customersDf["birthDay"])
    renamedCustomersDf = renameColumns(customersDf, custNewColumnsNames)
    
    return renamedCustomersDf

""" Iterate and save all customers in a list.

Parameters
    ids {dict.values} ids to iterate.

Returns
    {list[Response]} list of customers.

"""
def getCustomersWithId(ids: dict.values) -> list:
    customers = []

    for id in ids:
        customer = getCustomer(id)

        if not customer:
            continue

        customerModel = CustomerModel(**customer)
        customers.append(customerModel)
    
    return customers

""" Delete, add column and drop duplicates from Customers DataFrame.

Parameters
    df {DataFrame} DataFrame to transform.

Returns
    {DataFrame} resulting DataFrame.

"""
def transformCustomersDfForLaeData(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = deleteColumns(df, custColumnsToDelete)
    df = addPhoneFixColumn(df)

    return df

""" Add conditional column to Customers DataFrame.

Parameters
    df {DataFrame} DataFrame to modify.

Returns
    {DataFrame} DataFrame with 1 new columns.

"""
def addPhoneFixColumn(df: pd.DataFrame) -> pd.DataFrame:
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