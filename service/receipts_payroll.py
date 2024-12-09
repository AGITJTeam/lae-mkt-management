from service.columnsTransformations import rpNewColumnNames, rpColumnsToDelete, rpValuesToFilter
from service.helpers import deleteColumns, filterRows, renameColumns
from data.models.receipts_payroll_model import ReceiptsPayrollModel
from controllers.controller import getReceiptsPayroll
import pandas as pd

""" Create Receipts DataFrame with renamed columns with API response.

Returns
    {pandas.DataFrame} resulting DataFrame.

"""
def generateReceiptsPayrollDf(start: str, end: str) -> pd.DataFrame:
    receipts = []
    receiptsJson = getReceiptsPayroll(start, end)

    if not receiptsJson:
        raise Exception("There is no receipt yet...")

    for receipt in receiptsJson:
        for_value = receipt["for"]
        receipt.pop("for")
        receipt.update({"for_t": for_value})
        receiptsModel = ReceiptsPayrollModel(**receipt)
        receipts.append(receiptsModel)

    receiptsDf = pd.DataFrame(receipts)
    receiptsDf["date"] = pd.to_datetime(receiptsDf["date"])
    renamedReceiptsDf = renameColumns(receiptsDf, rpNewColumnNames)

    return renamedReceiptsDf

""" Delete, filter, and add columns from Receipts DataFrame.

Parameters
    df {pandas.DataFrame} DataFrame to transform.

Returns
    {pandas.DataFrame} resulting DataFrame.

"""
def transformReceiptsDfForLaeData(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = deleteColumns(df, rpColumnsToDelete)
    df = filterRows(df, "for", rpValuesToFilter)
    df = addCountingColumns(df)

    return df

""" Add counting columns for "For" column to Receipts DataFrame.

Parameters
    - df {pandas.DataFrame} DataFrame to modify.

Returns
    {pandas.DataFrame} resulting DataFrame.

"""
def addCountingColumns(df: pd.DataFrame) -> pd.DataFrame:
    df["nb"] = countForColumnValues(df, rpValuesToFilter[0])
    df["bf"] = countForColumnValues(df, rpValuesToFilter[1:4])
    df["endos"] = countForColumnValues(df, rpValuesToFilter[4:6])
    df["payments"] = countForColumnValues(df, rpValuesToFilter[6:9])
    df["invoice"] = countForColumnValues(df, rpValuesToFilter[9:11])
    df["dmv"] = countForColumnValues(df, rpValuesToFilter[11:17])
    df["towing"] = countForColumnValues(df, rpValuesToFilter[17])
    df["permit"] = countForColumnValues(df, rpValuesToFilter[18])
    df["traffic_school"] = countForColumnValues(df, rpValuesToFilter[19])
    df["renewal"] = countForColumnValues(df, rpValuesToFilter[20])
    df["trucking"] = countForColumnValues(df, rpValuesToFilter[21])
    df["immigration"] = countForColumnValues(df, rpValuesToFilter[22])
    
    return df

""" Count "For" column values and generate list of values.

Parameters
    - df {pandas.DataFrame} DataFrame from which the values will be obtained.
    - valuesToCount {list[str]} list of values to count

Returns
    {list[int]} list of counted values.

"""
def countForColumnValues(df: pd.DataFrame, valuesToCount: list[str]) -> list[int]:
    if isString(valuesToCount):
        stringToList = [valuesToCount]
        valuesToCount = stringToList

    forColumnValues = df["for"]
    countValues = []

    for forValue in forColumnValues:
        if forValue in valuesToCount:
            countValues.append(1)
        else:
            countValues.append(0)
    
    return countValues

def isString(value) -> bool:
    return type(value) == str
