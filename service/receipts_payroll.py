from service.columnsTransformations import rpNewColumnNames, rpColumnsToDelete, rpValuesToFilter
from service.helpers import deleteColumns, filterRows, renameColumns
from data.models.receipts_payroll_model import ReceiptsPayrollModel
from controllers.controller import getReceiptsPayroll
import pandas as pd

def generateReceiptsPayrollDf(start: str, end: str) -> pd.DataFrame:
    """ Create Receipts DataFrame with API response.

    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    receipts = []
    receiptsJson = getReceiptsPayroll(start, end)

    if not receiptsJson:
        raise Exception(f"No receipt payroll found from {start} to {end}")

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

def transformReceiptsDfForLaeData(df: pd.DataFrame) -> pd.DataFrame:
    """ Delete, filter, and add columns from Receipts DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to transform.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    df = df.copy()

    df = deleteColumns(df, rpColumnsToDelete)
    df = filterRows(df, "for", rpValuesToFilter)
    df = addServiceCountingColumns(df)

    return df

def addServiceCountingColumns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add counting service columns for "For" column to Receipts DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    df["nb"] = countForValues(df, rpValuesToFilter[0])
    df["bf"] = countForValues(df, rpValuesToFilter[1:4])
    df["endos"] = countForValues(df, rpValuesToFilter[4:6])
    df["payments"] = countForValues(df, rpValuesToFilter[6:9])
    df["invoice"] = countForValues(df, rpValuesToFilter[9:11])
    df["dmv"] = countForValues(df, rpValuesToFilter[11:17])
    df["towing"] = countForValues(df, rpValuesToFilter[17])
    df["permit"] = countForValues(df, rpValuesToFilter[18])
    df["traffic_school"] = countForValues(df, rpValuesToFilter[19])
    df["renewal"] = countForValues(df, rpValuesToFilter[20])
    df["trucking"] = countForValues(df, rpValuesToFilter[21])
    df["immigration"] = countForValues(df, rpValuesToFilter[22])
    
    return df

def countForValues(df: pd.DataFrame, valuesToCount: list[str]) -> list[int]:
    """ Count "For" column values and generate list of values.

    Parameters
        - df {pandas.DataFrame} DataFrame from which the values will be obtained.
        - valuesToCount {list[str]} list of values to count

    Returns
        {list[int]} list of counted values.
    """

    if type(valuesToCount) == str:
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
