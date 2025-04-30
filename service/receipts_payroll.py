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

    receiptTypeColumns = [
        "nb", "bf", "endos", "payments", "invoice",
        "dmv", "towing", "permit", "traffic_school",
        "renewal", "trucking", "immigration",
    ]
    df = df.copy()

    # Delete, filter and add columns to DataFrame.
    df = deleteColumns(df, rpColumnsToDelete)
    df = filterRows(df, "for", rpValuesToFilter)
    df = addReceiptTypeCountColumns(df)

    # Group by customer_id and sum Receipt type count columns.
    receiptTypeSums = df.groupby("customer_id")[receiptTypeColumns].sum()

    # Maintain first row for each customer_id of original Dataframe.
    otherColumns = (
        df
        .drop(columns=receiptTypeColumns)
        .groupby('customer_id')
        .first()
    )

    # Join unique customer_id and Receipt type sums.
    df = otherColumns.join(receiptTypeSums).reset_index()

    return df

def addReceiptTypeCountColumns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add Receipt type count columns according to "for" column to Receipts
    DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    df["nb"] = countReceiptType(df, rpValuesToFilter[0])
    df["bf"] = countReceiptType(df, rpValuesToFilter[1:4])
    df["endos"] = countReceiptType(df, rpValuesToFilter[4:6])
    df["payments"] = countReceiptType(df, rpValuesToFilter[6:9])
    df["invoice"] = countReceiptType(df, rpValuesToFilter[9:11])
    df["dmv"] = countReceiptType(df, rpValuesToFilter[11:17])
    df["towing"] = countReceiptType(df, rpValuesToFilter[17])
    df["permit"] = countReceiptType(df, rpValuesToFilter[18])
    df["traffic_school"] = countReceiptType(df, rpValuesToFilter[19])
    df["renewal"] = countReceiptType(df, rpValuesToFilter[20])
    df["trucking"] = countReceiptType(df, rpValuesToFilter[21])
    df["immigration"] = countReceiptType(df, rpValuesToFilter[22])
    
    return df

def countReceiptType(df: pd.DataFrame, valuesToCount: list[str]) -> list[int]:
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

    forValues = df["for"]
    countValues = []

    for forValue in forValues:
        if forValue in valuesToCount:
            countValues.append(1)
        else:
            countValues.append(0)
    
    return countValues
