from pandas import DataFrame

""" Delete list of columns from DataFrame.

Parameters
    - df {DataFrame} DataFrame to modify.
    - columns {list[str]} list of columns to delete.

Returns
    {DataFrame} resulting DataFrame.

"""
def deleteColumns(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.drop(columns=columns)

""" Filter list of values from row of a DataFrame.

Parameters
    - df {DataFrame} DataFrame to modify.
    - column {str} column to filter.
    - values {list[str]} list of values that will be filtered.

Returns
    {DataFrame} resulting DataFrame.

"""
def filterRows(df: DataFrame, column: str, values: list[str]) -> DataFrame:
    return df[df[column].isin(values)]

""" Rename all columns of a DataFrame.

Parameters
    - df {DataFrame} DataFrame to modify.
    - columns {dict} dictionary with old and new column names.

Returns
    {DataFrame} resulting DataFrame.

"""
def renameColumns(df: DataFrame, columns: dict) -> DataFrame:
    return df.rename(columns=columns)

"""Reorder all columns of a DataFrame.

Parameters
    - df {DataFrame} DataFrame to modify.
    - columns {dict} list with the order of the columns.

Returns
    {DataFrame} resulting DataFrame.


"""
def reorderColumns(df: DataFrame, columnsOrder: list[str]) -> DataFrame:
    return df.loc[:, columnsOrder]

""" Format date with the same format as API calls.

Parameters
    date {list[str]} the date in a list (month, day, year).

Returns
    {str} the date formatted.

"""
def formatDateToDatabase(date: list[str]) -> str:
    formattedDay = date[1] if int(date[1]) > 9 else date[1][1]
    
    return f"{formattedDay}-{date[2]}-{date[0]}"






""" Get all ids from the Receipt DataFrame.

Parameters
    df {DataFrame} Receipts DataFrame.

Returns
    {dict.values} the ids. It can be iterated as a list.

"""
def getCustomersIds(df) -> dict.values:
    idsColumnValues = df["customer_id"]
    dictCustomersIds = idsColumnValues.to_dict()
    return dictCustomersIds.values()