from pandas import DataFrame

def deleteColumns(df: DataFrame, columns: list[str]) -> DataFrame:
    """ Delete list of columns from DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.
        - columns {list[str]} list of columns to delete.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    return df.drop(columns=columns)

def filterRows(df: DataFrame, column: str, values: list[str]) -> DataFrame:
    """ Filter list of values from row of a DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.
        - column {str} column to filter.
        - values {list[str]} list of values that will be filtered.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    return df[df[column].isin(values)]

def renameColumns(df: DataFrame, columns: dict) -> DataFrame:
    """ Rename all columns of a DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.
        - columns {dict} dictionary with old and new column names.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    return df.rename(columns=columns)

def reorderColumns(df: DataFrame, columnsOrder: list[str]) -> DataFrame:
    """Reorder all columns of a DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to modify.
        - columns {dict} list with the order of the columns.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    return df.loc[:, columnsOrder]
