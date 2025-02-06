from service.customers_for_dash import fetchCustomersAddress
from service.receipts_for_dash import fetchReceipts
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def outOfState(start: str, end: str) -> list[dict] | None:
    """ Generates the company sales with physical adress data of
    the customer.
    
    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.

    Returns
        {flask.Response} list of dictionaries with the transformed data
        or None if excepction error is raised.
    """

    try:
        companySales = fetchReceipts(start, end)
        custIds = companySales["customer_id"].drop_duplicates().tolist()
        custAdresses = fetchCustomersAddress(custIds)
    except Exception as e:
        logger.error(f"Error generating data in outOfState.")
        raise
    else:
        custAdressesDf = pd.DataFrame(custAdresses)
        companySales = companySales.merge(custAdressesDf, on="customer_id")

        return processOutOfState(companySales)

def processOutOfState(companySales: pd.DataFrame) -> list[dict]:
    """ Processes and formats company sales data for out-of-state sales, 
        renaming columns, transforming data, and grouping it by relevant
        columns.

    Parameters
        - companySales {pd.DataFrame} A DataFrame containing company
          sales data.

    Returns
        {list[dict]} the transformed company sales.
    """

    RENAMED_COLUMNS = {
        "physical_state": "state",
        "gi_sum": "gi",
        "nb_total": "nb",
        "row_count": "receipts"
    }

    companySales = companySales.copy()

    companySales = transformPhysicalStateColumn(companySales)
    addDateColumns(companySales)
    companySales = groupDfByColumns(companySales)
    
    companySales = companySales.rename(columns=RENAMED_COLUMNS)
    companySales.sort_values(by="gi", inplace=True, ascending=False)
    data = companySales.to_dict(orient="records")

    return data

def transformPhysicalStateColumn(df: pd.DataFrame) -> pd.DataFrame:
    """ Transforms the 'physical_state' column by cleaning the state
        names and filtering out states that are not in the specified
        list.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing sales data.

    Returns
        {pandas.DataFrame} The DataFrame with the cleaned and filtered
            'physical_state' column.
    """

    STATE_LIST = [
        "Utah",
        "Nevada",
        "Arizona",
        "Texas",
        "Illinois",
        "Florida",
        "Ohio"
    ]

    df["physical_state"] = df["physical_state"].str.replace(r"[^a-zA-Z]", "", regex=True).str.title()
    df["physical_state"] = df["physical_state"].str.strip()
    df["physical_state"] = df["physical_state"].fillna("")
    df = df[df["physical_state"].str.contains("|".join(STATE_LIST))]

    return df

def addDateColumns(df: pd.DataFrame) -> None:
    """ Transforms date-related columns, including calculating the
        weekday, day difference, and week based on the date column.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing date-related data.

    Returns
        {None} The DataFrame is modified in place.
    """

    df["date"] = pd.to_datetime(df["date"])
    df["weekday"] = df["date"].dt.weekday
    df["day_diff"] = df["weekday"]
    df["week"] = df["date"] - pd.to_timedelta(df["day_diff"], unit="D")
    df["week"] = df["week"].dt.strftime("%Y-%m-%d")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

def groupDfByColumns(df: pd.DataFrame) -> pd.DataFrame:
    """ Groups the DataFrame by specific columns and aggregates the
        data based on the specified grouping criteria.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing sales data.

    Returns
        {pandas.DataFrame} The grouped DataFrame with aggregated values.
    """

    df = df.groupby(["week", "date", "for","physical_state"]).agg(
        gi_sum=("gi", "sum"),
        row_count=("gi", "count")
    ).reset_index()
    df = df.groupby(["physical_state"]).agg(
        gi_sum=("gi_sum", "sum"),
        nb_total=("row_count", lambda x: x[df["for"] == "NewB - EFT To Company"].sum()),
        row_count=("physical_state", "size")
    ).reset_index()

    return df
