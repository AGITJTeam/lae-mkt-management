from service.receipts_for_dash import fetchReceipts
from flask import jsonify, request, Response
import pandas as pd, datetime, logging

logger = logging.getLogger(__name__)

def dashOs(start: str, end: str) -> tuple[list[dict], list[dict]] | None:
    """ Generates the company sales and total sums to be shown in
    the Online Sales by Carrier page.

    Parameters
    - start {str} the beginning of the date range.
    - end {str} the end of the date range.

    Returns
        {tuple[list[dict], list[dict]] | None} the data that will be
        shown or None if exception raise an error.
    """

    if not start and not end:
        start = datetime.today().replace(day=1).strftime("%Y-%m-%d")
        end = datetime.today().strftime("%Y-%m-%d")

    try:
        company_Sales = fetchReceipts(start, end)
        companySalesPt, totalSums = processDashOs(company_Sales)
    except Exception as e:
        logger.error(f"Error generating data in dashOs: {str(e)}")
        raise
    else:
        return companySalesPt, totalSums

def processDashOs(companySales: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """ Transform the company sales to make it ready for the Online Sales
    by Carrier page.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {tuple[dict, dict]} Two dictionaries: company sales pivot data
        and total sums.
    """

    companySales = filterAndAggregateCols(companySales)
    companySales = groupByDateAndPayee(companySales)
    totalSumDf = generateTotalSumDf(companySales)
    companySalesPtDf = generateCompanySalesPt(companySales)

    totalSums = totalSumDf.to_dict(orient="records")
    companySalesPt = companySalesPtDf.to_dict(orient="records")

    return companySalesPt, totalSums

def filterAndAggregateCols(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Filter 'office_rec' and 'for' columns. Change 'date' column type
    and add some date columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Transformed company sales data filtered by criteria.
    """

    companySales = companySales[companySales["office_rec"].str.contains("|".join(["Online Sales"]))]
    companySales = companySales[companySales["for"].str.contains("NewB - EFT To Company")]
    companySales["date"] = pd.to_datetime(companySales["date"])
    companySales["weekday"] = companySales["date"].dt.weekday
    companySales["day_diff"] = companySales["weekday"]
    companySales["week"] = companySales["date"] - pd.to_timedelta(companySales["day_diff"], unit="D")
    companySales["week"] = companySales["week"].dt.strftime("%Y-%m-%d")
    companySales["date"] = companySales["date"].dt.strftime("%Y-%m-%d")

    return companySales

def groupByDateAndPayee(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Groups company sales data by date and payee, and calculates
        aggregates.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Grouped company sales data with renamed 
        columns.
    """

    COLS_TO_RENAME = {
        "row_count": "nb",
        "payee": "carrier"
    }

    companySales = (
        companySales
        .groupby(["date", "payee"])
        .agg(row_count = ("gi", "count"))
        .reset_index()
        .rename(columns=COLS_TO_RENAME)
    )

    return companySales

def generateTotalSumDf(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a DataFrame containing the total sum of the 'nb' column.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame with the total sum of the 'nb'
        column.
    """

    totalSumsDf = pd.DataFrame({
        "nb total": [companySales["nb"].sum()]
    })

    return totalSumsDf

def generateCompanySalesPt(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a pivot table for company sales with carriers as rows
        and dates as columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} A pivot table with company sales data, filling
        missing values with 0.
    """

    companySalesPt = (
        companySales.
        pivot_table(
            index=["carrier"],
            columns=["date"],
            values="nb"
        )
        .fillna(0)
        .reset_index()
    )

    return companySalesPt
