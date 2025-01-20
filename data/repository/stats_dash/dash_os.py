from service.receipts_for_dash import fetchReceipts
from flask import jsonify, request, Response
import pandas as pd, datetime, logging

logger = logging.getLogger(__name__)

def dashOs(start: str, end: str) -> Response:
    """ Generates the company sales and total sums to be shown in
    the Online Sales by Carrier page.

    Parameters
    - start {str} the beginning of the date range.
    - end {str} the end of the date range.

    Returns
        {flask.Response} the data that will be shown.
    """

    if not start and not end:
        start = datetime.today().replace(day=1).strftime("%Y-%m-%d")
        end = datetime.today().strftime("%Y-%m-%d")

    try:
        company_Sales = fetchReceipts(start, end)
        companySalesPt, totalSums = processDashOs(company_Sales)

        return jsonify({"daily_data": companySalesPt, "total_data": totalSums})
    except Exception as e:
        logger.error(f"Error generating data in dashOs: {str(e)}")
        return jsonify({"error": "Error generating Online Sales by Carrier data"}), 400

def processDashOs(companySales: pd.DataFrame) -> tuple[dict, dict]:
    """ Generates office-related dashboards with company sales data.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {tuple[dict, dict]} Two dictionaries: company sales pivot data
            and total sums.
    """

    companySales = initialTransformations(companySales)
    companySales = groupCompanySalesDf(companySales)
    totalSumDf = generateTotalSumDf(companySales)
    companySalesPtDf = generateCompanySalesPt(companySales)

    totalSums = totalSumDf.to_dict(orient="records")
    companySalesPt = companySalesPtDf.to_dict(orient="records")

    return companySalesPt, totalSums

def initialTransformations(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Performs initial transformations on the company sales data.

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

def groupCompanySalesDf(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Groups company sales data by date and carrier, and calculates
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
