from service.receipts_for_dash import fetchReceipts
from flask import jsonify, request, Response
import pandas as pd, logging

logger = logging.getLogger(__name__)

def topCarriers(start: str, end: str) -> Response:
    """ Generates the regular company sales, by office total sums to be shown in
    the Production for Aspire, Kemper and NatGen page.

    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.

    Returns
        {flask.Response} the data that will be shown.
    """

    try:
        companySales = fetchReceipts(start, end)
        companySalesOffice, totalSums, companySalesPT = processTopCarriers(companySales)
    except Exception as e:
        logger.error(f"Error generating data in topCarriers: {str(e)}")
        raise
    else:
        return companySalesPT, companySalesOffice, totalSums

def processTopCarriers(companySales: pd.DataFrame) -> tuple[list[dict], list[dict], list[dict]]:
    """ Generates the top carriers data by performing several
        transformations and aggregations on the company sales data,
        returning three separate lists of dictionaries: company sales
        office, total sums, and company sales pivot table.

    Parameters
        - companySales {pandas.DataFrame} A DataFrame containing company
          sales data.

    Returns
        {tuple} A tuple containing three lists of dictionaries:
        - companySalesOffice {list[dict]} Company sales office data.
        - totalSums {list[dict]} Total sums data for each carrier.
        - companySalesPT {list[dict]} Pivoted company sales data.
    """

    companySales = companySales.copy()

    initialTransformations(companySales)
    companySalesOfficeDf = generateCompanySalesOfficeDf(companySales)
    companySalesFinalDf = generateCompanySalesFinalDf(companySales)
    totalSumsDf = generateTotalSumDf(companySalesFinalDf)
    companySalesPTDf = generateCompanySalesPT(companySalesFinalDf)

    companySalesOffice = companySalesOfficeDf.to_dict(orient="records")
    totalSums = totalSumsDf.to_dict(orient="records")
    companySalesPT = companySalesPTDf.to_dict(orient="records")

    return companySalesOffice, totalSums, companySalesPT

def initialTransformations(df: pd.DataFrame) -> None:
    """ Performs initial transformations on the company sales data,
        including renaming, date parsing, and generating week information.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing company sales data.
    """

    df["payee"] = df["payee"].str.replace("National General", "NationalGeneral")
    df["date"] = pd.to_datetime(df["date"])
    df["weekday"] = df["date"].dt.weekday
    df["day_diff"] = df["weekday"]
    df["week"] = df["date"] - pd.to_timedelta(df["day_diff"], unit="D")
    df["week"] = df["week"].dt.strftime("%Y-%m-%d")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

def generateCompanySalesOfficeDf(df: pd.DataFrame) -> pd.DataFrame:
    """ Generates a company sales office DataFrame by grouping the data
        by date, carrier, and office, and aggregating the 'nb total'
        values for each group.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing company sales data.

    Returns
        {pandas.DataFrame} A DataFrame with aggregated company sales data
            by office.
    """


    RENAMED_COLUMNS = {"nb_total": "nb total"}
    
    df = df.copy()
    company_Sales = baseCompanySalesGroup(df)

    company_Sales_office = (
        company_Sales
        .groupby(by=["date", "carrier", "office"])
        .agg(nb_total=("row_count", lambda x: x[company_Sales["for"] == "NewB - EFT To Company"].sum()))
        .reset_index()
        .rename(columns=RENAMED_COLUMNS)
        .query(expr="`nb total` >= 1")
    )

    return company_Sales_office

def generateCompanySalesFinalDf(df: pd.DataFrame) -> pd.DataFrame:
    """ Generates the final company sales DataFrame by aggregating data 
        at the date and carrier level, renaming columns, and filtering
        results based on the 'nb total' value.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing company sales data.

    Returns
        {pandas.DataFrame} The final DataFrame with aggregated and filtered
            company sales data.
    """

    RENAMED_COLUMNS = {"nb_total": "nb total"}

    df = df.copy()
    company_Sales = baseCompanySalesGroup(df)

    company_Sales_final = (
        company_Sales
        .groupby(by=["date", "carrier"])
        .agg(nb_total=("row_count", lambda x: x[company_Sales["for"] == "NewB - EFT To Company"].sum()))
        .reset_index()
        .rename(columns=RENAMED_COLUMNS)
        .query(expr="`nb total` >= 1")
    )

    return company_Sales_final

def baseCompanySalesGroup(company_Sales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a base company sales DataFrame by grouping the data by
        week, date, office, user, and other relevant columns, and
        aggregating the row count.

    Parameters
        - company_Sales {pandas.DataFrame} A DataFrame containing company
         sales data.

    Returns
        {pandas.DataFrame} The base DataFrame with aggregated row count
            and renamed columns.
    """

    RENAMED_COLUMNS = {
        "payee": "carrier",
        "office_rec":"office"
    }

    company_Sales = company_Sales.copy()

    baseDf = (
        company_Sales
        .groupby(by=["week", "date", "office_rec", "usr", "for", "payee"])
        .agg(row_count = ("gi", "count"))
        .reset_index()
        .rename(columns=RENAMED_COLUMNS)
    )

    return baseDf

def generateTotalSumDf(df: pd.DataFrame) -> pd.DataFrame:
    """ Generates a summary DataFrame with total 'nb total' values per
        carrier.

    Parameters
        - df {pandas.DataFrame} A DataFrame containing company sales
          data.

    Returns
        {pandas.DataFrame} A DataFrame with total 'nb total' per carrier.
    """

    df = df.copy()

    totalSumDf = pd.DataFrame({
        "Kemper": [df.loc[df["carrier"] == "Kemper", "nb total"].sum()],
        "Aspire": [df.loc[df["carrier"] == "Aspire", "nb total"].sum()],
        "NationalGeneral": [df.loc[df["carrier"] == "NationalGeneral", "nb total"].sum()]
    })

    return totalSumDf

def generateCompanySalesPT(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a pivot table from the company sales data with
        'carrier' as rows and 'date' as columns, summarizing the
        'nb total' values.

    Parameters
        - companySales {pandas.DataFrame} A DataFrame containing company
          sales data.

    Returns
        {pandas.DataFrame} The pivot table with summarized 'nb total'
            values.
    """

    companySalesPT = (
        companySales
        .pivot_table(
            index=["carrier"],
            columns=["date"],
            values="nb total"
        )
        .fillna(0)
        .reset_index()
    )
    
    colsToConvert = companySalesPT.columns.difference(["carrier"])
    companySalesPT[colsToConvert] = (
        companySalesPT[colsToConvert]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
    )
    
    return companySalesPT
