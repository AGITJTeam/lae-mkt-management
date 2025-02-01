from data.repository.calls.compliance_repo import Compliance
from service.gi_logic import normalizeStr
from service.receipts_for_dash import fetchReceipts
import pandas as pd, logging

logger = logging.getLogger(__name__)

def dashCarriers(start: str, end: str, position: str, fullname: str) -> tuple[list[dict], list[dict]] | None:
    """ Generates the company sales and total sums to be shown in
    the Production by Carrier page.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
        - position {str} the "role" or position of the user.
        - fullname {str} the full name of the user.

    Returns
        {tuple[list[dict], list[dict]] | None} the data that will be
        shown or None if exception raise an error.
    """

    try:
        companySales = fetchReceipts(start, end)
        companySalesPT, totalSums = processDashCarriers(companySales, position, fullname)
    except Exception as e:
        logger.error(f"Error generating data in dashCarriers: {str(e)}")
        raise
    else:
        return companySalesPT, totalSums

def processDashCarriers(companySales: pd.DataFrame, position: str, fullname: str) -> tuple[list[dict], list[dict]]:
    """ Transform the company sales to make it ready for the Production
    by Carrier page.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - position {str} The "role" or position of the user.
        - fullname {str} The full name of the user.

    Returns
        {tuple[list[dict], list[dict]]} the transformed company sales
        and total sums.
    """

    POSITION_REQUIRING_FILTER = ["Regional Manager"]
    companySales = companySales.copy()

    addDateColumns(companySales)
    baseCompanySales = groupByDateColumns(companySales)

    if position in POSITION_REQUIRING_FILTER:
        mergeWithOfficesDf(baseCompanySales, fullname)
    
    companySalesFinal = groupByDatesAndCarriers(baseCompanySales)
    totalSumsDf = generateTotalSumDf(companySalesFinal)
    companySalesPTDf = generateCompanySalesPT(companySalesFinal)

    totalSums = totalSumsDf.to_dict(orient="records")
    companySalesPT = companySalesPTDf.to_dict(orient="records")

    return companySalesPT, totalSums

def addDateColumns(df: pd.DataFrame) -> None:
    """ Change 'date' column type and add some date columns.

    Parameters
        - df {pandas.DataFrame} Input DataFrame to be transformed.
    """

    df["date"] = pd.to_datetime(df["date"])
    df["weekday"] = df["date"].dt.weekday
    df["day_diff"] = df["weekday"]
    df["week"] = df["date"] - pd.to_timedelta(df["day_diff"], unit="D")
    df["week"] = df["week"].dt.strftime("%Y-%m-%d")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

def groupByDateColumns(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Groups company sales data by week, date, office, and payee.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} DataFrame with grouped and renamed columns.
    """

    RENAMED_COLUMNS = {
        "row_count": "receipts",
        "office_rec": "office",
        "payee": "carrier"
    }

    baseDf = (
        companySales
        .groupby(by=["week", "date", "office_rec", "for", "payee"])
        .agg(row_count = ("gi", "count"))
        .reset_index()
        .rename(columns=RENAMED_COLUMNS)
    )

    return baseDf

def mergeWithOfficesDf(companySales: pd.DataFrame, regName: str | list) -> pd.DataFrame:
    """ Merges company sales data with office information based on conditions.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - regName {str | list} list or string for regional filter.

    Returns
        {pandas.DataFrame} Merged DataFrame filtered by regional name(s).
    """

    if not isinstance(regName, list):
        regName = [regName]
        
    compliance = Compliance()
    response = compliance.getRegionalsByOffices()
    officesInfoDf = pd.DataFrame(response)
    companySales.rename(columns={"office": "Office"}, inplace=True)
    companySales = (
        pd.merge(
            left=companySales,
            right=officesInfoDf,
            on=["Office"],
            how="outer"
        )
        .fillna(0)
        .assign(Regional = lambda df: df["regional"].astype(str).str.strip().apply(normalizeStr))
        .loc[lambda df: df["regional"].isin(regName)]
    )

def groupByDatesAndCarriers(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a DataFrame with aggregated company sales data by week,
        date, and carrier.

    Parameters
        - companySales {pandas.DataFrame} The raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame with aggregated company sales and
        renamed columns.
    """

    RENAMED_COLUMNS = {"nb_total": "nb total"}

    companySalesFinal = (
        companySales
        .groupby(by=["week","date","carrier"])
        .agg(nb_total=("receipts", lambda x: x[companySales["for"] == "NewB - EFT To Company"].sum()))
        .reset_index()
        .rename(columns=RENAMED_COLUMNS)
    )

    return companySalesFinal

def generateTotalSumDf(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a DataFrame containing the total sum of the 'nb total'
        column.

    Parameters
        - companySales {pandas.DataFrame} The raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame with a single value representing the
        total sum.
    """

    totalSumDf = pd.DataFrame({
        "nb total": [companySales["nb total"].sum()]
    })

    return totalSumDf

def generateCompanySalesPT(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a pivot table for company sales, with carriers as rows
        and dates as columns.

    Parameters
        - companySales {pandas.DataFrame} The raw company sales data.

    Returns
        {pandas.DataFrame} A pivot table with company sales data,
        filling missing values with 0.
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
