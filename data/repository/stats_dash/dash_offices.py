from data.repository.calls.compliance_repo import Compliance
from service.payroll_report import generateAgiReport
from service.gi_logic import normalizeStr
from service.receipts_for_dash import fetchReceipts
from flask import session
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def dashOffices(start: str, end: str, reportId: int) -> tuple[list[dict], list[dict]] | None:
    """ Generates the company sales and total sums to be shown in
    the Production by Office page.

    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.
        - reportId {int} the id of the report to search for.

    Returns
        {tuple[list[dict], list[dict]] | None} the data that will be
        shown or None if exception raise an error.
    """

    compliance = Compliance()

    try:
        companySales = fetchReceipts(start, end)
        agiReport = generateAgiReport(reportId)
        offices = compliance.getRegionalsByOffices()

        companySalesProcessed, totalSums = processDashOffices(companySales, agiReport, offices)
    except Exception as e:
        logger.error(f"Error generating data in dashOffices: {str(e)}")
        raise
    else:
        return companySalesProcessed, totalSums

def processDashOffices(companySalesDf: pd.DataFrame, agiReport: pd.DataFrame, offices: list[dict]) -> tuple[list[dict], list[dict]]:
    """ Transform the company sales and agi reports to make it ready for the Production
    by Office page.

    Parameters
        - companySalesDf {pandas.DataFrame} Raw company sales data.
        - agiReport {pandas.DataFrame} Agent information report data.
        - offices {list[dict]} List of office data dictionaries.

    Returns
        {tuple[dict, dict]} the transformed company sales and total sums.
    """

    POSITION = session.get("position")
    REGNAME = session.get("fullname")

    companySalesDf = preMergeTransformations(companySalesDf)
    agentCount = generateAgentCount(agiReport)
    officeReport = generateOfficeReport(offices)

    companySalesDf = (
        companySalesDf
        .merge(right=agentCount, on="office", how="left")
        .merge(right=officeReport, on="office", how="left")
        .fillna(0)
    )

    if POSITION == "Regional Manager":
        regName = setRegName(REGNAME)
        companySalesDf = companySalesDf[companySalesDf["regional"].isin(regName)]

    totalSumsDf = generateTotalSums(companySalesDf)

    totalSums = totalSumsDf.to_dict(orient="records")
    companySales = companySalesDf.to_dict(orient="records")

    return companySales, totalSums

def preMergeTransformations(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Transforms company sales data through various cleaning steps.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Transformed company sales data.
    """

    addDateColumns(companySales)
    companySales = replaceOfficeValues(companySales)
    companySales = groupByOfficeAddition(companySales)

    return companySales

def addDateColumns(companySales: pd.DataFrame) -> None:
    """ Change 'date' column type and add some date columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Transformed company sales data with new
        columns.
    """

    companySales["date"] = pd.to_datetime(companySales["date"])
    companySales["weekday"] = companySales["date"].dt.weekday
    companySales["day_diff"] = companySales["weekday"]
    companySales["week"] = companySales["date"] - pd.to_timedelta(companySales["day_diff"], unit="D")
    companySales["date"] = companySales["date"].dt.strftime("%Y-%m-%d")
    companySales["week"] = companySales["week"].dt.strftime("%Y-%m-%d")

def replaceOfficeValues(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Replaces certain values in the 'office_rec' column of company
        sales data.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} The modified company sales data with
        replaced values.
    """

    REPLACEMENTS = {
        "8th": "8th Street",
        "Fontana2": "Fontana 2",
        "Registrations": "DMV"
    }

    companySales["office_rec"] = companySales["office_rec"].replace(REPLACEMENTS, regex=True)

    return companySales

def groupByOfficeAddition(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Groups company sales data by week, date, and office, and
        calculates aggregates.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame grouped by office with
        aggregated data.
    """

    COLS_TO_RENAMME = {
        "office_rec": "office",
        "gi_sum2": "gi total",
        "row_count2": "Receipts",
        "nb_total": "nb total"
    }

    csGroupedByDate = (
        companySales
        .groupby(["week", "date", "office_rec", "for"])
        .agg(
            gi_sum1 = ("gi", "sum"),
            row_count1 = ("gi", "count")
        )
        .reset_index()
    )

    csGroupedByOffice = (
        csGroupedByDate
        .groupby(["office_rec"])
        .agg(
            gi_sum2 = ("gi_sum1", "sum"),
            row_count2 = ("gi_sum1", "count"),
            nb_total = ("row_count1", lambda x: x[csGroupedByDate["for"] == "NewB - EFT To Company"].sum())
        )
        .reset_index()
        .rename(columns=COLS_TO_RENAMME)
        .sort_values(by="gi total", ascending=False)
    )

    return csGroupedByOffice

def generateAgentCount(agiReport: pd.DataFrame) -> pd.DataFrame:
    """ Generates a count of agents from the agent report.

    Parameters
        - agiReport {pandas.DataFrame} Agent information report data.

    Returns
        {pandas.DataFrame} A DataFrame with the number of agents per
        office.
    """

    COLS_TO_RENAME = {
        "Location": "office",
        "Manager": "manager",
        "Position": "position",
        "row_count": "count"
    }

    agiReport["Manager"] = agiReport["Manager"].str.split("(").str[0]
    agiReportGrouped = (
        agiReport
        .groupby(["Location", "Manager", "Position"])
        .agg(row_count = ("Location", "count"))
        .reset_index()
        .rename(columns=COLS_TO_RENAME)
    )
    filteredReport = agiReportGrouped.loc[agiReportGrouped["position"] == "Agent"]

    agentCount = (
        filteredReport
        .groupby(by=["office"], as_index=False)
        .agg(
            manager = ("manager", lambda x: ", ".join(sorted(x))),
            row_count = ("count", "sum")
        )
        .rename(columns={"row_count": "# agents"})
    )

    return agentCount

def generateOfficeReport(officeReport: list[dict]) -> pd.DataFrame:
    """ Generates an office report DataFrame with renamed columns.

    Parameters
        - officeReport {list[dict]} List of dictionaries containing 
        office data.

    Returns
        {pandas.DataFrame} A DataFrame with renamed columns for the 
        report.
    """

    COLS_TO_RENAME = {
        "District Email": "district email",
        "Regional Email": "regional email",
        "Manager Email": "manager email",
        "manager": "office manager",
        "LAE Office Name": "lae office name",
        "New Month Office Goal": "new month office goal"
    }

    return pd.DataFrame(officeReport).rename(columns=COLS_TO_RENAME)

def setRegName(regName: str | list) -> list:
    """ Normalizes and converts the regional name(s) into a list of
        strings.

    Parameters
        - regName {str | list} Regional name(s) to normalize.

    Returns
        {list} A list of normalized regional names.
    """

    if not isinstance(regName, list):
        regName = [regName]
    
    regName = [normalizeStr(str(name).strip()) for name in regName]

    return regName

def generateTotalSums(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates a DataFrame with total sums for 'gi total' and
        'nb total' columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame with the total sums for the
        specified columns.
    """

    totalSums = pd.DataFrame({
        "gi total": [companySales["gi total"].sum()],
        "nb total": [companySales["nb total"].sum()]
    })

    return totalSums
