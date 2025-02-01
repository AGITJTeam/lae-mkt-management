from data.repository.calls.compliance_repo import Compliance
from service.gi_logic import normalizeStr
from service.receipts_for_dash import fetchReceipts
from dateutil.relativedelta import relativedelta
from flask import jsonify, session, Response
from datetime import datetime, timedelta
import pandas as pd, numpy as np, logging

logger = logging.getLogger(__name__)

def dashProjections(position: str, fullname: str) -> Response:
    """ Generates the company sales and total sums to be shown in
    the Projection's Dashboard page.

    Parameters
        - position {str} the position or 'role' of the user.
        - fullname {str} the complete name of the user.

    Returns
        {flask.Response} the data that will be shown.
    """

    compliance = Compliance()
    dates = generateDates()

    try:
        companySales = fetchReceipts(dates["start"], dates["end"])
        offices = compliance.getRegionalsByOffices()
            
        companySalesProcessed, totalSums = processDashProjections(companySales, offices, dates, position, fullname)
    except Exception as e:
        logger.error(f"Error generating projection in dashProjections: {str(e)}")
        raise
    else:
        return companySalesProcessed, totalSums, dates["start"], dates["end"]

def processDashProjections(companySalesDf: pd.DataFrame, offices: list[dict], dates: dict, position: str, fullname: str) -> tuple[dict, dict]:
    """ Generates company sales projections and total sums based on
        positions.

    Parameters
        - companySalesDf {pandas.DataFrame} Company sales data.
        - offices {list[dict]} Office data.
        - dates {dict} Dictionary with date information (start, first
          day of current month, end, working days and total days).
        - position {str} the position or 'role' of the user.
        - fullname {str} the complete name of the user.

    Returns
        {tuple[dict, dict]} A tuple containing two dictionaries: one
            with company sales data and another with total sums.
    """

    companySalesDf = generateFinalCompanySales(companySalesDf, dates["firstDayCurrentMonth"], dates["workingDays"], dates["totalDays"])
    officeReport = generateOfficeReport(offices)

    companySalesDf = (
        companySalesDf
        .merge(right=officeReport, on=["office"], how="outer")
        .fillna(0)
    )

    if position == "Regional Manager":
        regName = setRegName(fullname)
        companySalesDf = companySalesDf[companySalesDf["regional"].isin(regName)]
    elif position == "Marketing":
        regName = setRegName(fullname)
        companySalesDf = companySalesDf[companySalesDf["district"].isin(["Paola Sanchez", "Trucking"])]
    
    totalSumsDf = generateTotalSums(companySalesDf, dates["workingDays"], dates["totalDays"])

    companySales = companySalesDf.to_dict(orient="records")
    totalSums = totalSumsDf.to_dict(orient="records")

    return companySales, totalSums

def generateDates() -> dict:
    """ Generates a dictionary with key date information, including
        working and total days.

    Returns
        {dict} Dictionary with date ranges and working day counts for
            the month.
    """
    
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    firstDayCurrentMonth = yesterday.replace(day=1)
    lastDayCurrentMonth = firstDayCurrentMonth + relativedelta(day=31)
    lastDayPreviousMonth = firstDayCurrentMonth - timedelta(days=1)
    firstDayOfPreviousMonth = lastDayPreviousMonth.replace(day=1)
    firstDayCurrentMonthNp = np.datetime64(firstDayCurrentMonth, "D")
    yesterdayNp = np.datetime64(yesterday, "D")

    workingDays = np.busday_count(
        firstDayCurrentMonthNp,
        yesterdayNp,
        weekmask="1111110"
    )

    totalDays = np.busday_count(
        firstDayCurrentMonthNp,
        lastDayCurrentMonth,
        weekmask="1111110"
    )
    
    dates = { 
        "start": firstDayOfPreviousMonth.isoformat(),
        "firstDayCurrentMonth": firstDayCurrentMonth.isoformat(),
        "end": yesterday.isoformat(),
        "workingDays": workingDays,
        "totalDays": totalDays
    }

    return dates

def generateFinalCompanySales(companySales: pd.DataFrame, start: str, workDays: int, totalDays: int) -> pd.DataFrame:
    """ Generates final company sales data by applying transformations
        and projections.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - start {str} The starting date for filtering data.
        - workDays {int} Number of working days in the month.
        - totalDays {int} Total number of days in the month.

    Returns
        {pandas.DataFrame} Transformed company sales data with projected
            values and differences.
    """

    companySales = baseCompanySales(companySales)
    companySalesLm = generateCompanySalesLm(companySales, start)
    companySalesCm = generateCompanySalesCm(companySales, start)
    companySales = addProjAndDiffColumns(companySalesLm, companySalesCm, workDays, totalDays)

    return companySales

def baseCompanySales(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Generates base company sales data by applying transformations
        and aggregations.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Aggregated company sales data with renamed
            columns.
    """

    COLS_TO_RENAME = {
        "office_rec": "office",
        "gi_sum": "gi",
        "row_count": "receipts"
    }

    initialTransformations(companySales)
    companySales = replaceOfficeValues(companySales)

    companySales = (
        companySales
        .groupby(["week", "date", "office_rec", "for"])
        .agg(
            gi_sum = ("gi", "sum"),
            row_count = ("gi", "count")
        )
        .reset_index()
        .rename(columns=COLS_TO_RENAME)
    )

    return companySales

def initialTransformations(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Applies initial transformations to the company sales data.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Transformed company sales data with date
            and week columns.
    """

    companySales["date"] = pd.to_datetime(companySales["date"])
    companySales["weekday"] = companySales["date"].dt.weekday
    companySales["day_diff"] = companySales["weekday"]
    companySales["week"] = companySales["date"] - pd.to_timedelta(companySales["day_diff"], unit="D")
    companySales["date"] = companySales["date"].dt.strftime("%Y-%m-%d")
    companySales["week"] = companySales["week"].dt.strftime("%Y-%m-%d")

def replaceOfficeValues(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Replaces specific values in the office_rec column of company
        sales data.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} Company sales data with updated office values.
    """

    REPLACEMENTS = {
        "8th": "8th Street",
        "Fontana2": "Fontana 2",
        "Registrations": "DMV"
    }

    companySales["office_rec"] = companySales["office_rec"].replace(REPLACEMENTS, regex=True)

    return companySales

def generateCompanySalesLm(companySales: pd.DataFrame, start: str) -> pd.DataFrame:
    """ Generates last month company sales data.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - start {str} Start date for filtering the data.

    Returns
        {pandas.DataFrame} Filtered and transformed company sales data.
    """

    colsToRename = {
        "gi_sum": "gi last month",
        "nb_total": "nb last month"
    }
    companySalesLm = companySales[(companySales["date"] < start)]
    companySalesLm = transformCompanySales(companySalesLm, colsToRename)

    return companySalesLm

def generateCompanySalesCm(companySales: pd.DataFrame, start: str) -> pd.DataFrame:
    """ Generates current moonth company sales.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - start {str} Start date for filtering the data.

    Returns
        {pandas.DataFrame} Filtered and transformed company sales data.
    """

    colsToRename = {
        "gi_sum": "gi",
        "nb_total": "nb"
    }
    companySalesCm = companySales[(companySales["date"] >= start)]
    companySalesCm = transformCompanySales(companySalesCm, colsToRename)

    return companySalesCm

def transformCompanySales(companySales: pd.DataFrame, columns: dict[str: str]) -> pd.DataFrame:
    """ Transforms company sales data by aggregating and renaming
        columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.
        - columns {dict} Dictionary mapping old column names to
          new names.

    Returns
        {pandas.DataFrame} Aggregated and renamed company sales data.
    """

    companySales = (
        companySales
        .groupby(["office"])
        .agg(
            gi_sum = ("gi", "sum"),
            nb_total = ("receipts", lambda x: x[companySales["for"] == "NewB - EFT To Company"].sum())
        )
        .reset_index()
        .rename(columns=columns)
    )

    return companySales

def addProjAndDiffColumns(companySalesLm: pd.DataFrame, companySalesCm: pd.DataFrame, workDays: int, totalDays: int):
    """ Adds projected and difference columns to company sales data.

    Parameters
        - companySalesLm {pandas.DataFrame} Company sales data from
          last month.
        - companySalesCm {pandas.DataFrame} Current month company
          sales data.
        - workDays {int} Number of workdays in the month.
        - totalDays {int} Total days in the month.

    Returns
        {pandas.DataFrame} Updated company sales data with projected and
            difference columns.
    """

    companySalesCm["gi projected"] = ((companySalesCm["gi"] / workDays) * totalDays)
    companySalesCm["nb projected"] = ((companySalesCm["nb"] / workDays) * totalDays).round().astype(int)

    companySalesCm = companySalesCm.merge(right=companySalesLm, on=["office"], how="outer")

    companySalesCm["monthly gi difference"] = companySalesCm["gi projected"] - companySalesCm["gi last month"]
    companySalesCm["monthly nb difference"] = companySalesCm["nb projected"] - companySalesCm["nb last month"]
    companySalesCm.sort_values(by="gi projected", inplace=True, ascending=False)

    return companySalesCm

def generateOfficeReport(officeReport: list[dict]) -> pd.DataFrame:
    """ Generates a DataFrame with office report data and renamed
        columns.

    Parameters
        - officeReport {list[dict]} List of office data dictionaries.

    Returns
        {pandas.DataFrame} DataFrame with renamed columns from office
            report data.
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
    """ Sets the regional name as a list of strings after normalization.

    Parameters
        - regName {str | list} Single regional name or list of regional
          names.

    Returns
        {list} List of normalized regional names.
    """

    if not isinstance(regName, list):
        regName = [regName]
    
    regName = [normalizeStr(str(name).strip()) for name in regName]
    return regName

def generateTotalSums(companySales: pd.DataFrame, workDays: int, totalDays: int) -> pd.DataFrame:
    """ Generates total sum statistics from company sales data.

    Parameters
        - companySales {pandas.DataFrame} Company sales data with
          various metrics.
        - workDays {int} Number of workdays in the month.
        - totalDays {int} Total days in the month.

    Returns
        {pandas.DataFrame} DataFrame with total sums and calculated
            differences.
    """

    companySales["goal difference"] = companySales["gi projected"] - companySales["new month office goal"]

    totalSums = pd.DataFrame({
        "Goal Total": [companySales["new month office goal"].sum()],
        "GI Total": [companySales["gi"].sum()],
        "NB Total": [companySales["nb"].sum()],
        "Projected GI Total": [companySales["gi projected"].sum()],
        "Projected NB Total": [companySales["nb projected"].sum()],
        "Goal Difference Total": [companySales["goal difference"].sum()],
        "GI Last Month": [companySales["gi last month"].sum()],
        "NB Last Month": [companySales["nb last month"].sum()],
        "Monthly GI Difference Total": [companySales["monthly gi difference"].sum()],
        "Monthly NB Difference Total": [companySales["monthly nb difference"].sum()],
        "Online Sales Total": (((companySales.loc[companySales["office"] == "Online Sales", "nb"].sum()) / workDays) * totalDays)},
    )

    return totalSums
