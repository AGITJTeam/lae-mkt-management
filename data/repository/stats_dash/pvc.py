from data.repository.calls.compliance_repo import Compliance
from service.payroll_report import generateAgiReport
from service.receipts_for_dash import fetchReceipts
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def dashPvc() -> tuple[list[dict], list[dict]] | None:
    """ Generates the PVC data to be shown in the PVC page.

    Returns
        {tuple[list[dict], list[dict]] | None} the data that will be
        shown or None if exception raise an error.
    """

    YESTERDAY_REPORT_ID = 91751417
    LAST_WEEK_REPORT_ID = 91756808
    YESTERDAY_MP_ID = 91756855
    LAST_WEEK_MP_ID = 91758575
    YESTERDAY = getYesterdayDate()
    START, END = getLastWeekRange()

    try:
        yesterdayData = processPvc(YESTERDAY_REPORT_ID, YESTERDAY_MP_ID, YESTERDAY, YESTERDAY)
        lastWeekData = processPvc(LAST_WEEK_REPORT_ID, LAST_WEEK_MP_ID, START, END)
    except Exception as e:
        logger.error(f"Error generating data in dashPvc: {str(e)}")
        raise
    else:
        return yesterdayData, lastWeekData

def getLastWeekRange() -> tuple[str, str]:
    """ Gets the start and end dates of the last week in YYYY-MM-DD format.
    
    Returns
        {tuple[str, str]} start and end dates of the last week.
    """
    
    today = datetime.today()
    lastMonday = today - timedelta(days=today.weekday() + 7)
    lastSunday = lastMonday + timedelta(days=6)
    
    return lastMonday.strftime("%Y-%m-%d"), lastSunday.strftime("%Y-%m-%d")

def getYesterdayDate() -> str:
    """ Gets the date of yesterday in YYYY-MM-DD format.
    
    Returns
        {str} date of yesterday.
    """
    
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def processPvc(pvcReportId: int, mpReportId: int, start: str, end: str) -> list[dict] | None:
    officesDataDf = cleanRegionalsEmailsDf()    
    giByAgentEmailDf = calculateGiByAgent(start, end)
    mpByAgentEmailDf = formatMissedPunches(mpReportId)
    pvcDf = formatPvcDf(pvcReportId)

    pvcDf = pvcDf.merge(right=officesDataDf, on="office", how="left")
    pvcDf = pvcDf.merge(right=giByAgentEmailDf, on="usr_email", how="left")
    pvcDf = pvcDf.merge(right=mpByAgentEmailDf, on="usr_email", how="left")

    pvcDf = replaceColumnValues(pvcDf)
    pvcDf = addPayrollColumns(pvcDf)
    pvcDf = addPercentagesColumns(pvcDf)
    pvcDf = renamePvcColumns(pvcDf)
    pvcDf = filNanValues(pvcDf)
    pvcDf = addMissingRegionals(pvcDf)

    pvcData = pvcDf.to_dict(orient="records")

    return pvcData

def cleanRegionalsEmailsDf() -> pd.DataFrame:
    """ Gets office data from Compliance db, change types, rename and
    delete unnecessary columns.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    COLS_TO_DELETE = [
        "district", "District Email", "Regional Email", "manager",
        "Manager Email", "LAE Office Name", "New Month Office Goal"
    ]

    compliance = Compliance()
    officeData = compliance.getRegionalsByOffices()
    officeDataDf = pd.DataFrame(officeData)

    officeDataDf.drop(columns=COLS_TO_DELETE, inplace=True)
    officeDataDf.rename({"regional": "Regional/Manager", "region": "Region"}, axis=1, inplace=True)

    return officeDataDf

def calculateGiByAgent(start: str, end: str) -> pd.DataFrame:
    """ Gets receipts data in a given date range and calculate the GI
    by agent.

    Parameters
        - start {str} beginning of date range.
        - end {str} end of date range.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    receipts = fetchReceipts(start, end)
    addAndFormatDateColumns(receipts)
    receipts = groupAndSumGiByAgent(receipts)

    return receipts

def addAndFormatDateColumns(companySales: pd.DataFrame) -> None:
    """ Change 'date' column type and add some date columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {None} DataFrame with new columns.
    """

    companySales["date"] = pd.to_datetime(companySales["date"])
    companySales["weekday"] = companySales["date"].dt.weekday
    companySales["day_diff"] = companySales["weekday"]
    companySales["week"] = companySales["date"] - pd.to_timedelta(
        companySales["day_diff"],
        unit="D"
    )
    companySales["date"] = companySales["date"].dt.strftime("%Y-%m-%d")
    companySales["week"] = companySales["week"].dt.strftime("%Y-%m-%d")

def groupAndSumGiByAgent(companySales: pd.DataFrame) -> pd.DataFrame:
    """ Groups company sales data by week, date, and office, and
        calculates the GI by agent.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {pandas.DataFrame} A DataFrame grouped by office with
        aggregated data.
    """

    COLS_TO_RENAMME = {
        "office_rec": "Office",
        "gi_sum2": "GI / Setter NB"
    }

    csGroupedByDate = (
        companySales
        .groupby(["week", "date", "usr_email", "for"])
        .agg(
            gi_sum1 = ("gi", "sum")
        )
        .reset_index()
    )

    csGroupedByOffice = (
        csGroupedByDate
        .groupby(["usr_email"])
        .agg(
            gi_sum2 = ("gi_sum1", "sum")
        )
        .rename(columns=COLS_TO_RENAMME)
        .sort_values(by="GI / Setter NB", ascending=False)
        .reset_index()
    )

    return csGroupedByOffice

def formatMissedPunches(reportId: int) -> pd.DataFrame:
    mpReport = generateAgiReport(reportId)

    mpReport = (
        mpReport
        .groupby(by=["Primary Email"])
        .agg(
            Missing=("Missed Punch", lambda x: f"Missing Punches: {x.count()}")
        )
        .reset_index()
    ).rename(columns={"Primary Email": "usr_email"})

    return mpReport

def formatPvcDf(reportId: int) -> pd.DataFrame:
    """ Change column types and add calculated columns to a Agi Report
    DataFrame.

    Parameters
        - reportId {int} id of the Agi Report.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    pvcReport = generateAgiReport(reportId)
    pvcReport.rename(
        columns={
            "Primary Email": "usr_email", "Entity(Location)": "office"
        },
        inplace=True
    )
    pvcReport["usr_email"] = pvcReport["usr_email"].str.lower()

    return pvcReport

def replaceColumnValues(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Replace '$' and '-' values in Hourly Pay, Regular Hours,
    Overtime - Daily Hours and Overtime - Weekly Hours columns and change
    column type.

    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to replace column values.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    pvcDf["Hourly Pay"] = (
        pvcDf["Hourly Pay"].str.replace("$", "").astype(float)
    )

    pvcDf["Regular Hours"] = (
        pvcDf["Regular Hours"].str.replace("-", "0").astype(float)
    )

    pvcDf["Overtime - Daily Hours"] = (
        pvcDf["Overtime - Daily Hours"].str.replace("-", "0").astype(float)
    )

    pvcDf["Overtime - Weekly Hours"] = (
        pvcDf["Overtime - Weekly Hours"].str.replace("-", "0").astype(float)
    )

    return pvcDf

def addPayrollColumns(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Add Payroll Cost, Payroll Cost with OT and Years Worked columns
    to the Pvc DataFrame.

    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to add Payroll and Years
        Worked columns.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    pvcDf["Date"] = pd.to_datetime(pvcDf["Date"])
    pvcDf["Date Hired"] = pd.to_datetime(pvcDf["Date Hired"])

    pvcDf["YrsWrkd"] = pvcDf.apply(
        lambda row: parseDateHired(row["Date Hired"], row["Date"]), axis=1
    )

    pvcDf["Payroll Cost"] = (
        pvcDf["Hourly Pay"] * pvcDf["Regular Hours"]
    )

    pvcDf["Payroll Cost with OT"] = (
        (pvcDf["Hourly Pay"] * pvcDf["Regular Hours"]) +
        (pvcDf["Hourly Pay"] * 1.5 * pvcDf["Overtime - Daily Hours"])
    )

    pvcDf["Date"] = pvcDf["Date"].dt.strftime("%Y-%m-%d")
    pvcDf["Date Hired"] = pvcDf["Date Hired"].dt.strftime("%Y-%m-%d")

    return pvcDf

def parseDateHired(hired: str, date) -> str:
    """ Gets the difference between the date hired and the date of the
    report in 'x years, y months' format.

    Parameters
        - hired {str} date hired in YYYY-MM-DD format.
        - date {str} date of the report in YYYY-MM-DD format.

    Returns
        {str} hired date in 'x years, y months' format.
    """

    delta = relativedelta(date, hired)
    return f"{delta.years} years, {delta.months} months"

def addPercentagesColumns(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Add PVC and PVC with OT percentages columns to the DataFrame.

    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to add percentages columns.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    pvcDf["PVC"] = (
        (pvcDf["Payroll Cost"] / pvcDf["GI / Setter NB"])
        .where(pvcDf["GI / Setter NB"] != 0, 0)
    )

    pvcDf["PVC with OT"] = (
        (pvcDf["Payroll Cost with OT"] / pvcDf["GI / Setter NB"])
        .where(pvcDf["GI / Setter NB"] != 0, 0)
    )

    return pvcDf

def renamePvcColumns(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Rename resulting Pvc DataFrame columns.

    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to rename.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    COLS = {
        "Entity(Position)": "Position",
        "office": "Location",
        "Employee Name": "Name",
        "Date Hired": "Hired Date"
    }

    pvcDf.rename(columns=COLS, inplace=True)
    pvcDf.drop_duplicates(subset=["usr_email"], inplace=True)
    pvcDf.drop(columns=["usr_email", "Date"], inplace=True)

    return pvcDf

def filNanValues(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Fill NaN values all columns of the Pvc DataFrame.

    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to fill NaN values.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    VALUES = {
        "Position": "",
        "Location": "",
        "Name": "",
        "Hired Date": "",
        "Hourly Pay": 0,
        "Regular Hours": 0,
        "Overtime - Daily Hours": 0,
        "Overtime - Weekly Hours": 0,
        "Regional/Manager": "",
        "Region": "",
        "GI / Setter NB": 0,
        "Missing": "",
        "YrsWrkd": "",
        "Payroll Cost": 0,
        "Payroll Cost with OT": 0,
        "PVC": 0,
        "PVC with OT": 0
    }

    pvcDf = pvcDf.fillna(value=VALUES)

    return pvcDf

def addMissingRegionals(pvcDf: pd.DataFrame) -> pd.DataFrame:
    """ Add missing regionals to Regional/Manager column. The missing
    regionales are "Call Center" and "Rancho Cucammonga".
    
    Parameters
        - pvcDf {pandas.DataFrame} DataFrame to add missing regionals.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    
    MISSING_REGIONALS = ["Call Center", "Rancho Cucamonga"]
    
    pvcDf.loc[pvcDf["Location"].isin(MISSING_REGIONALS) & (pvcDf["Regional/Manager"] == ""), "Regional/Manager"] = pvcDf["Location"]
    
    return pvcDf
