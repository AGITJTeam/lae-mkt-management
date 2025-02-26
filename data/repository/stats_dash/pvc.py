from data.repository.calls.main_data_repo import MainData
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
    mainData = MainData()
    agentEmails = mainData.getAgentRegionAndOffice()
    agentEmailsDf = pd.DataFrame(agentEmails)
    agentEmailsDf["usr_email"] = agentEmailsDf["usr_email"].str.lower()
    agentEmailsDf.rename({"officename": "Office", "regionname": "Region"}, axis=1, inplace=True)
    
    receipts = calculateGiByAgent(start, end)
    missingPunchesDf = processMpDf(mpReportId)
    pvcDf = processPvcDf(pvcReportId)

    pvcDf = pvcDf.merge(right=agentEmailsDf, on="usr_email", how="left")
    pvcDf = pvcDf.merge(right=receipts, on="usr_email", how="left")
    pvcDf = pvcDf.merge(right=missingPunchesDf, on="usr_email", how="left")
    
    pvcDf = addPercentagesColumns(pvcDf)
    pvcDf = renamePvcColumns(pvcDf)
    
    pvcData = pvcDf.to_dict(orient="records")

    return pvcData

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
    addDateColumns(receipts)
    receipts = groupByOfficeAddition(receipts)
    
    return receipts

def addDateColumns(companySales: pd.DataFrame) -> None:
    """ Change 'date' column type and add some date columns.

    Parameters
        - companySales {pandas.DataFrame} Raw company sales data.

    Returns
        {None} DataFrame with new columns.
    """

    companySales["date"] = pd.to_datetime(companySales["date"])
    companySales["weekday"] = companySales["date"].dt.weekday
    companySales["day_diff"] = companySales["weekday"]
    companySales["week"] = companySales["date"] - pd.to_timedelta(companySales["day_diff"], unit="D")
    companySales["date"] = companySales["date"].dt.strftime("%Y-%m-%d")
    companySales["week"] = companySales["week"].dt.strftime("%Y-%m-%d")

def groupByOfficeAddition(companySales: pd.DataFrame) -> pd.DataFrame:
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

def processMpDf(reportId: int) -> pd.DataFrame:
    mpReport = generateAgiReport(reportId)
    mpReport.rename(columns={"Primary Email": "usr_email", "Missed Punch": "Missing"}, inplace=True)
    mpReport.drop(columns=["Entity(Position)"], inplace=True)
    
    return mpReport

def processPvcDf(reportId: int) -> pd.DataFrame:
    """ Change column types and add calculated columns to a Agi Report
    DataFrame.
    
    Parameters
        - reportId {int} id of the Agi Report.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    pvcReport = generateAgiReport(reportId)
    pvcReport.rename(columns={"Primary Email": "usr_email"}, inplace=True)
    pvcReport["usr_email"] = pvcReport["usr_email"].str.lower()

    pvcReport["Regional Manager Name"] = pvcReport["Regional Manager Name"].str.split("(").str[0]

    pvcReport["Hourly Pay"] = pvcReport["Hourly Pay"].str.replace("$", "").astype(float)
    pvcReport["Regular Hours"] = pvcReport["Regular Hours"].str.replace("-", "0").astype(float)
    pvcReport["Overtime - Daily Hours"] = pvcReport["Overtime - Daily Hours"].str.replace("-", "0").astype(float)
    pvcReport["Overtime - Weekly Hours"] = pvcReport["Overtime - Weekly Hours"].str.replace("-", "0").astype(float)

    pvcReport["Date"] = pd.to_datetime(pvcReport["Date"])
    pvcReport["Date Hired"] = pd.to_datetime(pvcReport["Date Hired"])

    pvcReport["YrsWrkd"] = pvcReport.apply(lambda row: parseDateHired(row["Date Hired"], row["Date"]), axis=1)
    pvcReport["Payroll Cost"] = pvcReport["Hourly Pay"] * pvcReport["Regular Hours"]
    pvcReport["Payroll Cost with OT"] = (pvcReport["Hourly Pay"] * pvcReport["Regular Hours"]) + (pvcReport["Hourly Pay"] * 1.5 * pvcReport["Overtime - Daily Hours"])
    
    pvcReport["Date"] = pvcReport["Date"].dt.strftime("%Y-%m-%d")
    pvcReport["Date Hired"] = pvcReport["Date Hired"].dt.strftime("%Y-%m-%d")

    return pvcReport

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

    pvcDf["PVC"] = (pvcDf["Payroll Cost"] / pvcDf["GI / Setter NB"] * 100).where(pvcDf["GI / Setter NB"] != 0, 0)
    pvcDf["PVC with OT"] = (pvcDf["Payroll Cost with OT"] / pvcDf["GI / Setter NB"] * 100).where(pvcDf["GI / Setter NB"] != 0, 0)

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
        "Entity(Location)": "Location",
        "Employee Name": "Name",
        "Date Hired": "Hired Date",
        "Regional Manager Name": "Regional/Manager"
    }
    
    pvcDf.rename(columns=COLS, inplace=True)
    pvcDf.drop(columns=["usr_email", "Date"], inplace=True)
    
    return pvcDf