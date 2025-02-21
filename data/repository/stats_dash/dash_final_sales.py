from data.repository.calls.compliance_repo import Compliance
from service.receipts_for_dash import fetchReceipts
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def dashFinalSales(start: str, end: str, yesterday: str) -> list[dict] | None:
    """ Generates the company sales and total sums to be shown in
    the Production by Office page.

    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.
        - yesterday {str} yesterday's date.

    Returns
        {list[dict] | None} the data that will be
        shown or None if exception raise an error.
    """

    try:
        yesterdayData = getDataRange(yesterday, yesterday)
        lastWeekData = getDataRange(start, end)
    except Exception as e:
        logger.error(f"Error generating data in dashFinalSales: {str(e)}")
        raise
    else:
        return yesterdayData, lastWeekData

def getDataRange(start: str, end: str = None) -> list[dict] | None:
    compliance = Compliance()
    
    offices = compliance.getRegionalsByOffices()
    
    if end == None:
        companySales = fetchReceipts(start, start)
    else:
        companySales = fetchReceipts(start, end)
    
    return processDashOffices(companySales, offices)

def processDashOffices(companySalesDf: pd.DataFrame, offices: list[dict]) -> list[dict]:
    """ Transform the company sales and agi reports to make it ready for the Production
    by Office page.

    Parameters
        - companySalesDf {pandas.DataFrame} Raw company sales data.
        - offices {list[dict]} List of office data dictionaries.

    Returns
        {list[dict]} the transformed company sales and total sums.
    """

    companySalesDf = preMergeTransformations(companySalesDf)
    officeReport = generateOfficeReport(offices)
    workingDays = getWorkingDays()

    companySalesDf = (
        companySalesDf
        .merge(right=officeReport, on="office", how="left")
        .sort_values(by="regional")
    )
    companySalesDf = addDailyGoalColumn(companySalesDf, workingDays)

    companySales = companySalesDf.to_dict(orient="records")

    return companySales

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
    # esperar indicaciones sobre porqué existe 'irvine' en los recibos.
    # borrar la siguiente línea cuando se tenga la respuesta.
    companySales = companySales.query("office != 'Irvine'")

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

    COLS_TO_RENAME = {
        "office_rec": "office",
        "gi_sum2": "gi total",
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
            nb_total = ("row_count1", lambda x: x[csGroupedByDate["for"] == "NewB - EFT To Company"].sum())
        )
        .reset_index()
        .rename(columns=COLS_TO_RENAME)
    )

    return csGroupedByOffice

def generateOfficeReport(officeReport: list[dict]) -> pd.DataFrame:
    """ Generates an office report DataFrame with renamed columns.

    Parameters
        - officeReport {list[dict]} List of dictionaries containing 
        office data.

    Returns
        {pandas.DataFrame} A DataFrame with renamed columns for the 
        report.
    """

    COLS_TO_DELETE = [
        "district",
        "District Email",
        "Regional Email",
        "manager",
        "Manager Email",
        "LAE Office Name",
        "region"
    ]

    return (
        pd.DataFrame(officeReport)
        .drop(columns=COLS_TO_DELETE)
        .rename(columns={"New Month Office Goal": "month goal"})
    )

def getWorkingDays() -> int:
    """ Calculates the number of working days in the month.
    
    Returns
        {int} number of working days.
    """
    
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    firstDayNextMonth = datetime(today.year, today.month + 1, 1) if today.month < 12 else datetime(today.year + 1, 1, 1)
    
    firstDayCurrentMonthNp = np.datetime64(firstDayCurrentMonth, "D")
    firstDayNextMonthNp = np.datetime64(firstDayNextMonth, "D")
    
    workingDays = np.busday_count(
        begindates=firstDayCurrentMonthNp,
        enddates=firstDayNextMonthNp,
        weekmask="1111110"
    )
    
    return workingDays

def addDailyGoalColumn(companySales: pd.DataFrame, workingDays: int) -> pd.DataFrame:
    """ Adds daily goal and amount needed columns to the company sales DataFrame.
    
    Parameters
        - companySales {pandas.DataFrame} DataFrame containing company sales data.
        - workingDays {int} Number of working days in the month.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    
    ORDER = [
        "regional",
        "office",
        "nb total",
        "gi total",
        "daily goal",
        "amount needed"
    ]
    
    companySales["daily goal"] = companySales["month goal"].where(companySales["month goal"] != 0, 0) / workingDays
    companySales["amount needed"] = companySales["gi total"] - companySales["daily goal"]
    
    return (
        companySales
        .drop(columns=["month goal"])
        .reindex(columns=ORDER)
        .reset_index(drop=True)
    )
