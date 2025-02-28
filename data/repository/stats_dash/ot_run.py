from data.repository.calls.compliance_repo import Compliance
from data.repository.calls.helpers import postDataframeToDb
from service.payroll_report import generateAgiReport
from service.receipts_for_dash import fetchReceipts
from service.gi_logic import *
from flask import jsonify, Response
from Crypto.Cipher import AES
import pandas as pd
import numpy as np
import logging, base64, os, datetime

logger = logging.getLogger(__name__)

def otRun(start: str, end: str, username: str, encryptedPassword: str, reportName: str, dashUsername: str) -> Response | None:
    """ Process the data to generate an OT Report and post it in the database.
    
    Parameters
        - start {str} the beggining of the date range.
        - end {str} the end of the date range.
        - username {str} username of Secure2 platform.
        - encryptedPassword {str} encrypted password of Secure2 platform.
        - reportName {str} the name of the report to be created.
        - dashUsername {str} the username of the StatsDash webpage.

    Returns
        {flask.Response} success response or None if exception raise
        an error.
    """ 

    try:
        password = dencryptPassword(encryptedPassword)
        sales, weekSales = generateOtSalesAndWeeksales(start, end, username, password)
    except Exception as e:
        logger.error(f"Error generating OT Report data in otRun: {str(e)}")
        raise

    if sales.empty or weekSales.empty:
        logger.error(f"Error with generated OT Report data in otRun. Empty data")
        raise

    try:
        postOtReport(reportName, dashUsername)
        id = getLastOtReportId()

        postOtReportSales(sales, id, "ot_reports_sales")
        postOtReportSales(weekSales, id, "ot_reports_weeksales")
        return jsonify({"msg": "Success"}), 200
    except Exception as e:
        logger.error(f"Error posting OT Report data in otRun: {str(e)}")
        raise

def dencryptPassword(encryptedPassword: str) -> str:
    """ Decrypts a text using the AES encryption system.

    Parameters
        - encryptedPassword {str} the encrypted password.

    Returns
        {str} the decrypted password
    """

    SECRET_KEY = os.getenv("SECRET_KEY")
    cipher = AES.new(SECRET_KEY.encode(), AES.MODE_ECB)
    decrypted = cipher.decrypt(base64.b64decode(encryptedPassword))

    return decrypted.decode().strip()

def generateOtSalesAndWeeksales(start: str, end: str, username: str, password: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ Generates sales and weeksales dataframes to be used in OT Report.

    Returns
        {flask.Response} the data that will be shown.
    """

    REPORT_ONE_COLUMNS = {
        "reghours": "reg_hours",
        "salhours": "sal_hours",
        "mealpenalty": "meal_penalty",
        "weekot": "week_ot",
        "doubleot": "double_ot",
        "hrpay": "hr_pay",
        "paytype": "pay_type"
    }
    REPORT_ONE_ID = 91065101
    REPORT_TWO_ID = 91065102

    reportOneDf = generateAgiReport(REPORT_TWO_ID, username, password)
    reportTwoDf = generateAgiReport(REPORT_ONE_ID, username, password)
    receiptsDf = fetchReceipts(start, end)

    reportOne = transformAgiReports(reportOneDf, start, end, REPORT_ONE_COLUMNS)
    reportTwo = transformAgiReports(reportTwoDf, start, end)
    receipts = transformReceiptsDf(receiptsDf)

    uniqueReportOne = generateUniqueReportOne(reportOne, reportTwo)
    uniqueReportOne = addRoleToReportOne(uniqueReportOne)
    
    sales = generateSalesDf(uniqueReportOne, receipts)
    weekSales = generateWeekSales(sales)

    return sales, weekSales

def transformAgiReports(report: pd.DataFrame, start: str, end: str, renamedColumns: dict = None) -> pd.DataFrame:
    """ Transform the AGI Report DataFrame by normalizing column names,
    filtering date column, replacing values and filling NA values.

    Parameters:
        - report (pandas.DataFrame): The report to transform.
        - start {str} The start date for filtering the report.
        - end {str} The end date for filtering the report.
        - renamedColumns {dict}: A dictionary to rename columns 
        in the DataFrame if its neccesary.

    Returns:
        {pandas.DataFrame} Resulting DataFrame.
    """

    report = report.copy()

    report.columns = report.columns.str.lower()
    if renamedColumns is not None:
        report.rename(columns=renamedColumns, inplace=True)

    report["date"] = pd.to_datetime(report["date"])
    report = report[(report["date"] >= start) & (report["date"] <= end)]
    report.replace(to_replace="-", value="0", inplace=True)
    report.fillna(value=0, inplace=True)

    return report

def transformReceiptsDf(receipts: pd.DataFrame) -> pd.DataFrame:
    """ Transform the Receipts DataFrame by grouping it and adding GI
    and date columns.

    Parameters
        - receipts {pandas.DataFrame} The receipts data to transform.

    Returns
        {pandas.DataFrame} Resulting DataFrame.
    """

    receipts = receipts.copy()

    receipts["date"] = pd.to_datetime(receipts["date"]).dt.date
    receipts = (
        receipts
        .groupby(["date", "usr"])
        .agg({
            "gi": "sum",
            "for": lambda x: (x == "NewB - EFT To Company").sum()
        })
        .reset_index()
        .rename(columns={"for": "nb", "gi": "gi"})
    )
    receipts["date"] = pd.to_datetime(receipts["date"])
    receipts["weekday"] = receipts["date"].dt.weekday
    receipts["day_diff"] = receipts["weekday"]
    receipts["week"] = receipts["date"] - pd.to_timedelta(receipts["day_diff"], unit="D")
    
    return receipts

def generateUniqueReportOne(reportOne: pd.DataFrame, reportTwo: pd.DataFrame) -> pd.DataFrame:
    """  Generate the Unique AGI Report by merging Report One and Two.

    Parameters
        - reportOne {pandas.DataFrame} The first report to merge.
        - reportTwo {pandas.DataFrame} The second report to merge.

    Returns
        {pandas.DataFrame} Resulting DataFrame
    """

    COLUMNS_TO_DROP_DUPLICATES = [
        "id","name","email","location","position",
        "date","district","regional","manager"
    ]

    counts = generateMissedCountValues(reportTwo)
    uniqueCombinationsDf = reportTwo[COLUMNS_TO_DROP_DUPLICATES].drop_duplicates()
    uniqueReportOneDf = pd.merge(
        left=uniqueCombinationsDf,
        right=reportOne,
        how="outer",
        on=COLUMNS_TO_DROP_DUPLICATES
    )
    uniqueReportOneDf = addRegionalColumns(uniqueReportOneDf, counts)

    return uniqueReportOneDf

def generateMissedCountValues(reportTwo: pd.DataFrame) -> dict:
    """  Generate a dictionary with counts of missed values for 'missed'
    column in Report Two.

    Parameters
        - reportTwo {pandas.DataFrame} The report containing "Missed" data.

    Returns
        {dict} A dictionary with missed values grouped by ID and Date.
    """

    reportTwo["y_count"] = (
        reportTwo["missed"]
        .astype(str)
        .apply(lambda x: x.count("Y"))
    )

    counts = reportTwo.groupby(["id", "date"])["y_count"].sum().to_dict()

    return counts

def addRegionalColumns(reportOne: pd.DataFrame, counts: dict) -> pd.DataFrame:
    """ Add columns to report one related to regional information, add
    date related columns and normalize emails.

    Parameters
        - reportOne {pandas.DataFrame} The report to transform.
        - counts {dict} A dictionary of counts to use.

    Returns
        {pandas.DataFrame} Resulting DataFrame.
    """

    reportOne = reportOne.copy()

    reportOne.fillna(value=0, inplace=True)
    reportOne["date"] = pd.to_datetime(reportOne["date"])
    reportOne["missing"] = [counts.get((row["id"], row["date"]), 0) for index, row in reportOne.iterrows()]
    reportOne["email"] = reportOne["email"].str.lower()
    reportOne["district"] = reportOne["district"].str.split("(").str[0]
    reportOne["regional"] = reportOne["regional"].str.split("(").str[0]
    reportOne["manager"] = reportOne["manager"].str.split("(").str[0]
    reportOne["weekday"] = reportOne["date"].dt.weekday
    reportOne["day_diff"] = reportOne["weekday"]
    reportOne["week"] = reportOne["date"] - pd.to_timedelta(reportOne["day_diff"], unit="D")

    return reportOne

def addRoleToReportOne(uniqueReportOne: pd.DataFrame) -> pd.DataFrame:
    """ Adds a 'Role' column to the report by renaming specific columns
        and categorizing data based on conditions.

    Parameters
        - uniqueReportOne {pandas.DataFrame} A DataFrame containing the
        initial report data.

    Returns
        {pandas.DataFrame} The modified DataFrame with a new 'Role' column.
    """

    COLS_TO_RENAME = {
        "reg_hours": "hours",
        "sal_hours": "salary",
        "week_ot": "wk",
        "double_ot": "double",
        "meal_penalty": "penalty",
        "pay_type": "type_p",
        "hr_pay": "pay"
    }
    POSITIONS_TO_KEEP = [
        "Floor Assistant",
        "Floater", "Agent",
        "Trainee",
        "CSR",
        "Call Center",
        "Sales Development Representative"
    ]
    CONDITIONS = [
        (uniqueReportOne["location"] == "Commercial") & (uniqueReportOne["position"] != "Senior Commercial Underwriter"),
        (uniqueReportOne["position"].isin(POSITIONS_TO_KEEP)) & (uniqueReportOne["location"] != "Commercial")
    ]
    CHOICES = ["Commercial", "Sales"]

    uniqueReportOne.rename(columns=COLS_TO_RENAME, inplace=True)
    uniqueReportOne["role"] = np.select(condlist=CONDITIONS, choicelist=CHOICES, default="Admin")

    return uniqueReportOne

def generateSalesDf(uniqueReportOne: pd.DataFrame, receipts: pd.DataFrame) -> pd.DataFrame:
    """ Generates a sales DataFrame by merging the report data with
        receipts, selecting specific columns, and formatting them.

    Parameters
        - uniqueReportOne {pandas.DataFrame} A DataFrame containing the
        report data.
        - receipts {pandas.DataFrame} A DataFrame containing receipt data.

    Returns
        {pandas.DataFrame} The merged and formatted sales DataFrame.
    """

    COLS_TO_KEEP = [
        "role", "name", "location", "position", "date", "week", "hours", "salary",
        "overtime", "wk", "double", "penalty", "sick", "vacation", "type_p",
        "pay", "missing", "gi", "nb", "district", "regional", "manager"
    ]

    sales = (
        pd.merge(
            left=uniqueReportOne,
            right=receipts,
            left_on=["date", "week"],
            right_on=["date", "week"],
            how="left"
        )
        .fillna(0)
    )

    sales["pay"] = sales["pay"].fillna(0).apply(
        lambda x: x.replace("$", "") if isinstance(x, str) else x
    )
    

    sales = (
        sales[COLS_TO_KEEP]
        .assign(
            date = lambda df: df["date"].dt.strftime("%Y-%m-%d"),
            week = lambda df: df["week"].dt.strftime("%Y-%m-%d")
        )
        .fillna("")
    )

    # sales["date"] = pd.to_datetime(arg=sales["date"], format="%Y-%m-%d")
    # sales["week"] = pd.to_datetime(arg=sales["week"], format="%Y-%m-%d")

    return sales

def generateWeekSales(resultSales: pd.DataFrame) -> pd.DataFrame:
    """ Generates weekly sales data by grouping the data by specific
        columns, performing calculations on certain columns, and
        applying custom calculations.

    Parameters
        - resultSales {pandas.DataFrame} A DataFrame containing the sales
        data.

    Returns
        {pandas.DataFrame} The aggregated weekly sales data with calculated
        columns.
    """

    COLS_TO_CONVERT = [
        "hours","salary","overtime","wk","double","penalty","sick",
        "vacation","missing","gi","nb"
    ]
    COLS_GROUP = [
        "role", "name", "location", "position", "week","district",
        "regional", "manager"
    ]
    COLS_TO_KEEP = [
        "role","name","location", "position","week","hours","overtime",
        "penalty","extra","missing","gi","nb","district","regional","manager"
    ]

    resultSales[COLS_TO_CONVERT] = resultSales[COLS_TO_CONVERT].apply(
        lambda col: pd.to_numeric(col, errors="coerce").fillna(0)
    )

    columnsOperations = {col: "sum" for col in COLS_TO_CONVERT}
    weekSales = resultSales.groupby(COLS_GROUP).agg(columnsOperations).reset_index()

    weekSales["hours"] += weekSales["salary"]
    weekSales["overtime"] += weekSales["wk"] + weekSales["double"]
    weekSales["extra"] = weekSales["sick"] + weekSales["vacation"]
    weekSales = weekSales[COLS_TO_KEEP]

    calculations = {
        "gi for authorized": calculateLeftGi,
        "unauthorized ot": calculateIfUnauthorizedOt,
        "reason": calculateIfUnauthorizedOtRes,
        "justified ot": calculateAuthorizedOt,
        "unjustified ot": calculateUnauthorizedOt,
    }

    for col, func in calculations.items():
        weekSales[col] = weekSales.apply(
            lambda row: func(row["overtime"], row["gi"], row["position"]), axis=1
        )
    
    return weekSales

def postOtReport(reportName: str, dashUsername: str) -> None:
    """ Post the OT Report name and date created in the database.
    
    Parameters
        - reportName {str} The name of the report to be created.
        - dashUsername {str} The username of the StatsDash webpage.
    """

    now = datetime.datetime.now()
    data = {
        "report_name": reportName,
        "date_created": now,
        "created_by": dashUsername
    }

    otReportDf = pd.DataFrame(data=[data])
    postDataframeToDb(data=otReportDf, table="ot_reports", mode="append", filename="k_db.ini")

def getLastOtReportId() -> int:
    """ Once the OT Report is created, get its id.
    
    Returns
        {int} The id of the OT Report just created.
    """

    compliance = Compliance()

    try:
        response = compliance.getLastOtReportId()
    except Exception as e:
        logger.error(f"Error in getLastOtReportId: {str(e)}")
        raise
    else:
        if not response:
            raise Exception("No OT Report Id found")
        
        id = response[0]["id"]
        return id

def getNumberOfOtReports() -> int:
    """ Retrieve the number of OT Reports in the database.
    
    Returns
        {int} the number of OT Reports in the database.
    """

    compliance = Compliance()

    try:
        response = compliance.getNumberOfOtReports()
    except Exception as e:
        logger.error(f"Error in getNumberOfOtReports: {str(e)}")
        raise
    else:        
        return response[0]["count"]

def postOtReportSales(df: pd.DataFrame, otReportId: int, table: str) -> Response:
    """ Post all the sales registered for the OT Report in the database.
    
    Parameters
        - df {pandas.DataFrame} The sales data to be posted.
        - otReportId {int} The id of the OT Report just created.
        - table {str} The table where the data will be posted.
    """

    df["id_ot_report"] = otReportId

    postDataframeToDb(data=df, table=table, mode="append", filename="k_db.ini")
