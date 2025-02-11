from data.repository.stats_dash.mkt_helpers import *
from werkzeug.datastructures import FileStorage
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def generateYelpCallsReport(start: str, end: str, yelpCalls: FileStorage, yelpCodes: FileStorage) -> pd.DataFrame:
    """ Generate the DataFrame containing the Yelp Calls Report data.

    Parameters
        - start {str} The start date for generating the report.
        - end {str} The end date for generating the report.
        - yelpCalls {werkzeug.datastructures.FileStorage} The uploaded
        file containing Yelp calls data.
        - yelpCodes {werkzeug.datastructures.FileStorage} The uploaded
        file containing Yelp codes data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    try:
        yelpDf = crossYelpDataWithLae(start, end, yelpCalls)
        officeDf = generateOfficeDataForYelpDf(yelpDf, yelpCodes)
        yelpReportDf = pd.concat([yelpDf, officeDf], axis=1)
    except Exception as e:
        logger.error(f"Error generating Yelp calls report in generateYelpCallsReport: {str(e)}")
        raise
    else:
        return yelpReportDf

def crossYelpDataWithLae(start: str, end: str, yelpCalls: FileStorage) -> pd.DataFrame:
    """ Generate the DataFrame containing the Yelp Calls data crossed
    with the Lae data.
    
    Parameters
        - start {str} The start date for generating the report.
        - end {str} The end date for generating the report.
        - yelpCalls {werkzeug.datastructures.FileStorage} The uploaded
        file containing Yelp calls data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    GMB_COLUMN_NAMES: list[str] = [
        "nb", "bf", "endos", "payments",
        "invoice", "dmv", "towing", "permit",
        "traffic_school", "renewal", "trucking", "immigration"
    ]

    yelpDf = generateYelpCallsDf(yelpCalls)
    onePhoneDf = generateLaeOnePhoneDf(start, end, GMB_COLUMN_NAMES)
    twoPhoneDf = generateLaeTwoPhoneDf(start, end, GMB_COLUMN_NAMES)
    data = []

    for row in yelpDf.iterrows():
        callerNumber = row[1].at["Caller Number"]

        if callerNumber == "":
            founded = [None] * len(onePhoneDf.columns)
        else:
            founded = searchNumberInLaeData(callerNumber, onePhoneDf, twoPhoneDf)
        
        data.append(founded)

    laeNumbersData = pd.DataFrame(data=data, columns=GMB_COLUMN_NAMES)
    crossedYelp = pd.concat([yelpDf, laeNumbersData], axis=1)

    return crossedYelp

def generateYelpCallsDf(file: FileStorage) -> pd.DataFrame:
    """ Generates a DataFrame from the Yelp calls file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} The uploaded file
        containing Yelp calls data.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    yelpDf = pd.read_excel(io=file, sheet_name="Detail", skiprows=4, engine="xlrd")

    yelpDf = yelpDf.astype({
        "Date": str,
        "Number Called": str,
        "Termination Number": str,
        "Caller Number": str,
        "Duration": str,
        "Start Time": str,
        "Answer Time": str,
        "End Time": str
    })
    yelpDf["Caller Number"] = (
        yelpDf["Caller Number"]
        .str.split(".").str[0]
        .fillna("")
    )

    return yelpDf

def generateOfficeDataForYelpDf(yelpCalls: pd.DataFrame, yelpCodes: FileStorage) -> pd.DataFrame:
    """ Generate the DataFrame containing the Office data from Yelp
    codes file for the Yelp Calls data.
    
    Parameters
        - yelpCalls {pandas.DataFrame} The DataFrame containing Yelp and
        Lae crossed data.
        - yelpCodes {werkzeug.datastructures.FileStorage} The uploaded
        file containing Yelp codes data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    codesDf = generateYelpCodesDf(yelpCodes)
    data = []

    for row in yelpCalls.iterrows():
        numberCalled = row[1].at["Number Called"]
        founded = codesDf[codesDf["Number Called"].isin([numberCalled])]
        officeData = founded.drop(columns=["Yelp Code", "Number Called"])
        data.append(officeData)

    officeDataForYelp = pd.concat(data, ignore_index=True)
    return officeDataForYelp

def generateYelpCodesDf(file: FileStorage) -> pd.DataFrame:
    """ Generates a DataFrame from the Yelp calls file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} The uploaded file
        containing Yelp codes data.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    codesDf = pd.read_excel(io=file, engine="openpyxl")

    codesDf = codesDf.astype({
        "Yelp Code": str,
        "Address": str,
        "City": str,
        "Zip Code": str,
        "Number Called": str
    })
    codesDf["Number Called"] = codesDf["Number Called"].str.removeprefix("1")

    return codesDf
