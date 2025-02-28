from data.repository.stats_dash.mkt_helpers import *
from werkzeug.datastructures import FileStorage
import pandas as pd
import io, logging

logger = logging.getLogger(__name__)

GMB_COLUMN_NAMES: list[str] = [
    "nb",
    "bf",
    "endos",
    "payments",
    "invoice",
    "dmv",
    "towing",
    "permit",
    "traffic_school",
    "renewal",
    "trucking",
    "immigration",
]

def generateGmbCallsReport(start: str, end: str, file: FileStorage) -> pd.DataFrame:
    """ Generate the DataFrame containing a GMB calls report.

    Parameters
        - start {str} The start date for generating the report.
        - end {str} The end date for generating the report.
        - file {werkzeug.datastructures.FileStorage} The uploaded file
        containing GMB calls data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    try:
        return generateGmbCrossDf(start, end, file)
    except Exception as e:
        logger.error(f"Error generating GMB calls report in generateGmbCallsReport: {str(e)}")
        raise

def generateGmbCrossDf(start: str, end: str, file: FileStorage) -> pd.DataFrame:
    """ Transform the lae phone data and GMB calls file data to cross
    them for GMB Calls Report Creation page.
    
    Parameters
        - start {str} The start date for generating the report.
        - end {str} The end date for generating the report.
        - file {werkzeug.datastructures.FileStorage} The uploaded file
        containing GMB calls data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    onePhoneDf = generateLaeOnePhoneDf(start, end, GMB_COLUMN_NAMES)
    twoPhoneDf = generateLaeTwoPhoneDf(start, end, GMB_COLUMN_NAMES)
    gmbDf = generateGmbDf(file)

    gmbNumbers = gmbDf["Caller"].tolist()
    result = []
    
    for number in gmbNumbers:
        num = "".join(number.split("-"))
        founded = searchNumberInLaeData(num, onePhoneDf, twoPhoneDf)
        result.append(founded)

    gmbCountedFor = pd.DataFrame(data=result, columns=GMB_COLUMN_NAMES)
    crossedGmb = pd.concat([gmbDf, gmbCountedFor], axis=1)

    return crossedGmb

def generateGmbDf(file: FileStorage) -> pd.DataFrame:
    """ Generates a DataFrame from the GMB calls file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} The uploaded file
        containing GMB calls data.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    fileStream = io.TextIOWrapper(file.stream, encoding="utf-8")
    gmbDf = pd.read_csv(fileStream, skiprows=9, delimiter=",")
    fileStream.close()

    return gmbDf
