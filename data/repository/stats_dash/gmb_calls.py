
from flask import flash, jsonify, make_response, request, Response
from werkzeug.datastructures import FileStorage
import pandas as pd, io, logging

from data.repository.calls.lae_data_repo import LaeData

logger = logging.getLogger(__name__)

GMB_COLUMN_NAMES: dict[str, str] = {
    "0": "nb",
    "1": "bf",
    "2": "endos",
    "3": "payments",
    "4": "invoice",
    "5": "dmv",
    "6": "towing",
    "7": "permit",
    "8": "traffic_school",
    "9": "renewal",
    "10": "trucking",
    "11": "immigration",
}

def generateExcelReport(start: str, end: str, file: FileStorage) -> pd.DataFrame:
    """  Generate a PDF response containing a GMB calls report.

    Parameters
        - start {str} The start date for generating the report.
        - end {str} The end date for generating the report.
        - file {FileStorage} The uploaded file containing GMB calls data.

    Returns
        {Response} A Flask response object.
            Contains the generated Excel report as a downloadable file.
    """

    try:
        return generateGmbCrossDf(start, end, file)
    except Exception as e:
        logger.error(f"Error generating GMB calls report in generateExcelReport: {str(e)}")
        raise

def generateGmbCrossDf(start: str, end: str, file) -> pd.DataFrame:
    onePhoneDf = generateLaeOnePhoneDf(start, end)
    twoPhoneDf = generateLaeTwoPhoneDf(start, end)
    gmbDf = generateGmbDf(file)

    gmbNumbers = gmbDf["Caller"].tolist()
    result = []
    
    for number in gmbNumbers:
        num = "".join(number.split("-"))
        founded = searchGmbNumberInLaeData(num, onePhoneDf, twoPhoneDf)
        result.append(founded)
    
    gmbCountedFor = pd.DataFrame(data=result, columns=GMB_COLUMN_NAMES.values())
    crossedGmb = pd.concat([gmbDf, gmbCountedFor], axis=1)

    return crossedGmb

def generateLaeOnePhoneDf(start: str, end: str) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data indexed by one phone.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "phone fix" 
            containing aggregated LAE data.
    """

    laeDf = generateLaeDataDf(start, end)

    laeOnePhoneDf = pd.pivot_table(
        data=laeDf,
        values=GMB_COLUMN_NAMES.values(),
        index=["phone_fix"],
        aggfunc="sum",
        dropna=False
    )
    laeOnePhoneDf = laeOnePhoneDf.loc[:, GMB_COLUMN_NAMES.values()]

    return laeOnePhoneDf

def generateLaeTwoPhoneDf(start: str, end: str) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data filtered by two phones.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "cell phone"
            and "phone" for aggregated LAE data with two phones.
    """

    laeDf = generateLaeDataDf(start, end)
    laeFiltered = laeDf[laeDf["phone_fix"].isin(["2 Phones"])]
    laeTwoPhonePv = pd.pivot_table(
        data=laeFiltered,
        values=GMB_COLUMN_NAMES.values(),
        index=["cell_phone", "phone"],
        aggfunc="sum"
    )
    laeTwoPhonePv = laeTwoPhonePv.loc[:, GMB_COLUMN_NAMES.values()]

    return laeTwoPhonePv


def generateLaeDataDf(start: str, end: str) -> pd.DataFrame:
    lae = LaeData()
    response = lae.getBetweenDates(start, end)

    return pd.DataFrame(response)

def generateGmbDf(file) -> pd.DataFrame:
    fileStream = io.TextIOWrapper(file.stream, encoding="utf-8")
    gmbDf = pd.read_csv(fileStream, skiprows=9, delimiter=",")
    fileStream.close()

    return gmbDf

def searchGmbNumberInLaeData(number: str, onePhonePv: pd.DataFrame, twoPhonePv: pd.DataFrame) -> list:
    if number in onePhonePv.index:
        #print("Founded in Lae Phone Fix column")
        rows = onePhonePv.loc[number]
        if isinstance(rows, pd.DataFrame):
            return rows.sum(axis=0).tolist()

        return rows.to_list()

    if number in twoPhonePv.index.get_level_values("cell_phone"):
        #print("Found in Lae Cell Phone column with 2 Phones filter")
        rows = twoPhonePv.loc[(number, slice(None))]
        if isinstance(rows, pd.DataFrame):
            return rows.sum(axis=0).tolist()
        
        return rows.values.flatten().tolist()

    if number in twoPhonePv.index.get_level_values("phone"):
        filtered = twoPhonePv[twoPhonePv.index.get_level_values("phone") == number]
        #print("Found in Lae Phone column with 2 Phones filter")
        if isinstance(filtered, pd.DataFrame):
            return filtered.sum(axis=0).tolist()

        return filtered.values.flatten().tolist()
    
    return [None] * len(onePhonePv.columns)
