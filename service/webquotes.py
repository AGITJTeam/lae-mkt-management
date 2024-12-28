from service.columnsTransformations import wqNewNewColumnNames
from service.helpers import renameColumns
from data.models.webquotes_model import WebquotesModel
from controllers.controller import getWebquotes
import pandas as pd

def generateWebquotesDf(start: str, end: str) -> pd.DataFrame:
    """ Create Webquotes DataFrame with renamed and parsed columns with API
    response.

    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    
    webquotes = []
    webquotesJson = getWebquotes(start, end)
    justWebquotes = webquotesJson["data"]

    for webquote in justWebquotes:
        renamedWebquote = renameJsonKeysForWebquotesModel(webquote)
        webquoteModel = WebquotesModel(**renamedWebquote)
        webquotes.append(webquoteModel)
    
    webquotesDf = pd.DataFrame(webquotes)
    webquotesDf["submission_date"] = pd.to_datetime(webquotesDf["submission_date"], format="%m/%d/%Y", errors="coerce").dt.date
    webquotesDf["submission_on_time"] = pd.to_datetime(webquotesDf["submission_on_time"], format="%I:%M %p", errors="coerce").dt.time
    webquotesDf["date_sold"] = pd.to_datetime(webquotesDf["date_sold"], format="mixed", errors="coerce")
    renamedWebquotesDf = renameColumns(webquotesDf, wqNewNewColumnNames)

    return renamedWebquotesDf

def renameJsonKeysForWebquotesModel(webquotesJson: dict) -> dict:
    """ Delete and create renamed keys for webquote in json format to save
    it in the Webquote Model.

    Parameters
        - webquotesJson {dict} the webquotes in python dict format (works
        as a json object).

    Returns
        {dict} the json with renamed keys.
    """

    oldAndNewColumnNames = {
        "Submission on Time": "submission_on_time",
        "Model Year": "model_year",
        "Marital Status": "marital_status",
        "License Status": "licence_status",
        "Residence Status": "residence_status",
        "region worked at": "region_worked_at",
        "Sold at": "sold_at",
        "Date Sold": "date_sold",
        "Campaign ID": "campaign_id"
    }

    for column in oldAndNewColumnNames.items():
        data = webquotesJson[column[0]]
        webquotesJson.pop(column[0])
        webquotesJson.update({column[1]: data})
    
    return webquotesJson
