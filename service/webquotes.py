from service.columnsTransformations import wqNewNewColumnNames
from service.helpers import renameColumns
from data.models.webquotes_model import WebquotesModel
from controllers.controller import getWebquotes
import pandas as pd

def generateWebquotes() -> pd.DataFrame:
    webquotes = []
    webquotesJson = getWebquotes()
    justWebquotes = webquotesJson["data"]

    for webquote in justWebquotes:
        renamedWebquote = renameJsonForWebquotesModel(webquote)
        webquoteModel = WebquotesModel(**renamedWebquote)
        webquotes.append(webquoteModel)
    
    webquotesDf = pd.DataFrame(webquotes)
    webquotesDf["submission_date"] = pd.to_datetime(webquotesDf["submission_date"], format="%m/%d/%Y", errors="coerce").dt.date
    webquotesDf["submission_on_time"] = pd.to_datetime(webquotesDf["submission_on_time"], format="%I:%M %p", errors="coerce").dt.time
    webquotesDf["date_sold"] = pd.to_datetime(webquotesDf["date_sold"], format="mixed", errors="coerce")
    renamedWebquotesDf = renameColumns(webquotesDf, wqNewNewColumnNames)

    return renamedWebquotesDf

def renameJsonForWebquotesModel(json):
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
        data = json[column[0]]
        json.pop(column[0])
        json.update({column[1]: data})
    
    return json