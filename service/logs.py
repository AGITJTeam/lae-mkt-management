from service.columnsTransformations import logsNewNewColumnNames
from service.helpers import renameColumns
from data.models.logs_model import LogsModel
import pandas as pd

def generateLogsDf(logsInList: pd.Series) -> pd.DataFrame:
    logsList = []

    if logsInList.empty:
        return
    
    for logs in logsInList:
        for log in logs:
            if not log:
                continue

            logModel = LogsModel(**log)
            logsList.append(logModel)
    
    logsDf = pd.DataFrame(logsList)
    logsDf["dateLog"] = pd.to_datetime(logsDf["dateLog"])
    renamedLogsDf = renameColumns(logsDf, logsNewNewColumnNames)

    return renamedLogsDf
