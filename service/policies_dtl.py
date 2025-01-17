from service.columnsTransformations import polDtlNewNewColumnNames
from service.helpers import renameColumns
from data.models.policies_dtl import PoliciesDtlModel
import pandas as pd

def generatePoliciesDtlDf(policiesDtlInList: pd.Series) -> pd.DataFrame:
    """ Create Policies DTL DataFrame with Policies DTL list.

    Parameters
        - policiesDtlInList {pandas.Series} policies dtls in pandas Series (works
        as python lists) to iterate with.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    
    policies = []

    if policiesDtlInList.empty:
        return

    for policiesDtl in policiesDtlInList:
        for policy in policiesDtl:
            if not policy:
                continue

            policieDtlModel = PoliciesDtlModel(**policy)
            policies.append(policieDtlModel)
    
    policiesDtlDf = pd.DataFrame(policies)
    renamedPoliciesDtlDf = renameColumns(policiesDtlDf, polDtlNewNewColumnNames)

    return renamedPoliciesDtlDf
