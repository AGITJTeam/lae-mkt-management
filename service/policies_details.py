from service.columnsTransformations import polNewColumnNames, polColumnsToDelete
from service.helpers import renameColumns, deleteColumns
from data.models.policies__details_model import PoliciesDetailsModel
from controllers.controller import getPoliciesDetails
import pandas as pd

""" Create Policies DataFrame with API response and transformations.

Parameters
    receiptsDf {pandas.DataFrame} from which the ids will be obtained.

Returns
    {pandas.DataFrame} resulting DataFrame.

"""
def generatePoliciesDf(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    customersIds = receiptsDf["customer_id"].tolist()
    policies = getPoliciessWithReceiptId(customersIds)
    policiesDf = pd.DataFrame(policies)
    parsedPoliciesDf = parseDateColumns(policiesDf)
    renamedPoliciesDf = renameColumns(parsedPoliciesDf, polNewColumnNames)
    newPoliciesDf = deleteColumns(renamedPoliciesDf, polColumnsToDelete)
    
    return newPoliciesDf

""" Iterate and save all policies in a list.

Parameters
    ids {dict.values} ids to iterate.

Returns
    {list[requests.Response]} list of customers.

"""
def getPoliciessWithReceiptId(ids: dict.values) -> list:
    policies = []

    for id in ids:
        policyInList = getPoliciesDetails(id)

        if not policyInList:
            continue
        
        for policy in policyInList:
            policyModel = PoliciesDetailsModel(**policy)
            policies.append(policyModel)
    
    return policies

""" Deletes Policies Df columns that contains lists.

Parameters
    policiesDf {pandas.DataFame} the DF from which the columns will be
    deleted.

Returns
    {pandas.DataFrame} the DF with less columns.

"""
def deleteColumnWithListValues(policiesDf: pd.DataFrame) -> pd.DataFrame:
    columnsToDelete = ["vehicle_insureds", "policies_dtl", "logs", "locations"]

    return deleteColumns(policiesDf, columnsToDelete)

""" Parse 'effectiveDate', 'expirationDate', 'dateCreated' and
    'lastUpdated' from string to sql datetime.

Parameters
    policiesDf {pandas.DataFame} the DF from which the columns will be parsed.

Returns
    {pandas.DataFrame} the DF with parsed columns.

"""
def parseDateColumns(policiesDf: pd.DataFrame) -> pd.DataFrame:
    policiesDf["effectiveDate"] = pd.to_datetime(policiesDf["effectiveDate"])
    policiesDf["expirationDate"] = pd.to_datetime(policiesDf["expirationDate"])
    policiesDf["dateCreated"] = pd.to_datetime(policiesDf["dateCreated"])
    policiesDf["lastUpdated"] = pd.to_datetime(policiesDf["lastUpdated"])

    return policiesDf
