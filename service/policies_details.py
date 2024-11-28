from service.columnsTransformations import polNewColumnNames, polColumnsToDelete
from service.helpers import getCustomersIds, renameColumns, deleteColumns
from data.models.policies__details_model import PoliciesDetailsModel
from controllers.controller import getPoliciesDetails
import pandas as pd

""" Create Policies DataFrame with API response and transformations.

Parameters
    receiptsDf {DataFrame} from which the ids will be obtained.

Returns
    {DataFrame} resulting DataFrame.

"""
def generatePoliciesDf(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    customersIds = getCustomersIds(receiptsDf)
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
    {list[Response]} list of customers.

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
    policiesDf {DataFame} the DF from which the columns will be
    deleted.

Returns
    {DataFrame} the DF with less columns.

"""
def deleteColumnWithListValues(policiesDf: pd.DataFrame) -> pd.DataFrame:
    columnsToDelete = ["vehicle_insureds", "policies_dtl", "logs", "locations"]

    return deleteColumns(policiesDf, columnsToDelete)

""" Parse 'effectiveDate', 'expirationDate', 'dateCreated' and
    'lastUpdated' from string to sql datetime.

Parameters
    policiesDf {DataFame} the DF from which the columns will be parsed.

Returns
    {DataFrame} the DF with parsed columns.

"""
def parseDateColumns(policiesDf: pd.DataFrame) -> pd.DataFrame:
    policiesDf["effectiveDate"] = pd.to_datetime(policiesDf["effectiveDate"])
    policiesDf["expirationDate"] = pd.to_datetime(policiesDf["expirationDate"])
    policiesDf["dateCreated"] = pd.to_datetime(policiesDf["dateCreated"])
    policiesDf["lastUpdated"] = pd.to_datetime(policiesDf["lastUpdated"])

    return policiesDf
