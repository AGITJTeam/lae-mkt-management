from service.columnsTransformations import polNewColumnNames, polColumnsToDelete
from service.helpers import getCustomersIds, renameColumns, deleteColumns
from data.models.policies__details_model import PoliciesDetailsModel
from controllers.controller import getPoliciesDetails
import pandas as pd

""" Create Customers DataFrame with API response and transformations.

Parameters
    receiptsDf {DataFrame} from which the ids will be obtained.

Returns
    {DataFrame} resulting DataFrame.

"""
def generatePoliciesDf(receiptsDf: pd.DataFrame) -> pd.DataFrame:
    customersIds = getCustomersIds(receiptsDf)
    policies = getPoliciessWithReceiptId(customersIds)
    policiesDf = pd.DataFrame(policies)
    renamedPoliciesDf = renameColumns(policiesDf, polNewColumnNames)
    newPoliciesDf = deleteColumns(renamedPoliciesDf, polColumnsToDelete)
    
    return newPoliciesDf

""" Iterate and save all customers in a list.

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

def deleteColumnWithListValues(policiesDf: pd.DataFrame) -> pd.DataFrame:
    columnsToDelete = ["vehicle_insureds", "policies_dtl", "logs", "locations"]
    return deleteColumns(policiesDf, columnsToDelete)