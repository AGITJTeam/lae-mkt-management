from service.columnsTransformations import rColumnsToDelete, rNewColumnNames
from service.helpers import deleteColumns, renameColumns
from data.models.receipts_model import ReceiptModel
from controllers.controller import getReceipt
import pandas as pd
import time

def generateReceiptsDf(receiptPayroll: pd.DataFrame) -> pd.DataFrame:
    """ Create Receipts DataFrame with with API response.

    Parameters
        - receiptsPayroll {pandas.DataFrame} DataFrame from which the
        ids will be obtained.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    receipts = getReceiptById(receiptPayroll)
    receiptsDf = pd.DataFrame(receipts)
    receiptsDf["date"] = pd.to_datetime(receiptsDf["date"])
    receiptsDf["dateCreated"] = pd.to_datetime(receiptsDf["dateCreated"])
    receiptsDf["lastUpdated"] = pd.to_datetime(receiptsDf["lastUpdated"])
    renamedReceiptsDf = renameColumns(receiptsDf, rNewColumnNames)
    finalReceiptsDf = deleteColumns(renamedReceiptsDf, rColumnsToDelete)
    
    return finalReceiptsDf

def getReceiptById(receiptPayroll: pd.DataFrame) -> list[ReceiptModel]:
    """ Create a list of Receipts models with the API Calls.
    
    Parameters
        - receiptsPayroll {pandas.DataFrame} DataFrame from which the
        ids will be obtained.

    Returns
        {list[ReceiptModel]} list from which a DataFrame will be created.
    """


    ids = receiptPayroll["id_receipt_hdr"].tolist()
    receipts = []

    for id in ids:
        receipt = getReceipt(id)

        if not receipt:
            continue

        receiptModel = ReceiptModel(**receipt)
        receipts.append(receiptModel)

        time.sleep(1.0)
    
    return receipts
