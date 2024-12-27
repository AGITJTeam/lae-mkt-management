from service.columnsTransformations import rColumnsToDelete, rNewColumnNames
from service.helpers import deleteColumns, renameColumns
from data.models.receipts_model import ReceiptModel
from controllers.controller import getReceipt
import pandas as pd

def generateReceiptsDf(receiptPayroll: pd.DataFrame) -> pd.DataFrame:
    receipts = getReceiptById(receiptPayroll)
    receiptsDf = pd.DataFrame(receipts)
    receiptsDf["date"] = pd.to_datetime(receiptsDf["date"])
    receiptsDf["dateCreated"] = pd.to_datetime(receiptsDf["dateCreated"])
    receiptsDf["lastUpdated"] = pd.to_datetime(receiptsDf["lastUpdated"])
    renamedReceiptsDf = renameColumns(receiptsDf, rNewColumnNames)
    finalReceiptsDf = deleteColumns(renamedReceiptsDf, rColumnsToDelete)
    
    return finalReceiptsDf

def getReceiptById(receiptPayroll: pd.DataFrame):
    ids = receiptPayroll["id_receipt_hdr"].tolist()
    receipts = []

    for id in ids:
        receipt = getReceipt(id)

        if not receipt:
            continue

        receiptModel = ReceiptModel(**receipt)
        receipts.append(receiptModel)
    
    return receipts
