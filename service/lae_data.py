from service.columnsTransformations import laeDataColumnsNewOrder
from service.helpers import reorderColumns
import pandas as pd

""" Create Lae DataFrame with Receipts and Customers DataFrames.

Returns
    {DataFrame} resulting DataFrame.

"""
def generateLaeData(receipts: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    laeData = receipts.merge(customers, how="left", on="customer_id")
    data = reorderColumns(laeData, laeDataColumnsNewOrder)

    return data