from service.columnsTransformations import laeDataColumnsNewOrder
from service.helpers import reorderColumns
import pandas as pd

""" Create Lae DataFrame with Receipts and Customers DataFrames.

Parameters
    - receipts {DataFrame} main DataFrame to merge with Customers.
    - customers {DataFrame} secundary DataFrame to merge and get
      additional data.

Returns
    {DataFrame} resulting DataFrame.

"""
def generateLaeData(receipts: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    laeData = receipts.merge(customers, how="left", on="customer_id")
    data = reorderColumns(laeData, laeDataColumnsNewOrder)

    return data
