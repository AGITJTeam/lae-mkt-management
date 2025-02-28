from service.columnsTransformations import laeDataColumnsNewOrder
from service.helpers import reorderColumns
import pandas as pd

def generateLaeData(receipts: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """ Create Lae DataFrame with Receipts and Customers DataFrames.

    Parameters
        - receipts {pandas.DataFrame} Receipts Payroll DataFrame.
        - customers {DataFrame} Customers DataFrame.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    laeData = receipts.merge(customers, how="left", on="customer_id")
    data = reorderColumns(laeData, laeDataColumnsNewOrder)

    return data
