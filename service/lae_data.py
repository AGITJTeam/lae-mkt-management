from service.columnsTransformations import laeDataColumnsNewOrder
from service.helpers import reorderColumns
import pandas as pd

def generateLaeData(receipts: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """ Create Lae DataFrame with Receipts and Customers DataFrames.

    Parameters
        - receipts {pandas.DataFrame} main DataFrame to merge with Customers.
        - customers {DataFrame} secundary DataFrame to merge and get
        additional data.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    laeData = receipts.merge(customers, how="left", on="customer_id")
    data = reorderColumns(laeData, laeDataColumnsNewOrder)

    return data

def generateLaeDataDf(start: str, end: str) -> pd.DataFrame:
    """ Generate a DataFrame with LAE data for a given date range.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.

    Returns
        {pd.DataFrame} a DataFrame containing the processed LAE data 
            with formatted columns and structure.
    """

    laeData = []
    laeJson = getLaeData(start, end)

    if not laeJson:
        raise Exception("No Lae data yet...")
    
    for lae in laeJson:
        for_value = lae["for"]
        lae.pop("for")
        lae.update({"for_t": for_value})
        laeModel = LaeModel(**lae)
        laeData.append(laeModel)
    
    laeDf = pd.DataFrame(laeData)
    laeDf["date"] = pd.to_datetime(laeDf["date"])
    laeDf.rename(columns=LAE_COLUMN_NAMES, inplace=True)
    laeDf = laeDf.loc[:, LAE_ORDER]

    return laeDf

def generateLaeOnePhoneDf(start: str, end: str) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data indexed by one phone.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "phone fix" 
            containing aggregated LAE data.
    """

    laeDf = generateLaeDataDf(start, end)
    laeOnePhoneDf = pd.pivot_table(
        data=laeDf,
        values=OPPT_COLUMNS,
        index=["phone fix"],
        aggfunc="sum",
        dropna=False
    )
    laeOnePhoneDf = laeOnePhoneDf.loc[:, OPPT_COLUMNS]

    return laeOnePhoneDf

def generateLaeTwoPhoneDf(start: str, end: str) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data filtered by two phones.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "cell phone"
            and "phone" for aggregated LAE data with two phones.
    """

    laeDf = generateLaeDataDf(start, end)
    laeFiltered = laeDf[laeDf["phone fix"].isin(["2 Phones"])]
    laeTwoPhonePv = pd.pivot_table(
        data=laeFiltered,
        values=OPPT_COLUMNS,
        index=["cell phone", "phone"],
        aggfunc="sum"
    )
    laeTwoPhonePv = laeTwoPhonePv.loc[:, OPPT_COLUMNS]

    return laeTwoPhonePv
