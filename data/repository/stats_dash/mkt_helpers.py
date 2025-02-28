import pandas as pd

from data.repository.calls.lae_data_repo import LaeData


def generateLaeOnePhoneDf(start: str, end: str, columns: list) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data indexed by one phone.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.
        - columns {list} the columns to be shown in the pivot table.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "phone fix" 
        containing aggregated LAE data.
    """

    laeDf = generateLaeDataDf(start, end)

    laeOnePhoneDf = pd.pivot_table(
        data=laeDf,
        values=columns,
        index=["phone_fix"],
        aggfunc="sum",
        dropna=False
    )
    laeOnePhoneDf = laeOnePhoneDf.loc[:, columns]

    return laeOnePhoneDf

def generateLaeTwoPhoneDf(start: str, end: str, columns: list) -> pd.DataFrame:
    """ Generate a pivot table DataFrame for LAE data filtered by two phones.

    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.
        - columns {list} the columns to be shown in the pivot table.

    Returns
        {pd.DataFrame} a pivot table DataFrame indexed by "cell phone"
        and "phone" for aggregated LAE data with two phones.
    """

    laeDf = generateLaeDataDf(start, end)
    laeFiltered = laeDf[laeDf["phone_fix"].isin(["2 Phones"])]
    laeTwoPhonePv = pd.pivot_table(
        data=laeFiltered,
        values=columns,
        index=["cell_phone", "phone"],
        aggfunc="sum"
    )
    laeTwoPhonePv = laeTwoPhonePv.loc[:, columns]

    return laeTwoPhonePv


def generateLaeDataDf(start: str, end: str) -> pd.DataFrame:
    """ Gets the LAE data between the start and end dates.
    
    Parameters
        - start {str} the start date for retrieving LAE data.
        - end {str} the end date for retrieving LAE data.
    
    Returns
        {pandas.DataFrame} Lae Data in the given date range.
    """

    lae = LaeData()
    response = lae.getBetweenDates(start, end)

    return pd.DataFrame(response)

def searchNumberInLaeData(number: str, onePhonePv: pd.DataFrame, twoPhonePv: pd.DataFrame) -> list:
    """ Search number from GMB and Yelp calls file in LAE one phone or
    two phones DataFrames.
    
    Parameters
        - number {str} The number to search.
        - onePhonePv {pandas.DataFrame} Pivot table DataFrame indexed
        by "phone fix".
        - twoPhonePv {pandas.DataFrame} Pivot table DataFrame indexed
        by "cell phone" and "phone".
    
    Returns
        {list} List of values for the number.
    """

    if number in onePhonePv.index:
        # Founded in Lae Phone Fix column
        rows = onePhonePv.loc[number]
        if isinstance(rows, pd.DataFrame):
            return rows.sum(axis=0).tolist()

        return rows.to_list()

    if number in twoPhonePv.index.get_level_values("cell_phone"):
        # Found in Lae Cell Phone column with 2 Phones filter
        rows = twoPhonePv.loc[(number, slice(None))]
        if isinstance(rows, pd.DataFrame):
            return rows.sum(axis=0).tolist()
        
        return rows.values.flatten().tolist()

    if number in twoPhonePv.index.get_level_values("phone"):
        filtered = twoPhonePv[twoPhonePv.index.get_level_values("phone") == number]
        # Found in Lae Phone column with 2 Phones filter
        if isinstance(filtered, pd.DataFrame):
            return filtered.sum(axis=0).tolist()

        return filtered.values.flatten().tolist()

    return [None] * len(onePhonePv.columns)
