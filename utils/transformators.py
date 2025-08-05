import pandas as pd

replacements = [
    ("en", "English"),
    ("english", "English"),
    ("inglés", "English"),
    ("ingles", "English"),
    ("es", "Spanish"),
    ("spanish", "Spanish"),
    ("español", "Spanish"),
    ("espanol", "Spanish"),
]

def formatWebquotesLanguage(wq: list[dict]) -> list[dict]:
    """ Formats "preference" column of Webquotes DataFrame.

    Parameters
        - wq {list[dict]} Webquotes data before formatting.

    Returns
        {list[dict]} Webquotes data after formatting.
    """

    df = pd.DataFrame(wq)
    df["preference"] = df["preference"].str.lower()
    replacementDict = dict(replacements)
    df["preference"] = df["preference"].replace(replacementDict)
    return df.to_dict(orient="records")

def formatWebquotesDateSold(wq: list[dict]) -> list[dict]:
    """ Formats "date_sold" column of Webquotes DataFrame.

    Parameters
        - wq {list[dict]} Webquotes data before formatting.

    Returns
        {list[dict]} Webquotes data after formatting.
    """

    df = pd.DataFrame(wq)
    df['date_sold'] = df['date_sold'].apply(lambda x: "" if pd.isna(x) else str(x)) 
    df["submission_on_time"] = df["submission_on_time"].apply(lambda x: x.strftime("%H:%M:%S") if pd.notnull(x) else "")

    return df.to_dict(orient="records")
