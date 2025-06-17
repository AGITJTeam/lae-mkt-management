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
