from data.repository.calls.helpers import generateTwoMonthsDateRange, postDataframeToDb
from data.repository.calls.webquotes_repo import Webquotes
from service.webquotes import generateWebquotesDf
from datetime import datetime

def updateWebquotesTables(start: str, end: str) -> None:
    """ Updates Webquotes table in db with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    webquotesDf = generateWebquotesDf(start, end)
    postDataframeToDb(webquotesDf, "webquotes", "append")

def addWebquotesTodayRecords() -> None:
    """ Generates today's date to add to Webquotes table. """

    today = datetime.today().date().isoformat()
    updateWebquotesTables(start=today, end=today)
    print(f"Webquotes data from {today} added...")

def updateWebquotesPreviousRecords() -> None:
    """ Generate last and current month date ranges to update
    Webquotes table. """

    webquotes = Webquotes()
    lastDate = webquotes.getLastRecord()[0]["submission_date"]
    dateRanges = generateTwoMonthsDateRange(lastDate)
    
    dataAvailable = any(
        not generateWebquotesDf(date["start"], date["end"]).empty
        for date in dateRanges
    )
    
    if not dataAvailable:
        print(f"No data from {dateRanges[0]['start']} to {dateRanges[0]['end']} to update.")
        raise Exception("No data found")

    firstDayLastMonth = dateRanges[0]["start"]
    yesterday = dateRanges[1]["end"]
    webquotes.deleteLastMonthData(firstDayLastMonth, yesterday)
    print(f"Webquotes data from {firstDayLastMonth} to {yesterday} deleted...")

    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in db with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")
