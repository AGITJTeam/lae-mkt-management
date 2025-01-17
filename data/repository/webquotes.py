from data.repository.calls.helpers import generateStartAndEndDates, generateTwoMonthsDateRange, postData
from data.repository.calls.webquotes_repo import Webquotes
from service.webquotes import generateWebquotesDf
from datetime import datetime

def updateWebquotesTables(start: str, end: str) -> None:
    """ Updates Webquotes table in vm with a date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    webquotesDf = generateWebquotesDf(start, end)
    postData(webquotesDf, "webquotes", "append")

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
    start, end = generateStartAndEndDates(lastDate)
    webquotes.deleteLastMonthData(start, end)
    print(f"Webquotes data from {start} to {end} deleted...")

    for date in dateRanges:
        updateWebquotesTables(date["start"], date["end"])
        print(f"Webquotes data from {date["start"]} to {date["end"]} updated...")

def addWebquotesSpecificDateRange(start: str, end: str) -> None:
    """ Add data to Webquotes table in vm with an specific date range.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    updateWebquotesTables(start, end)
    print(f"Webquotes data from {start} to {end} added...")
