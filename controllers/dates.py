from datetime import date, timedelta
import os

def getDates() -> tuple[str, str]:
    """ Generate start and end date for making an API call and updates
    start_date.txt file with new start date.

    Returns
        {tuple[str, str]} tuple with start and end dates.
    """

    start = getStartDateFromFile()
    startDate = date.fromisoformat(start)
    endDate = addDaysToDate(startDate, 3)
    end = endDate.isoformat()

    print("Date range for calling API.")
    print(f"Start: {start}")
    print(f"End: {end}")

    setNewStartDate(end)
    
    return (start, end)

def getStartDateFromFile() -> str:
    """ Read "start_dates.txt" and get the single line in the file.

    Returns
        {str} the date in the file.
    """

    path = os.path.dirname(os.path.abspath(__file__))
    datePath = os.path.join(path, "start_date.txt")
    start = ""

    with open(datePath, "r") as dateFile:
        start = dateFile.readline()

        if len(start) == 0:
            raise ValueError("controllers.get_dates.getDates(). Start date from \"start_date.txt\" is empty.")
    
    return start

def addDaysToDate(date: date, days: int) -> date:
    """ Add days to a date.

    Parameters
        - date {date} the date to add days to.
        - days {int} number of days to add.

    Returns
        {date} date with added days.
    """

    sevenDays = timedelta(days=days)
    return date + sevenDays

def setNewStartDate(start: str) -> None:
    """ Adds 1 day to end date and write it on 'start_date.txt'

    Parameters
        - start {str} the date to add 1 day.
    """

    startDate = date.fromisoformat(start)
    newStart = addDaysToDate(startDate, 1)
    startStr = newStart.isoformat()
    print(f"New date in /controllers/start_date.py: {startStr}")

    updateStartDateFile(startStr)

def updateStartDateFile(date: str) -> None:
    """ Writes new date on 'start_date.txt'

    Parameters
        - date {str} the date to write.
    """

    path = os.path.dirname(os.path.abspath(__file__))
    datePath = os.path.join(path, "start_date.txt")

    with open(datePath, "w") as dateFile:
        dateFile.write(date)
