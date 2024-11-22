from calendar import monthrange
import os

""" Generate start and end date for Receipts Payroll API call.

Returns
    {tuple[str, str]} tuple with start and end dates.

"""
def getDates() -> tuple[str, str]:
    start = getStartDateFromFile()

    if len(start) == 0:
        raise ValueError("controllers.get_dates.getDates(). Start date from \"start_date.txt\" is empty.")

    end = getEndDate(start)

    print("Rango de fechas para llamada a la API.")
    print(f"Inicio: {start}")
    print(f"Fin: {end}\n")

    updateStartDateFile(end)
    
    return (start, end)

""" Read "start_dates.txt" and get the single line in the file.

Returns
    {str} the date in the file.

"""
def getStartDateFromFile() -> str:
    path = os.path.dirname(os.path.abspath(__file__))
    datePath = os.path.join(path, "start_date.txt")
    start = ""

    with open(datePath, "r") as dateFile:
        start = dateFile.readline()

        if len(start) == 0:
            start = "1-01-2024"
    
    return start

""" Generates end date by evaluating the start date.

Returns
    {str} the end date.

"""
def getEndDate(start) -> str:
    startSplitted = start.split("-")

    return evaluateDate(startSplitted, 6)

""" Evaluates start date to generate the end date.

Parameters
    dateInList {list[str]} the start date in a list.

Returns
    {str} the end date.

"""
def evaluateDate(dateInList: list[str], daysToAdd: int) -> str:
    month = int(dateInList[0])
    day = int(dateInList[1])
    year = int(dateInList[2])
    end = ""

    if isEndDayInNextMonth(year, month, day, daysToAdd):
        if isEndMonthInNextYear(month):
            end = endDateInDifferentYears(year, month, day)
        else:
            end = endDateInDifferentMonths(year, month, day)
    else:
        end = endDateInSameMonth(year, month, day, daysToAdd)
    
    return end

""" Check if the start day + daysToAdd still in the start month.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.
    - daysToAdd {int} days to add to day parameter.

Returns
    {bool} result of the condition.

"""
def isEndDayInNextMonth(year: int, month: int, day: int, daysToAdd: int) -> bool:
    lastDayMonth = monthrange(year, month)[1]
    
    return day + daysToAdd > lastDayMonth

""" Check if start month + 1 still in the start year.

Parameters
    - month {int} start month.

Returns
    {bool} result of the condition.

"""
def isEndMonthInNextYear(month: int) -> bool:
    return month + 1 == 13

""" Build the end date for the next year.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.

Returns
    {str} the end date.

"""
def endDateInDifferentYears(year: int, month: int, day: int) -> str:
    year += 1
    month = 1
    newDay = endDayInNextMonth(year, month, day)

    return dateFormatter(year, month, newDay)

""" Build the end date for the next month.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.

Returns
    {str} the end date.

"""
def endDateInDifferentMonths(year: int, month: int, day: int) -> str:
    newDay = endDayInNextMonth(year, month, day)
    month += 1

    return dateFormatter(year, month, newDay)

""" Build the end date for the same month.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.

Returns
    {str} the end date.

"""
def endDateInSameMonth(year: int, month: int, day: int, daysToAdd: int) -> str:
    day += daysToAdd

    return dateFormatter(year, month, day)

""" Determine if the end day is in the next month.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.

Returns
    {int} the end day.

"""
def endDayInNextMonth(year: int, month: int, day: int) -> str:
    monthDays = monthrange(year, month)[1]
    monthDaysDiff = monthDays - day
    endDay = 6 - monthDaysDiff

    return endDay

""" Adds 1 day to end date and write it on 'start_date.txt'

Parameters
    start {str} the date to add 1 day.

"""
def updateStartDateFile(start: str) -> None:
    splittedStart = start.split("-")
    newStart = evaluateDate(splittedStart, 1)
    setStartDateInFile(newStart)

""" Writes new date on 'start_date.txt'

Parameters
    date {str} the date to write.

"""
def setStartDateInFile(date: str) -> None:
    path = os.path.dirname(os.path.abspath(__file__))
    datePath = os.path.join(path, "start_date.txt")

    with open(datePath, "w") as dateFile:
        dateFile.write(date)

""" Format end date for the "endDate" parameter in the Receipts API.

Parameters
    - year {int} start year.
    - month {int} start month.
    - day {int} start day.

Returns
    {str} end date in m-dd-yyyy format.

"""
def dateFormatter(year: int, month: int, day: int) -> str:
    if len(str(day)) == 1:
        return str(f"{month}-0{day}-{year}")
    
    return str(f"{month}-{day}-{year}")

start, end = getDates()