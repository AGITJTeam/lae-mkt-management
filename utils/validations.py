from datetime import datetime, timedelta
import re

def validateStringDate(strDate: str) -> bool:
    """ Checks if a string is a valid date and if it's not in
    the future.

    Parameters
        - strDate {str} the string to check.

    Returns
        {bool} True if the string is a valid date, False otherwise.
    """
    
    pattern = r"^\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])$"

    if not re.fullmatch(pattern, strDate):
        return False
    try:
        today = datetime.now().date()
        date = datetime.strptime(strDate, "%Y-%m-%d").date()
        return date <= today
    except ValueError:
        return False

def validateNumber(number: str) -> bool:
    """ Checks if a string is a number.

    Parameters
        - number {str} the number to check.

    Returns
        {bool} True if the number is an integer, False otherwise.
    """

    if isinstance(number, str) and number.isdigit():
        return True

    return False

def valCurrentMonthDates(start: str, end: str) -> bool:
    """ Checks if string dates correspond to current month first day
    and today.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.

    Returns
        {bool} True if the dates correspond to current month, False
        otherwise.
    """

    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    firstDayCurrentMonthStr = firstDayCurrentMonth.isoformat()
    strToday = today.isoformat()

    if start == firstDayCurrentMonthStr and end == strToday:
        return True

    return False

def valPreviousMonthDates(start: str, end: str) -> bool:
    """ Checks if string dates correspond to previous month first and
    last day.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.

    Returns
        {bool} True if the dates correspond to previous month, False
        otherwise.
    """

    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    lastDayPreviousMonth = (firstDayCurrentMonth - timedelta(days=1))
    firstDayPreviousMonth = lastDayPreviousMonth.replace(day=1)

    firstDayPreviousMonthStr = firstDayPreviousMonth.isoformat()
    lastDayPreviousMonthStr = lastDayPreviousMonth.isoformat()

    if start == firstDayPreviousMonthStr and end == lastDayPreviousMonthStr:
        return True

    return False

def valTwoMonthsDates(start: str, end: str) -> bool:
    """ Checks if string dates correspond to previous month first day
    and today.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.

    Returns
        {bool} True if the dates correspond to previous and current
        month, False otherwise.
    """

    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    lastDayPreviousMonth = (firstDayCurrentMonth - timedelta(days=1))
    firstDayPreviousMonth = lastDayPreviousMonth.replace(day=1)
    
    firstDayPreviousMonthStr = firstDayPreviousMonth.isoformat()
    strToday = today.isoformat()

    if start == firstDayPreviousMonthStr and end == strToday:
        return True

    return False

def valLastToCurrentYearDates(start: str, end: str) -> bool:
    """ Checks if string dates correspond to first day of last year
    and today. This escenario only happens with Webquotes endpoint.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.

    Returns
        {bool} True if the dates correspond to first day of last year
        and today, False otherwise.
    """

    firstDayLastYear = "2024-01-01"
    strToday = datetime.today().date().isoformat()

    if start == firstDayLastYear and end == strToday:
        return True

    return False
