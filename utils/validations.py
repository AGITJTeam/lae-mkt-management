from redisCli import redisCli
from datetime import datetime, timedelta
from werkzeug.datastructures import FileStorage
import json, re

def valPreMadeRedisData(start: str, end: str, redisKey: str, validators: dict) -> dict | None:
    """ Checks if date range corresponds to pre-made Redis keys.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.
        - newKeyName {str} Redis key if pre-made keys do not exist.
        - validators {dict} dictionary with the Redis keys and validator
        functions.
    """

    # 1) Checks Redis connection.
    if not redisCli:
        return None

    for key, validatorFunc in validators.items():
        # 2) Checks if date range correspond to pre-made Redis keys.
        if validatorFunc(start, end):
            # 3) Returns data if Redis key exists.
            cachedData = redisCli.get(key)
            if cachedData:
                return json.loads(cachedData)

    # 4) Checks if particular Redis key exists.
    cachedData = redisCli.get(redisKey)

    # 5) Returns data if Redis key exists.
    return json.loads(cachedData) if cachedData else None

def valPreMadeHashData(start: str, end: str, redisKey: str, validators: dict, hashKeys: list) -> dict | None:
    """ Checks if date range corresponds to pre-made Redis keys of Hash type.

    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.
        - newKeyName {str} Redis key if pre-made keys do not exist.
        - validators {dict} dictionary with the Redis keys and validator
        functions.
        - hashKeys {list} list with Hash keys.
    """

    # 1) Checks Redis connection.
    if not redisCli:
        return None

    for key, validatorFunc in validators.items():
        # 2) Checks if date range correspond to pre-made Redis keys.
        if validatorFunc(start, end):
            # 3) Returns data if Redis key exists.
            cachedData = redisCli.hgetall(key)
            if cachedData:
                return {k: json.loads(cachedData.get(k, "{}")) for k in hashKeys}

    # 4) Checks if particular Redis key exists.
    cachedData = redisCli.hgetall(redisKey)

    # 5) Returns data if Redis key exists.
    return {k: json.loads(cachedData.get(k, "{}")) for k in hashKeys} if cachedData else None

def valDateRanges(start: str, end: str) -> bool:
    """ Checks if strings dates are valid dates, dates are not in the
    future and start date is before end date.
    
    Parameters
        - start {str} the beginning of the range.
        - end {str} the end of the range.

    Returns
        {bool} True if the dates are valid, False otherwise.
    """

    if not validateStringDate(start) or not validateStringDate(end):
        return False

    today = datetime.today().date()
    startDate = datetime.strptime(start, "%Y-%m-%d").date()
    endDate = datetime.strptime(end, "%Y-%m-%d").date()

    if startDate > endDate:
        return False

    if startDate > today or endDate > today:
        return False

    return True

def validateStringDate(strDate: str) -> bool:
    """ Checks if a string is a valid date.

    Parameters
        - strDate {str} the string to check.

    Returns
        {bool} True if the string is a valid date, False otherwise.
    """

    PATTERN = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$"

    if not re.fullmatch(PATTERN, strDate):
        return False
    try:
        datetime.strptime(strDate, "%Y-%m-%d").date()
        return True
    except ValueError:
        return False

def validateStringNumber(number: str) -> bool:
    """ Checks if a string is a number.

    Parameters
        - number {str} the number to check.

    Returns
        {bool} True if the number is an integer, False otherwise.
    """

    if not isinstance(number, str) or not number.isdigit(): 
        return False

    return True

def validateEmail(email: str) -> bool:
    """ Checks if a string is a valid email.

    Parameters
        - email {str} the email to check.

    Returns
        {bool} True if the email is valid, False otherwise.
    """

    PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9]+(?:\.[a-zA-Z0-9]+)+$"

    if not re.fullmatch(PATTERN, email):
        return False

    return True

def validateFileStorage(file: FileStorage) -> bool:
    """ Checks if a file is valid.

    Parameters
        - file {werkzeug.datastructures.FileStorage} the file to check.

    Returns
        {bool} True if the file is valid, False otherwise.
    """

    if not isinstance(file, FileStorage):
        return False

    return True

def validateTxtFile(file: FileStorage) -> bool:
    """ Checks if a file is a valid txt file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} the file to check.

    Returns
        {bool} True if the file is a valid txt file, False otherwise.
    """

    if not validateFileStorage(file) or not file.filename.endswith(".txt"):
        return False

    return True

def validateXlsFile(file: FileStorage) -> bool:
    """ Checks if a file is a valid xls file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} the file to check.

    Returns
        {bool} True if the file is a valid xls file, False otherwise.
    """

    if not validateFileStorage(file) or not file.filename.endswith(".xls"):
        return False

    return True

def validateXlsxFile(file: FileStorage) -> bool:
    """ Checks if a file is a valid xlsx file.

    Parameters
        - file {werkzeug.datastructures.FileStorage} the file to check.

    Returns
        {bool} True if the file is a valid xlsx file, False otherwise.
    """

    if not validateFileStorage(file) or not file.filename.endswith(".xlsx"):
        return False

    return True

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

    # Create auxiliar datetime variables.
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)

    # Create only date strings from datetime variables.
    firstDayCurrentMonthStr = firstDayCurrentMonth.isoformat()
    strToday = today.isoformat()

    if start != firstDayCurrentMonthStr or end != strToday:
        return False

    return True

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

    # Create auxiliar datetime variables.
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    lastDayPreviousMonth = (firstDayCurrentMonth - timedelta(days=1))
    firstDayPreviousMonth = lastDayPreviousMonth.replace(day=1)

    # Create only date strings from datetime variables.
    firstDayPreviousMonthStr = firstDayPreviousMonth.isoformat()
    lastDayPreviousMonthStr = lastDayPreviousMonth.isoformat()

    if start != firstDayPreviousMonthStr or end != lastDayPreviousMonthStr:
        return False

    return True

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

    # Create auxiliar datetime variables.
    today = datetime.today().date()
    firstDayCurrentMonth = today.replace(day=1)
    lastDayPreviousMonth = (firstDayCurrentMonth - timedelta(days=1))
    firstDayPreviousMonth = lastDayPreviousMonth.replace(day=1)

    # Create only date strings from datetime variables.
    firstDayPreviousMonthStr = firstDayPreviousMonth.isoformat()
    strToday = today.isoformat()

    if start != firstDayPreviousMonthStr or end != strToday:
        return False

    return True

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

    # Create only date strings.
    firstDayLastYear = "2024-01-01"
    strToday = datetime.today().date().isoformat()

    if start != firstDayLastYear or end != strToday: 
        return False

    return True
