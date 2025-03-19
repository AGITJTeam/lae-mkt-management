from datetime import datetime
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
