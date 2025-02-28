import unicodedata

def normalizeStr(value=None):
    """ Normalize a string by removing accents and non-ASCII characters.

    Parameters
        - value {str | None} The string to be normalized. If None, the 
        function returns an empty string.

    Returns
        {str} The normalized string with accents and special characters 
            removed.
    """

    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")

def goalGi(ot: int | float) -> int | float:
    """ Calculate the GI goal based on overtime worked.

    Parameters
        - ot {int | float} The overtime hours worked.

    Returns
        {int | float} The total GI goal based on the overtime.
    """

    INITIAL_AMOUNT = 2750
    
    if ot <= 0.25:
        return INITIAL_AMOUNT
    
    additionalTime = ot - 0.25
    additionalMinutes = additionalTime * 60
    additionalAmount = (50/3) * additionalMinutes
    totalAmountNeeded = INITIAL_AMOUNT + additionalAmount

    return totalAmountNeeded

def doneOt(gi: int | float) -> int | float:
    """ Calculate the overtime completed based on the GI achieved.

    Parameters
       - gi {int | float} The GI achieved by the user.

    Returns
        {int | float} The overtime completed in hours.
    """

    INITIAL_AMOUNT = 2750
    
    if gi < INITIAL_AMOUNT:
        return 0
    elif gi == INITIAL_AMOUNT:
        return 0.25
    
    additionalAmount = gi - INITIAL_AMOUNT
    additionalMinutes = additionalAmount / (50 / 3)
    additionalTimeHours = additionalMinutes / 60
    totalSpentTime = 0.25 + additionalTimeHours
    
    return totalSpentTime

def calculateLeftGi(ot: int | float, gi: int | float, position: str) -> int | float:
    """ Calculate the remaining GI required based on OT and position.

    Parameters
        - ot {int | float} The overtime hours worked.
        - gi {int | float} The current GI achieved.
        - position {str} The position of the employee.

    Returns
        {int | float} The GI left to meet the required goal, or 0 if no 
            additional GI is needed.
    """

    neededGi = goalGi(ot)

    if position == "Floor Assistant":
        return 0
    if ot == 0:
        return 0
    elif ot <= 0.25 and gi >= 2750:
        return 0
    elif ot <= 0.25 and gi < 2750:
        return neededGi - gi
    elif ot > 0.25 and gi >= neededGi:
        return 0
    elif ot > 0.25 and gi < neededGi:
        return neededGi - gi

def calculateIfUnauthorizedOt(ot: int | float, gi: int | float, position: str) -> str:
    """ Determine if OT is unauthorized based on OT, GI, and position.

    Parameters
        - ot {int} The overtime hours worked.
        - gi {int} The current GI achieved.
        - position {str} The position of the employee.

    Returns
        {str} "Yes" if the OT is unauthorized, otherwise "No".
    """

    neededGi = goalGi(ot)

    if position == "Floor Assistant":
        if ot <= 2:
            return "No"
        else:
            return "Yes"
    if ot == 0:
        return "No"
    elif ot <= 0.25 and gi >= 2750:
        return "No"
    elif ot <= 0.25 and gi < 2750:
        return "Yes"
    elif ot > 0.25 and gi >= neededGi:
        return "No"
    elif ot > 0.25 and gi < neededGi:
        return "Yes"

def calculateIfUnauthorizedOtRes(ot: int | float, gi: int | float, position: str) -> str:
    """Provide a detailed description of unauthorized OT status.

    Parameters
        - ot {int | float} The overtime hours worked.
        - gi {int | float} The current GI achieved.
        - position {str} The position of the employee.

    Returns
        {str} A description of the unauthorized OT status.
    """

    neededGi = goalGi(ot)

    if position == "Floor Assistant":
        if ot == 0:
            return "Zero OT"
        elif ot <= 2:
            return "Less than 2 Hrs"
        else:
            return "Over 2 Hrs"
    if ot == 0:
        return "Zero OT"
    elif ot <= 0.25 and gi >= 2750:
        return "Over $2750 & under 15 mins"
    elif ot <= 0.25 and gi < 2750:
        return "Under $2750 GI & under 15 mins"
    elif ot > 0.25 and gi >= neededGi:
        return "Justified GI"
    elif ot > 0.25 and gi < neededGi:
        return "Unjustified GI"

def calculateAuthorizedOt(ot: int | float, gi: int | float, position: str) -> int | float:
    """ Calculate the authorized OT based on GI, OT, and position.

    Parameters
        - ot {int | float}    The overtime hours worked.
        - gi {int | float}    The current GI achieved.
        - position {str}      The position of the employee.

    Returns
        {int | float}    The authorized OT in hours based on the inputs.
    """
    
    neededGi = goalGi(ot)
    completed = doneOt(gi)

    if position == "Floor Assistant":
        if ot > 2:
            return 2
        else:
            return ot
    if ot == 0:
        return 0
    elif ot <= 0.25 and gi >= 2750:
        return ot
    elif ot <= 0.25 and gi < 2750:
        return completed
    elif ot > 0.25 and gi >= neededGi:
        return ot
    elif ot > 0.25 and gi < neededGi:
        return completed

def calculateUnauthorizedOt(ot: int | float, gi: int | float, position: str) -> int | float:
    """ Calculate the unauthorized OT based on GI, OT, and position.

    Parameters
        - ot {int | float}    The overtime hours worked.
        - gi {int | float}    The current GI achieved.
        - position {str}      The position of the employee.

    Returns
        {int | float}    The unauthorized OT in hours based on the inputs.
    """

    neededGi = goalGi(ot)
    completed = doneOt(gi)

    if position == "Floor Assistant":
        if ot > 2:
            return ot - 2
        else:
            return 0
    if ot == 0:
        return 0
    elif ot <= 0.25 and gi >= 2750:
        return 0
    elif ot <= 0.25 and gi < 2750:
        return ot - completed
    elif ot > 0.25 and gi >= neededGi :
        return 0
    elif ot > 0.25 and gi < neededGi:
        return ot - completed
