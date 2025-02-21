from data.repository.calls.main_data_repo import MainData

def countDialpadCallsByDateRange(start: str, end: str) -> tuple[list[dict], list[dict]]:
    """ Retrieve the count of all and unique Dialpad calls per region
    and office from the database.
    
    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.

    Returns
        {tuple[list[dict], list[dict]]} the data that will be shown.
    """

    mainData = MainData()

    allDialpadCalls = mainData.getAllDialpadCalls(start, end)
    uniqueDialpadCalls = mainData.getUniqueDialpadCalls(start, end)


    return allDialpadCalls, uniqueDialpadCalls
