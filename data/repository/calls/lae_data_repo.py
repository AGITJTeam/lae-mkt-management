from data.repository.interfaces.i_lae_data import ILaeData
from data.repository.calls.helpers import getData, executeOperation

class LaeData(ILaeData):
    """
    Handles every petition of lae_data table from database.

    Methods
        - getBetweenDates.
        - deleteLastMonthData.
    """
    
    def getBetweenDates(self, start, end):
        """ {list[dict]} get receipts in a range of dates.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT * FROM lae_data WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query=query, filename="flask_api.ini")
    
    def deleteLastMonthData(self, start, end):
        """ execute DELETE operation that erase rows between a date
        range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"DELETE FROM lae_data WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return executeOperation(query=query, filename="flask_api.ini")
