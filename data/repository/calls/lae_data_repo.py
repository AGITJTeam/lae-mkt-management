from data.repository.interfaces.i_lae_data import ILaeData
from data.repository.calls.helpers import getData, executeOperation

class LaeData(ILaeData):
    """
    Handles every petition of lae_data table from database.

    Methods
        - getAllData.
        - getBetweenDates.
        - getByCustomerId.
        - getLastRecord.
        - deleteLastMonthData.
    """

    def getAllData(self):
        """ {list[dict]} get all receipts from database. """

        query = "SELECT * FROM lae_data ORDER BY date ASC;"

        return getData(query)
    
    def getBetweenDates(self, start, end):
        """ {list[dict]} get receipts in a range of dates.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT * FROM lae_data WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query)

    def getByCustomerId(self, id):
        """ {list[dict]} get receipt by customer id.

        Parameters
            - id {int} the id of the customer.
        """

        query = f"SELECT * FROM lae_data WHERE customer_id = {id}"

        return getData(query)
    
    def getLastRecord(self):
        """ {list[dict]} get the last date from 'date' column. """
        
        query = "SELECT date FROM lae_data ORDER BY date DESC LIMIT 1;"

        return getData(query)
    
    def deleteLastMonthData(self, start, end):
        """ execute DELETE operation that erase rows between a date
        range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"DELETE FROM lae_data WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        executeOperation(query)
