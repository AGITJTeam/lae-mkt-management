from data.repository.interfaces.i_lae_data import ILaeData
from data.repository.calls.helpers import getData, executeOperation

class LaeData(ILaeData):
    """
    Handles every GET petition of lae_data table from database.

    Methods
        - getAllData {list[dict]} get all receipts from database.
        - getBetweenDates {list[dict]} get receipts in a range of
          dates.
        - getByCustomerId {list[dict]} get receipt by customer id.
        - getLastRecord {list[dict]} get the last date from 'date'
          column.
        - deleteLastMonthData execute DELETE operation that erase rows
          between a date range.
    """

    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM lae_data ORDER BY date ASC;"

        return getData(query)
    
    def getBetweenDates(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT * FROM lae_data WHERE date BETWEEN \'{start}\' AND \'{end}\';"

        return getData(query)

    def getByCustomerId(self, id: int) -> list[dict]:
        """
        Parameters
            id {int} the id of the customer.
        """
        query = f"SELECT * FROM lae_data WHERE customer_id = {id}"

        return getData(query)
    
    def getLastRecord(self) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = "SELECT date FROM lae_data ORDER BY date DESC LIMIT 1;"

        return getData(query)
    
    def deleteLastMonthData(self, start: str, end: str) -> None:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"DELETE FROM lae_data WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        executeOperation(query)
