from data.repository.interfaces.i_lae_data import ILaeData
from data.repository.calls.helpers import getData

class LaeData(ILaeData):
    """
    Handles every GET petition of lae_data table from database.

    Methods
        - getAllData {list[dict]} get all receipts from database.
        - getDataBetweenDates {list[dict]} get receipts in a range of
          dates.
        - getReceiptsByCustId {list[dict]} get receipt by customer id.
    """

    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM lae_data ORDER BY date ASC;"

        return getData(query)
    
    def getDataBetweenDates(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT * FROM lae_data WHERE date BETWEEN \'{start}\' AND \'{end}\';"

        return getData(query)

    def getReceiptsByCustId(self, id: int) -> list[dict]:
        """
        Parameters
            id {int} the id of the customer.
        """
        query = f"SELECT * FROM lae_data WHERE customer_id = {id}"

        return getData(query)
