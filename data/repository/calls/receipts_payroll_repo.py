from data.repository.interfaces.i_receipts_payroll import IReceiptsPayroll
from data.repository.calls.helpers import getData

class ReceiptsPayroll(IReceiptsPayroll):
    """
    Handles every GET petition of receipts_payroll table from database.

    Methods
        - getAllData {list[dict]} get all receipts from database.
        - getDataBetweenDates {list[dict]} get receipts in a range of
          dates.
        - getReceiptsByCustId {list[dict]} get receipt by customer id.
    """

    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM receipts_payroll ORDER BY date ASC;"

        return getData(query)
    
    def getDataBetweenDates(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT * FROM receipts_payroll WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query)

    def getReceiptsByCustId(self, id: int):
        """
        Parameters
            id {int} the id of the customer.
        """
        query = f"SELECT * FROM receipts_payroll WHERE customer_id = {id};"

        return getData(query)
