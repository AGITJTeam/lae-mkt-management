from data.repository.interfaces.i_receipts import IReceipts
from data.repository.calls.helpers import getData, executeOperation

class Receipts(IReceipts):
    """
    Handles every petition of Receipts table from database.

    Methods
        - getLastRecord.
        - getBetweenDates.
        - deleteByIds.
    """

    def getLastRecord(self):
        """ {list[dict]} get receitps by their id given a date range. """

        query = "SELECT date FROM receipts ORDER BY date DESC LIMIT 1;"

        return getData(query=query, filename="flask_api.ini")

    def getBetweenDates(self, start, end):
        """ {list[dict]} get receipts in a range of dates.
        
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT id_receipt_hdr, date, customer_id, id_employee_usr, id_employee_csr1, total_amnt_receipt, amount_paid, fiduciary, non_fiduciary, office FROM receipts WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query=query, filename="flask_api.ini")

    def deleteByIds(self, ids):
        """ Delete receipts by its ids.

        Parameters
            ids {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM receipts WHERE id_receipt_hdr IN ({values})"

        return executeOperation(query=query, filename="flask_api.ini")
