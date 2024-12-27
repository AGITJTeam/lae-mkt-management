from data.repository.interfaces.i_receipts import IReceipts
from data.repository.calls.helpers import getData, executeOperation

class Receipts(IReceipts):
    """
    Handles every petition of Receipts table from database.

    Methods
        - getByIds {list[dict]} get receitps by their id given a date
          range.
        - deleteByIds delete receipts by its ids.
    """

    def getLastRecord(self):
        query = "SELECT date FROM receipts ORDER BY date DESC LIMIT 1;"

        return getData(query)

    def getBetweenDates(self, start: str, end: str):
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT id_receipt_hdr, date, customer_id, id_employee_usr, id_employee_csr1, total_amnt_receipt, amount_paid, fiduciary, non_fiduciary, office FROM receipts WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query)

    def deleteByIds(self, ids: list):
        """
        Parameters
            ids {list} list of customers ids.
        """
        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM receipts WHERE id_receipt_hdr IN ({values})"

        return executeOperation(query)
