from data.repository.interfaces.i_receipts_payroll import IReceiptsPayroll
from data.repository.calls.helpers import getData, executeOperation

class ReceiptsPayroll(IReceiptsPayroll):
    """
    Handles every petition of receipts_payroll table from database.

    Methods
        - getBetweenDates.
        - getByCustomerId.
        - getLastRecord.
        - deleteLastMonthData.
    """
    
    def getBetweenDates(self, start, end, *args):
        """ {list[dict]} get receipts in a range of dates.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
            - args {tuple} filters of the endpoint.
        """

        query = ""

        if len(args) > 0:
            bankAccount = args[0]
            query = f"SELECT * FROM receipts_payroll WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\' AND bank_account LIKE '%{bankAccount}%';"
        else:
            query = f"SELECT * FROM receipts_payroll WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return getData(query=query, filename="flask_api.ini")

    def getByCustomerId(self, id):
        """ {list[dict]} get receipt by customer id.

        Parameters
            - id {int} the id of the customer.
        """

        query = f"SELECT * FROM receipts_payroll WHERE customer_id = {id};"

        return getData(query=query, filename="flask_api.ini")
    
    def getLastRecord(self):
        """ {list[dict]} get the last date from 'date' column. """

        query = "SELECT date FROM receipts_payroll ORDER BY date DESC LIMIT 1;"

        return getData(query=query, filename="flask_api.ini")
    
    def deleteLastMonthData(self, start, end):
        """ execute DELETE operation that erase rows between a
        date range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        
        query = f"DELETE FROM receipts_payroll WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        return executeOperation(query=query, filename="flask_api.ini")
