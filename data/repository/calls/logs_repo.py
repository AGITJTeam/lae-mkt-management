from data.repository.interfaces.i_logs import ILogs
from data.repository.calls.helpers import getData

class Logs(ILogs):
    """
    Handles every GET petition of logs table from database.

    Methods
        - getById {list[dict]} get log by id.
        - getByPoliciesHdrId {list[dict]} get log by policie hrd id.
        - getByReceiptHdrId {list[dict]} get log by receipt hrd id.
        - getByEmployeeId {list[dict]} get log by employee id.
    """

    def getById(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the log.
        """
        query = f"SELECT * FROM logs WHERE id_log == {id};"

        return getData(query)
    
    def getByPoliciesHdrId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the policie hdr.
        """
        query = f"SELECT * FROM logs WHERE id_policies_hdr == {id};"

        return getData(query)
        
    def getByReceiptHdrId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the receipt hdr.
        """
        query = f"SELECT * FROM logs WHERE id_receips_hdr == {id};"

        return getData(query)
        
    def getByEmployeeId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the employee.
        """
        query = f"SELECT * FROM logs WHERE id_employee_usr == {id};"

        return getData(query)