from data.repository.interfaces.i_policies_details import IPoliciesDetails
from data.repository.calls.helpers import executeOperation, getData

class PoliciesDetails(IPoliciesDetails):
    """
    Handles every GET petition of policies_receipts table from database.

    Methods
        - getAllData.
        - getDataByCustId.
        - deleteByIds.
    """

    def getAllData(self):
        """ {list[dict]} get all policies from database. """

        query = "SELECT * FROM policies_details;"

        return getData(query)
    
    def getDataByCustId(self, ids):
        values = ", ".join(str(id) for id in ids)
        query = f"SELECT * FROM policies_details WHERE id_customer IN ({values})"

        return getData(query)

    def deleteByIds(self, ids):
        """ Delete customers by its ids.

        Parameters
            ids {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM policies_details WHERE id_customer IN ({values})"

        return executeOperation(query)