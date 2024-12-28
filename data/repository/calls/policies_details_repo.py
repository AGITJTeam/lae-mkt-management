from data.repository.interfaces.i_policies_details import IPoliciesDetails
from data.repository.calls.helpers import getData

class PoliciesDetails(IPoliciesDetails):
    """
    Handles every GET petition of policies_receipts table from database.

    Methods
        - getAllData.
    """

    def getAllData(self):
        """ {list[dict]} get all policies from database. """

        query = "SELECT * FROM policies_details;"

        return getData(query)