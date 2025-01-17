from data.repository.interfaces.i_policies_dtl import IPoliciesDtl
from data.repository.calls.helpers import executeOperation, getData

class PoliciesDtl(IPoliciesDtl):
    """
    Handles every GET petition of policies_dtl table from database.

    Methods
        - getById.
        - getByPoliciesHdrId.
        - getByCustomerId.
        - getByProductId.
    """

    def getById(self, id):
        """ {list[dict]} get policie dtl by id.

        Parameters
            - id {int} the id of the policie dtl.
        """

        query = f"SELECT * FROM policies_dtl WHERE id_policies_dtl == {id};"

        return getData(query)
    
    def getByPoliciesHdrId(self, id):
        """ {list[dict]} get policie dtl by policie hrd id.

        Parameters
            - id {int} the id of the policie hdr.
        """

        query = f"SELECT * FROM policies_dtl WHERE id_policies_hrd == {id};"

        return getData(query)
        
    def getByCustomerId(self, id):
        """ {list[dict]} get policie dtl by customer id.

        Parameters
            - id {int} the id of the customer.
        """

        query = f"SELECT * FROM policies_dtl WHERE customer_id == {id};"

        return getData(query)
        
    def getByProductId(self, id):
        """ {list[dict]} get policie dtl by product id.

        Parameters
            - id {int} the id of the product.
        """
        query = f"SELECT * FROM policies_dtl WHERE id_product == {id};"

        return getData(query)

    def deleteByIds(self, ids):
        """ Delete customers by its ids.

        Parameters
            ids {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM policies_dtl WHERE customer_id IN ({values})"

        return executeOperation(query)