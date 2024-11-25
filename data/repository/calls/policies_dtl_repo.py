from data.repository.interfaces.i_policies_dtl import IPoliciesDtl
from data.repository.calls.helpers import getData

class PoliciesDtl(IPoliciesDtl):
    """
    Handles every GET petition of policies_dtl table from database.

    Methods
        - getById {list[dict]} get policie dtl by id.
        - getByPoliciesHdrId {list[dict]} get policie dtl by policie
          hrd id.
        - getByCustomerId {list[dict]} get policie dtl by customer id.
        - getByProductId {list[dict]} get policie dtl by product id.
    """

    def getById(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the policie dtl.
        """
        query = f"SELECT * FROM policies_dtl WHERE id_policies_dtl == {id};"

        return getData(query)
    
    def getByPoliciesHdrId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the policie hdr.
        """
        query = f"SELECT * FROM policies_dtl WHERE id_policies_hrd == {id};"

        return getData(query)
        
    def getByCustomerId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the customer.
        """
        query = f"SELECT * FROM policies_dtl WHERE customer_id == {id};"

        return getData(query)
        
    def getByProductId(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the product.
        """
        query = f"SELECT * FROM policies_dtl WHERE id_product == {id};"

        return getData(query)