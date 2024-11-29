from data.repository.interfaces.i_customers import ICustomers
from data.repository.calls.helpers import getData

class Customers(ICustomers):
    """
    Handles every GET petition of Customers table from database.

    Methods
        - getAllData {list[dict]} get all customers in database.
        - getById {list[dict]} get customer by customer id.
        - getByIds {list[dict]} get customers by a list of it's ids.
        - getByCustomerType {list[dict]} get customer by id of
          customer type.
    """
    
    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM customers;"

        return getData(query)

    def getById(self, id: int) -> list[dict]:
        """
        Parameters
            id {int} the id of the customer.
        """
        query = f"SELECT * FROM customers WHERE customer_id = {id};"

        return getData(query)
    
    def getByIds(self, ids: list) -> list[dict]:
        """
        Parameters
            id {list} list of customers ids.
        """
        values = ", ".join(str(id) for id in ids)
        query = f"SELECT * FROM customers WHERE customer_id IN ({values});"

        return getData(query)

    def getByCustomerType(self, idType: int) -> list[dict]:
        """
        Parameters
            idType {int} the id of the customer type.
        """
        query = f"SELECT * FROM customers WHERE id_cust_type = {idType};"

        return getData(query)
    