from data.repository.interfaces.i_customers import ICustomers
from data.repository.calls.helpers import getData

class Customers(ICustomers):
    """
    Handles every GET petition of customers table from database.

    Methods
        - getAllData {list[dict]} get all customers in database.
        - getCustomerById {list[dict]} get customer by customer id.
        - getCustomerByCustType {list[dict]} get customer by id of
          customer type.
    """
    
    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM customers;"

        return getData(query)

    def getCustomerById(self, id: int) -> list[dict]:
        """
        Parameters
            id {int} the id of the customer.
        """
        query = f"SELECT * FROM customers WHERE idCustomer = {id};"

        return getData(query)

    def getCustomerByCustType(self, idType: int) -> list[dict]:
        """
        Parameters
            idType {int} the id of the customer type.
        """
        query = f"SELECT * FROM customers WHERE idCustType = {idType};"

        return getData(query)