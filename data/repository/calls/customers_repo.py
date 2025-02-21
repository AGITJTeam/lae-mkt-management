from data.repository.interfaces.i_customers import ICustomers
from data.repository.calls.helpers import getData, executeOperation

class Customers(ICustomers):
    """
    Handles every petition of Customers table from database.

    Methods
        - getAllData.
        - getById.
        - getByIds.
        - deleteByIds.
    """

    def getAllData(self):
        """ {list[dict]} get all customers in database. """
        query = "SELECT * FROM customers;"
        return getData(query=query, filename="flask_api.ini")

    def getById(self, id):
        """ {list[dict]} get customer by customer id.

        Parameters
            id {int} the id of the customer.
        """

        query = f"SELECT DISTINCT * FROM customers WHERE customer_id = {id};"

        return getData(query=query, filename="flask_api.ini")
    
    def getByIds(self, ids):
        """ {list[dict]} get customers by a list of it's ids.

        Parameters
            id {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"SELECT DISTINCT * FROM customers WHERE customer_id IN ({values});"

        return getData(query=query, filename="flask_api.ini")
    
    def deleteByIds(self, ids):
        """ Delete customers by its ids.

        Parameters
            ids {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM customers WHERE customer_id IN ({values})"

        return executeOperation(query=query, filename="flask_api.ini")
