from data.repository.interfaces.i_employees import IEmployees
from data.repository.calls.helpers import getData

class Employees(IEmployees):
    """
    Handles every GET petition of employees table from database.

    Methods
        - getAllData.
    """

    def getAllData(self):
        """ {list[dict]} get all employees from database. """

        query = "SELECT * FROM employees;"

        return getData(query=query, filename="flask_api.ini")
