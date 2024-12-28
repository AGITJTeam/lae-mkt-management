from data.repository.interfaces.i_employees import IEmployees
from data.repository.calls.helpers import getData

class Employees(IEmployees):
    """
    Handles every GET petition of employees table from database.

    Methods
        - getAllData.
        - getById.
        - getByEmailWork.
        - getByUsername.
    """

    def getAllData(self):
        """ {list[dict]} get all employees from database. """

        query = "SELECT * FROM employees;"

        return getData(query)

    def getById(self, id):
        """ {list[dict]} get employee by employee id.

        Parameters
            id {int} the id of the employee.
        """

        query = f"SELECT * FROM employees WHERE idemployee = {id};"

        return getData(query)

    def getByEmailWork(self, emailWork):
        """ {list[dict]} get employee by emailWork.

        Parameters
            emailWork {str} employee work email.
        """

        query = f"SELECT * FROM employees WHERE emailWork = \'{emailWork}\';"

        return getData(query)
    
    def getByUsername(self, username):
        """ {list[dict]} get employee by username.
        
        Parameters
            username {str} employee username.
        """
        query = f"SELECT * FROM employees WHERE username = \'{username}\';"

        return getData(query)
