from data.repository.interfaces.i_employees import IEmployees
from data.repository.calls.helpers import getData

class Employees(IEmployees):
    """
    Handles every GET petition of employees table from database.

    Methods
        - getAllData {list[dict]} get all employees from database.
        - getEmployeeById {list[dict]} get employee by employee id.
        - getEmployeeByEmailWork {list[dict]} get employee by emailWork.
        - getEmployeeByUsername {list[dict]} get employee by username.
    """

    def getAllData(self) -> list[dict]:
        query = "SELECT * FROM employees;"

        return getData(query)

    def getEmployeeById(self, id: int) -> list[dict]:
        """
        Parameters
            id {int} the id of the employee.
        """
        query = f"SELECT * FROM employees WHERE idemployee = {id};"

        return getData(query)

    def getEmployeeByEmailWork(self, emailWork: str) -> list[dict]:
        """
        Parameters
            emailWork {str} employee work email.
        """
        query = f"SELECT * FROM employees WHERE emailWork = \'{emailWork}\';"

        return getData(query)
    
    def getEmployeeByUsername(self, username: str) -> list[dict]:
        """
        Parameters
            username {str} employee username.
        """
        query = f"SELECT * FROM employees WHERE username = \'{username}\';"

        return getData(query)
