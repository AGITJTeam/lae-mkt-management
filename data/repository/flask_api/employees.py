from data.repository.calls.helpers import postDataframeToDb
from service.employees import generateEmployeesDf

def updateEmployeesTable() -> None:
    """ Updates Employees table in db. """

    employeesDf = generateEmployeesDf()
    postDataframeToDb(data=employeesDf, table="employees", mode="replace", filename="flask_api.ini")
    print("Employees table generated and posted...")
