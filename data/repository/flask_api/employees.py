from data.repository.calls.helpers import postDataframeToDb
from service.employees import generateEmployeesDf

def updateEmployeesTable() -> None:
    """ Updates Employees table in db. """

    employeesDf = generateEmployeesDf()
    postDataframeToDb(employeesDf, "employees", "replace")
    print("Employees table generated and posted...")
