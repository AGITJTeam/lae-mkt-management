from data.repository.calls.helpers import postData
from service.employees import generateEmployeesDf

def updateEmployeesTable() -> None:
    """ Updates Employees table in vm with Lae Employees endpoint response. """

    employeesDf = generateEmployeesDf()
    postData(employeesDf, "employees", "replace")
    print("Employees table generated and posted...")
