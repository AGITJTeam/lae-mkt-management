from service.columnsTransformations import empNewColumnsNames
from service.helpers import renameColumns
from data.models.employees_model import EmployeeModel
from controllers.controller import getEmployees
import pandas as pd

def generateEmployeesDf() -> pd.DataFrame:
    """ Create Employees DataFrame with renamed columns with API response.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    
    employees = []
    employeesJson = getEmployees()

    for employee in employeesJson:
        employeeModel = EmployeeModel(**employee)
        employees.append(employeeModel)
    
    employeesDf = pd.DataFrame(employees)
    renamedEmployeesDf = renameColumns(employeesDf, empNewColumnsNames)
    renamedEmployeesDf.sort_values(by=["date_created"], inplace=True) 
    renamedEmployeesDf["id"] = range(1, len(renamedEmployeesDf) + 1)
    print(renamedEmployeesDf)

    return renamedEmployeesDf
