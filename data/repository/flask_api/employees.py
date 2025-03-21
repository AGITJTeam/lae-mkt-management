from data.repository.calls.employees_repo import Employees
from data.repository.calls.helpers import postDataframeToDb
from service.employees import generateEmployeesDf
import json, redis

def updateEmployeesTable() -> None:
    """ Updates Employees table in db. """

    employeesDf = generateEmployeesDf()
    postDataframeToDb(data=employeesDf, table="employees", mode="replace", filename="flask_api.ini")
    print("Employees table generated and posted...")

def updateRedisKey() -> None:
    """ Updates Redis keys with retrieved employees data. """

    # Open Redis connection.
    redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # Generates all Customers data.
    employees = Employees()
    employeesJson = employees.getAllData()

    # Defines expiration time, redis Key and formatted data.
    expirationTime = 60*60*10
    redisKey = "AllEmployees"
    data = json.dumps(obj=employeesJson, default=str)

    # Set Redis key with 10 hours expiration time.
    redisCli.set(name=redisKey, value=data, ex=expirationTime)

    # Close Redis connection.
    redisCli.close()
