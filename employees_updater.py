from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.employees import *

print("-"*50)

updateEmployeesTable()
date, time = generateDateTimeUpdated()
print(f"\n {date} {time} \n")

print("-"*50)
