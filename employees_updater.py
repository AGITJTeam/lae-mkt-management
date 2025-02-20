from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.employees import *
import os, sys

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    updateEmployeesTable()
except Exception as e:
    print(f"Error updating receipts in employees_updater.py: {str(e)}.")
    os.system(f'echo "{PYTHON} {SCRIPTS}/employee_updater.py >> {LOGS}/employees.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)
