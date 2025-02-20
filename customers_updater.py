from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.customers import *
import os, sys

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    updateCustomersPreviousRecords()

    os.system(f'echo "{PYTHON} {SCRIPTS}/lae_updater.py >> {LOGS}/lae.log 2>&1" | at now + 3 minutes')
except Exception as e:
    print(f"Error updating customers in customers_updater.py: {str(e)}.")
    os.system(f'echo "{PYTHON} {SCRIPTS}/customers_updater.py >> {LOGS}/customers.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addCustomersSpecificRange("2024-09-22", "2024-09-30")
