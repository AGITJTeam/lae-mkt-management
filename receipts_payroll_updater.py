from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.receipts_payroll import *
import os, sys

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    addReceiptsPayrollTodayRecords()
    updateReceiptsPayrollPreviousRecords()
    
    os.system(f'echo "{PYTHON} {SCRIPTS}/receipts_updater.py >> {LOGS}/receipts.log 2>&1" | at now + 3 minutes')
except Exception as e:
    print(f"Error updating receipts in receipts_payroll_updater.py: {str(e)}.")
    os.system(f'echo "{PYTHON} {SCRIPTS}/receipts_payroll_updater.py >> {LOGS}/receipts_payroll.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsPayrollSpecificDateRange("2024-09-15", "2024-09-30")
