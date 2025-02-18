from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.receipts_payroll import *
import os, sys

try:
    print("-"*50)
    addReceiptsPayrollTodayRecords()
    updateReceiptsPayrollPreviousRecords()
    
    os.system('echo "$PYTHON $SCRIPTS/customers_updater.py >> $LOGS/customers.log 2>&1" | at now + 3 minutes')
except Exception as e:
    print(f"Error updating receipts in receipts_payroll_updater.py: {str(e)}.")
    os.system('echo "$PYTHON $SCRIPTS/receipts_payroll_updater.py >> $LOGS/receipts_payroll.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsPayrollSpecificDateRange("2024-09-15", "2024-09-30")
