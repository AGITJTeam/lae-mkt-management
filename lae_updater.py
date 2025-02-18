from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.lae import *
import os, sys

try:
    print("-"*50)
    updateLaeDataTablesPreviousRecords()
except Exception as e:
    print(f"Error updating lae data in lae_updater.py: {str(e)}.")
    os.system(f'echo ""$PYTHON $SCRIPTS/lae_updater.py >> $LOGS/lae.log 2>&1" | at now + 5 minutes"')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)


# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format. Receipts Payroll data must have
# been added first.
#addLaeSpecificDateRange("2025-01-01", "2025-01-08")
