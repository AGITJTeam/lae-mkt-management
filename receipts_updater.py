from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.receipts import *
import os, sys

try:
    print("-"*50)
    updateReceiptsYesterdayRecords()
    addReceiptsTodayRecords()
except Exception:
    print(f"Error updating Receipts in receipts_updater.py.")
    os.system(f'echo ""$PYTHON $SCRIPTS/receipts_updater.py >> $LOGS/receipts.log 2>&1" | at now + 5 minutes"')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsSpecificRange("2024-12-21", "2024-12-26")
