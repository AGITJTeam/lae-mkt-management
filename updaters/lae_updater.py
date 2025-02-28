import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.lae import *

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    updateLaeDataTablesPreviousRecords()
except Exception as e:
    print(f"Error updating lae data in lae_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m lae_updater >> $LOGS/lae.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)


# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format. Receipts Payroll data must have
# been added first.
#addLaeSpecificDateRange("2025-01-01", "2025-01-08")
