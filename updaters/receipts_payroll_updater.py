import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from redisCli import redisCli
from data.repository.flask_api.receipts_payroll import (
    updateReceiptsPayrollPreviousRecords,
    updateTwoMonthsRedisKeys,
    addReceiptsPayrollSpecificDateRange
)

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

logger = logging.getLogger(__name__)

try:
    logger.info("-"*50)
    updateReceiptsPayrollPreviousRecords()

    if redisCli:
        updateTwoMonthsRedisKeys()
    
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m receipts_updater >> {LOGS}/receipts.log 2>&1" | at now + 3 minutes')
except Exception as e:
    logger.error(f"Error updating Receipts in receipts_payroll_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m receipts_payroll_updater >> {LOGS}/receipts_payroll.log 2>&1" | at now + 3 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    logger.info(f"\n{date} {time}\n")
    logger.info("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsPayrollSpecificDateRange("start", "end")
