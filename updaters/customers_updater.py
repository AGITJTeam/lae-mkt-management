import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from redisCli import redisCli
from data.repository.flask_api.customers import (
    updateCustomersPreviousRecords,
    updateRedisKey,
    addCustomersSpecificRange
)

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

logger = logging.getLogger(__name__)

try:
    logger.info("-"*50)
    updateCustomersPreviousRecords()

    if redisCli:
        updateRedisKey()

    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m lae_updater >> {LOGS}/lae.log 2>&1" | at now + 3 minutes')
except Exception as e:
    logger.error(f"Error updating Customers in customers_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m customers_updater >> {LOGS}/customers.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    logger.info(f"\n{date} {time}\n")
    logger.info("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
# addCustomersSpecificRange("start", "end")
