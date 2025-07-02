import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from logs.config import setupLogging
from redisCli import redisCli
from data.repository.flask_api.webquotes import (
    updateWebquotesPreviousRecords,
    updateWTwoMonthsRedisKeys,
    updateWDTwoMonthsRedisKeys,
    updateAllWebquotesRedisKey,
    addWebquotesSpecificDateRange
)

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

setupLogging()
logger = logging.getLogger(__name__)

try:
    logger.info("-"*50)
    updateWebquotesPreviousRecords()

    if redisCli:
        updateWTwoMonthsRedisKeys()
        updateWDTwoMonthsRedisKeys()
        updateAllWebquotesRedisKey()

    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m receipts_payroll_updater >> {LOGS}/receipts_payroll.log 2>&1" | at now + 3 minutes')
except Exception as e:
    logger.error(f"Error updating Webquotes in webquotes_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m webquotes_updater >> {LOGS}/webquotes.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    # Info por external log in /home/berenice/Documents/cron-logs
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addWebquotesSpecificDateRange("start", "end")
