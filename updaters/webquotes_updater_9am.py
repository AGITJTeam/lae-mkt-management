import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.webquotes import (
    addWebquotesTodayRecords,
    updateWTwoMonthsRedisKeys,
    updateWDTwoMonthsRedisKeys,
    updateAllWebquotesRedisKey
)

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    addWebquotesTodayRecords()
    updateWTwoMonthsRedisKeys()
    updateWDTwoMonthsRedisKeys()
    updateAllWebquotesRedisKey()

    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m receipts_payroll_updater_9am >> {LOGS}/receipts_payroll.log 2>&1" | at now + 3 minutes')
except Exception as e:
    print(f"Error updating Webquotes in webquotes_updater_9am.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m webquotes_updater_9am >> {LOGS}/webquotes.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
else:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)
