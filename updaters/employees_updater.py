import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.employees import *
from redisCli import redisCli

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

logger = logging.getLogger(__name__)

try:
    logger.info("-"*50)
    updateEmployeesTable()

    if redisCli:
        updateRedisKey()
except Exception as e:
    logger.error(f"Error updating all Employees in employees_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m employee_updater >> {LOGS}/employees.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    logger.info(f"\n{date} {time}\n")
    logger.info("-"*50)
