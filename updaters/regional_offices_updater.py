import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.regional_offices import updateRedisKey
from logs.config import setupLogging
from redisCli import redisCli

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

setupLogging()
logger = logging.getLogger(__name__)

try:
    if redisCli:
        logger.info("-"*50)
        updateRedisKey()
        os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m dialpad_calls_updater >> {LOGS}/dialpad_calls.log 2>&1" | at now + 3 minutes')
except Exception as e:
    logger.error(f"Error generating report in regional_offices.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m regional_offices_updater >> {LOGS}/regional_offices.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    # Info por external log in /home/berenice/Documents/cron-logs
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
