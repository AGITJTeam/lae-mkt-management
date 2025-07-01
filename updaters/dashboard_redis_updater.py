import logging, os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from redisCli import redisCli
from data.repository.stats_dash.redis_keys import (
    updateFinalSalesKey,
    updatePvcKey,
    updateOnlineSalesKey,
    updateTopCarriersKey,
    updateOutOfStateKey
)

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

logger = logging.getLogger(__name__)

try:
    if redisCli:
        logger.info("-"*50)
        updateFinalSalesKey()
        #updatePvcKey()
        updateOnlineSalesKey()
        updateTopCarriersKey()
        updateOutOfStateKey()
except Exception as e:
    logger.error(f"Error updating Redis Keys for StatsDashboard in dashboard_redis_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m dashboard_redis_updater >> {LOGS}/dash_redis.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    logger.info(f"\n{date} {time}\n")
    logger.info("-"*50)
