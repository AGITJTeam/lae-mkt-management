import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.dynamic_forms import updateTwoMonthsRedisKeys

PYTHON="/home/berenice/Documents/python-dev.v3/venv/bin/python"
SCRIPTS="/home/berenice/Documents/python-dev.v3/updaters"
LOGS="/home/berenice/Documents/cron-logs"

try:
    print("-"*50)
    updateTwoMonthsRedisKeys()
    
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m regional_offices_updater >> {LOGS}/regional_offices.log 2>&1" | at now + 3 minutes')
except Exception as e:
    print(f"Error generating report in dynamic_form_updater.py: {str(e)}.")
    os.system(f'echo "cd {SCRIPTS} && {PYTHON} -m dynamic_form_updater >> {LOGS}/dynamic_form.log 2>&1" | at now + 5 minutes')
    sys.exit(1)
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")
    print("-"*50)
