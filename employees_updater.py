from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.employees import *
import logging

logger = logging.getLogger(__name__)

print("-"*50)

try:
    updateEmployeesTable()
except Exception as e:
    logger.error(f"Error updating employees: {str(e)}")
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")

print("-"*50)
