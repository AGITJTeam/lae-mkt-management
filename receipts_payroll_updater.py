from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.flask_api.receipts_payroll import *
import logging

logger = logging.getLogger(__name__)

print("-"*50)

try:
    updateReceiptsPayrollPreviousRecords()
    addReceiptsPayrollTodayRecords()
except Exception as e:
    logger.error(f"Error updating receipts payroll: {str(e)}")
finally:
    date, time = generateDateTimeUpdated()
    print(f"\n{date} {time}\n")

print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsPayrollSpecificDateRange("2024-09-15", "2024-09-30")