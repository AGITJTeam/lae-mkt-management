from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.receipts_payroll import *

print("-"*50)

updateReceiptsPayrollPreviousRecords()
addReceiptsPayrollTodayRecords()
date, time = generateDateTimeUpdated()
print(f"\n{date} {time}\n")

print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsPayrollSpecificDateRange("2024-09-15", "2024-09-30")