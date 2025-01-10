from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.repository import (
    updateLaeDataTablesPreviousRecords,
    addLaeSpecificDateRange
)

print("-"*50)

updateLaeDataTablesPreviousRecords()
date, time = generateDateTimeUpdated()
print(f"\n{date} {time}\n")

print("-"*50)


# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format. Receipts Payroll data must have
# been added first.
#addLaeSpecificDateRange("2025-01-01", "2025-01-08")
