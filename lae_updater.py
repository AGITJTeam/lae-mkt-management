from data.repository.repository import (
    updateLaeDataTablesPreviousRecords,
    addLaeDataTablesTodayRecords,
    addLaeSpecificDateRange
)

print("-"*50)
updateLaeDataTablesPreviousRecords()
addLaeDataTablesTodayRecords()
print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format. Receipts Payroll data must have
# been added first.
#addLaeSpecificDateRange("start", "end")