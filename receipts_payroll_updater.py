from data.repository.repository import (
    updateReceiptsPayrollPreviousRecords,
    addReceiptsPayrollTodayRecords,
    addReceiptsPayrollSpecificDateRange
)

print("-"*50)
updateReceiptsPayrollPreviousRecords()
addReceiptsPayrollTodayRecords()
print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
# addReceiptsPayrollSpecificDateRange("start", "end")