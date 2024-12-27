from data.repository.repository import (
    updateReceiptsPreviousRecords,
    addReceiptsTodayRecords,
    addReceiptsSpecificRange
)

print("-"*50)
updateReceiptsPreviousRecords()
addReceiptsTodayRecords()
print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsSpecificRange("2024-12-21", "2024-12-26")