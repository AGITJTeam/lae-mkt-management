from data.repository.repository import updateReceiptsPreviousRecords, addReceiptsTodayRecords, addReceiptsSpecificDateRange

updateReceiptsPreviousRecords()
addReceiptsTodayRecords()

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
# addReceiptsSpecificDateRange("start", "end")