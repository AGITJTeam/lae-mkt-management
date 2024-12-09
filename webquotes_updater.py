from data.repository.repository import updateWebquotesPreviousRecords, addWebquotesTodayRecords, addWebquotesSpecificDateRange

updateWebquotesPreviousRecords()
addWebquotesTodayRecords()

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
# addWebquotesSpecificDateRange("start", "end")