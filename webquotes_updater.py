from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.repository import (
    updateWebquotesPreviousRecords,
    addWebquotesTodayRecords,
    addWebquotesSpecificDateRange
)

print("-"*50)

updateWebquotesPreviousRecords()
addWebquotesTodayRecords()
date, time = generateDateTimeUpdated()
print(f"\n{date} {time}\n")

print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addWebquotesSpecificDateRange("start", "end")
