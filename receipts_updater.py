from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.receipts import *

print("-"*50)

updateReceiptsYesterdayRecords()
addReceiptsTodayRecords()
date, time = generateDateTimeUpdated()
print(f"\n{date} {time}\n")

print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addReceiptsSpecificRange("2024-12-21", "2024-12-26")