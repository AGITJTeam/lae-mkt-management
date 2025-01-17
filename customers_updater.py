from data.repository.calls.helpers import generateDateTimeUpdated
from data.repository.customers import *

print("-"*50)

updateCustomersPreviousRecords()
date, time = generateDateTimeUpdated()
print(f"\n{date} {time}\n")

print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
#addCustomersSpecificRange("2024-09-22", "2024-09-30")
