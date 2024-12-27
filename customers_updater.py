from data.repository.repository import (
    updateCustomersPreviousRecords,
    addCustomersSpecificRange
)

print("-"*50)
updateCustomersPreviousRecords()
print("-"*50)

# add data from a specific date range, substitute 'start' and 'end'
# with a date in YYYY-MM-DD format.
# addCustomersSpecificRange("start", "end")
