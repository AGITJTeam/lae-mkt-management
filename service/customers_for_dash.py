from data.repository.calls.customers_repo import Customers

def fetchCustomersAddress(ids: list):
    """ Gets customer data and retrieves only the physical address
    fields.
    
    Parameters
        - ids {list} A list of customer IDs.
    
    Returns
        {list[dict]} A list of dictionaries containing the physical
        address data of the customers.
    """

    customers = Customers()
    customersAddress = []

    data = customers.getByIds(ids)
    
    for customer in data:
        customerId = customer["customer_id"]
        physicalCity = customer["physical_city"]
        physicalState = customer["physical_state"]
        physicalZipCode = customer["physical_zip_code"]

        custAddress = {
            "customer_id": customerId,
            "physical_city": physicalCity,
            "physical_state": physicalState,
            "physical_zip_code": physicalZipCode
        }

        customersAddress.append(custAddress)
    
    return customersAddress
