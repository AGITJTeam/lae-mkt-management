import requests as rq

BASE_LAE_API_URL = "http://50.18.96.65:8080"
TIMEOUT = 30

def getEmployees() -> rq.Response:
    """ Call Employees endpoint to get employee data.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"{BASE_LAE_API_URL}/Employees"

    try:
        employeesRequest = rq.get(url=url, timeout=TIMEOUT)

        if employeesRequest.status_code != rq.codes.ok:
            response = f"controllers.controller.py.getEmployees(). Status {employeesRequest.status_code}"
            print(response)
        else:
            return employeesRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getEmployees(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getEmployees(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getEmployees(). requests.ReadTimeOut: {e}"
        print(response)

def getReceiptsPayroll(start: str, end: str) -> rq.Response:
    """ Call Receipts/Payroll endpoint to get Customer Id and more data.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"{BASE_LAE_API_URL}/Receipts/PayRoll?startDate={start}&endDate={end}"

    try:
        rpRequest = rq.get(url=url, timeout=TIMEOUT)

        if rpRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getReceiptsPayroll(). Status {rpRequest.status_code} for date range from {start} to {end}"
            print(response)

            return {}
        else:
            return rpRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getReceiptsPayroll(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getReceiptsPayroll(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getReceiptsPayroll(). requests.ReadTimeOut: {e}"
        print(response)

def getCustomer(id: int) -> rq.Response:
    """ Call Customers endpoint to get customer data.

    Parameters
        - id {int} the id of the customer.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"{BASE_LAE_API_URL}/Customers/{id}"

    try:
        customersRequest = rq.get(url=url, timeout=TIMEOUT)

        if customersRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getCustomer(). Status {customersRequest.status_code} for Customer Id {id}"
            print(response)

            return {}
        else:
            return customersRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getCustomer(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getCustomer(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getCustomer(). requests.ReadTimeOut: {e}"
        print(response)

def getPoliciesDetails(id: int) -> rq.Response:
    """ Call Policies/Details endpoint to get Id Policie Hdr and more data.

    Parameters
        - id {int} the id of the policy.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"{BASE_LAE_API_URL}/Policies/Details/{id}"
    
    try:
        policiesRequest = rq.get(url, timeout=TIMEOUT)

        if policiesRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getPoliciesDetails(). Status {policiesRequest.status_code} for Customer Id {id}"
            print(response)
            
            return {}
        else:
            return policiesRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getPoliciesDetails(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getPoliciesDetails(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getPoliciesDetails(). requests.ReadTimeOut: {e}"
        print(response)

def getWebquotes(start: str, end: str) -> rq.Response:
    """ Call Webquotes endpoint to get webquotes data.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"https://app.adrianas.com/api/webquotes/csv?search=&agent=&clistatus=&fromDate={start}&toDate={end}&limit=all&zone=&manager=true&workedAt=&theagent=&referer=&fromDateS=&toDateS=&excluded=&language=&fulldata=false&dialpadCallCenter=&office_worked=&state=&office="

    try:
        wqRequest = rq.get(url=url, timeout=TIMEOUT)

        if wqRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getWebquotes(). Status {wqRequest.status_code} for date range from {start} to {end}"
            print(response)
        else:
            return wqRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getWebquotes(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getWebquotes(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getWebquotes(). requests.ReadTimeOut: {e}"
        print(response)

def getReceipt(id: int) -> rq.Response:
    """ Call Receitps endpoint to get only receipt data with its id.

    Parameters
        - id {int} the id of the receipt.

    Returns
        {requests.Response} api response in Json format.
    """

    url = f"{BASE_LAE_API_URL}/Receipts/{id}"

    try:
        receiptsRequest = rq.get(url=url, timeout=TIMEOUT)

        if receiptsRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getReceipt(). Status {receiptsRequest.status_code} for Receipt id {id}."
            print(response)
        else:
            return receiptsRequest.json()
    except ConnectionError as e:
        response = f"controllers.controller.py.getReceipt(). Connection error: {e}"
        print(response)
    except TimeoutError as e:
        response = f"controllers.controller.py.getReceipt(). Timeout error: {e}"
        print(response)
    except rq.exceptions.ReadTimeout as e:
        response = f"controllers.controller.py.getReceipt(). requests.ReadTimeOut: {e}"
        print(response)
