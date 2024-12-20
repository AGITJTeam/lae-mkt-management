import requests as rq

""" Call Employees endpoint to get employee data.

Returns
    {requests.Response} api response in Json format.

"""
def getEmployees() -> rq.Response:
    URL = "http://50.18.96.65:8080/Employees"

    try:
        employeesRequest = rq.get(URL, timeout=5)

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

""" Call Receipts/Payroll endpoint to get Customer Id and more data.

Returns
    {requests.Response} api response in Json format.

"""
def getReceiptsPayroll(start: str, end: str) -> rq.Response:
    url = f"http://50.18.96.65:8080/Receipts/PayRoll?startDate={start}&endDate={end}"

    try:
        rpRequest = rq.get(url, timeout=5)

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

""" Call Customers endpoint to get customer data.

Parameters
    id {int} the id of the customer.

Returns
    {requests.Response} api response in Json format.

"""
def getCustomer(id: int) -> rq.Response:
    url = f"http://50.18.96.65:8080/Customers/{id}"

    try:
        customersRequest = rq.get(url, timeout=10)

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

""" Call Policies/Details endpoint to get Id Policie Hdr and more data.

Parameters
    id {int} the id of the policy.

Returns
    {requests.Response} api response in Json format.

"""
def getPoliciesDetails(id: int) -> rq.Response:
    url = f"http://50.18.96.65:8080/Policies/Details/{id}"
    
    try:
        policiesRequest = rq.get(url, timeout=15)

        if policiesRequest.status_code != rq.codes.ok:
            #response = f"--controllers.controller.py.getPoliciesDetails(). Status {policiesRequest.status_code} for Customer Id {id}"
            #print(response)
            
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

""" Call Webquotes endpoint to get webquotes data.

Returns
    {requests.Response} api response in Json format.

"""
def getWebquotes(start: str, end: str) -> rq.Response:
    url = f"https://app.adrianas.com/api/webquotes/csv?search=&agent=&clistatus=&fromDate={start}&toDate={end}&limit=20000&zone=&manager=true&workedAt=&theagent=&referer=&fromDateS=&toDateS=&excluded=&language=&fulldata=false&dialpadCallCenter=&office_worked=&state=&office="

    try:
        wqRequest = rq.get(url, timeout=15)

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
