import requests as rq

""" Call Employees endpoint to get employee data.

Returns
    {Response} api response in Json format.

"""
def getEmployees() -> rq.Response:
    url = "http://54.183.172.143:8080/Employees"

    try:
        employeesRequest = rq.get(url, timeout=5)

        if employeesRequest.status_code != rq.codes.ok:
            response = {
                "pathException": "controllers.controller.py.getEmployee()",
                "message": f"Status code respnose: {employeesRequest.status_code}"
            }
            print(response)
        else:
            return employeesRequest.json()
    except ConnectionError as e:
        response = {
            "pathException": "controllers.controller.py.getEmployee()",
            "message": f"Connection error: {e}"
        }
        print(response)
    except TimeoutError as e:
        response = {
            "pathException": "controllers.controller.py.getEmployee()",
            "message": f"Timeout error: {e}"
        }
        print(response)

""" Call Receipts/Payroll endpoint to get Customer Id and more data.

Returns
    {Response} api response in Json format.

"""
def getReceiptsPayroll(start: str, end: str) -> rq.Response:
    url = f"http://54.183.172.143:8080/Receipts/PayRoll?startDate={start}&endDate={end}"

    try:
        rpRequest = rq.get(url, timeout=5)

        if rpRequest.status_code != rq.codes.ok:
            response = {
                "pathException": "controllers.controller.py.getReceiptsPayroll()",
                "message": f"Status code respnose: {rpRequest.status_code}"
            }
            print(response)

            return {}
        else:
            return rpRequest.json()
    except ConnectionError as e:
        response = {
            "pathException": "controllers.controller.py.getReceiptsPayroll()",
            "message": f"Connection error: {e}"
        }
        print(response)
    except TimeoutError as e:
        response = {
            "pathException": "controllers.controller.py.getReceiptsPayroll()",
            "message": f"Timeout error: {e}"
        }
        print(response)

""" Call Customers endpoint to get customer data.

Parameters
    id {int} the id of the customer.

Returns
    {Response} api response in Json format.

"""
def getCustomer(id: int) -> rq.Response:
    url = f"http://54.183.172.143:8080/Customers/{id}"

    try:
        customersRequest = rq.get(url, timeout=10)

        if customersRequest.status_code != rq.codes.ok:
            response = {
                "pathException": "controllers.controller.py.getCustomer()",
                "message": f"Status code respnose: {customersRequest.status_code}"
            }
            print(response)

            return {}
        else:
            return customersRequest.json()
    except ConnectionError as e:
        response = {
            "pathException": "controllers.controller.py.getCustomer()",
            "message": f"Connection error: {e}"
        }
        print(response)
    except TimeoutError as e:
        response = {
            "pathException": "controllers.controller.py.getCustomer()",
            "message": f"Timeout error: {e}"
        }
        print(response)

""" Call Policies/Details endpoint to get Id Policie Hdr and more data.

Parameters
    id {int} the id of the policy.

Returns
    {Response} api response in Json format.

"""
def getPoliciesDetails(id: int) -> rq.Response:
    # fix this later
    url = f"{policiesUrl}/{id}"
    
    try:
        policiesRequest = rq.get(url, timeout=5)

        if policiesRequest.status_code != rq.codes.ok:
            response = {
                "pathException": "controllers.controller.py.getPoliciesDetails()",
                "message": f"Status code response: {policiesRequest.status_code}"
            }
            
            return {}
        else:
            return policiesRequest.json()
    except ConnectionError as e:
        response = {
            "pathException": "controllers.controller.py.getPoliciesDetails()",
            "message": f"Connection error: {e}"
        }
        print(response)
    except TimeoutError as e:
        response = {
            "pathException": "controllers.controller.py.getPoliciesDetails()",
            "message": f"Timeout error: {e}"
        }
        print(response)

""" Call Webquotes endpoint to get webquotes data.

Returns
    {Response} api response in Json format.

"""
def getWebquotes(start: str, end: str) -> rq.Response:
    url = f"https://app.adrianas.com/api/webquotes/csv?search=&agent=&clistatus=&fromDate={start}&toDate={end}&limit=20000&zone=&manager=true&workedAt=&theagent=&referer=&fromDateS=&toDateS=&excluded=&language=&fulldata=false&dialpadCallCenter=&office_worked=&state=&office="

    try:
        wqRequest = rq.get(url, timeout=15)

        if wqRequest.status_code != rq.codes.ok:
            response = {
                "pathException": "controllers.controller.py.getReceiptsPayroll()",
                "message": f"Status code respnose: {wqRequest.status_code}"
            }
            print(response)
        else:
            return wqRequest.json()
    except ConnectionError as e:
        response = {
            "pathException": "controllers.controller.py.getReceiptsPayroll()",
            "message": f"Connection error: {e}"
        }
        print(response)
    except TimeoutError as e:
        response = {
            "pathException": "controllers.controller.py.getReceiptsPayroll()",
            "message": f"Timeout error: {e}"
        }
        print(response)