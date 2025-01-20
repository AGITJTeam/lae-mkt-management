import requests as rq
import os, logging

logger = logging.getLogger(__name__)

BASE_LAE_API_URL = "http://50.18.96.65:8080"
SECURE2_URL = "http://secure2.saashr.com/ta/rest/v1"

USERNAME = os.getenv("S2_USER")
PASSWORD = os.getenv("S2_PASS")
COMPANY_SHORTNAME = "AGI04"
TIMEOUT = 30

def getEmployees() -> rq.Response:
    """ Call Employees endpoint to get employee data.

    Returns
        {requests.Response} api response in Json format.
    """

    URL = f"{BASE_LAE_API_URL}/Employees"

    # try:
    #     employeesRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     rq.exceptions.ConnectionError,
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     logger.error(f"Error in getEmployees: {str(e)}")
    #     raise
    # else:
    #     if employeesRequest.status_code != rq.codes.ok:
    #         logger.error(f"Status code {employeesRequest.status_code} in getEmployees")
    #     else:
    #         return employeesRequest.json()

    try:
        employeesRequest = rq.get(url=URL, timeout=TIMEOUT)

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

    URL = f"{BASE_LAE_API_URL}/Receipts/PayRoll?startDate={start}&endDate={end}"

    # try:
    #     rpRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     rq.exceptions.ConnectionError,
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     logger.error(f"Error in getReceiptsPayroll: {str(e)}")
    #     raise
    # else:
    #     if rpRequest.status_code != rq.codes.ok:
    #         logger.error(f"Status code {rpRequest.status_code} in getReceiptsPayroll from {start} to {end}.")
    #         return {}
    #     else:
    #         return rpRequest.json()

    try:
        rpRequest = rq.get(url=URL, timeout=TIMEOUT)

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

    URL = f"{BASE_LAE_API_URL}/Customers/{id}"

    # try:
    #     customersRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     rq.exceptions.ConnectionError,
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     logger.error(f"Error in getCustomer: {str(e)}")
    #     raise
    # else:
    #     if customersRequest.status_code != rq.codes.ok:
    #         logger.error(f"Error fetching customer in getCustomer with id {id}. Response status {customersRequest.status_code}")
    #         return {}
    #     else:
    #         return customersRequest.json()

    try:
        customersRequest = rq.get(url=URL, timeout=TIMEOUT)

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

    URL = f"{BASE_LAE_API_URL}/Policies/Details/{id}"

    # try:
    #     policiesRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     rq.exceptions.ConnectionError, 
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     logger.error(f"Error in getPoliciesDetails: {str(e)}")
    #     raise
    # else:
    #     if policiesRequest.status_code != rq.codes.ok:
    #         logger.error(f"Error fetching policy in getPoliciesDetails with id {id}. Response status {policiesRequest.status_code}")
    #         return {}
    #     else:
    #         return policiesRequest.json()

    try:
        policiesRequest = rq.get(url=URL, timeout=TIMEOUT)

        if policiesRequest.status_code != rq.codes.ok:
            response = f"--controllers.controller.py.getPoliciesDetails(). Status {policiesRequest.status_code} for Policy Id {id}"
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

    URL = (
        "https://app.adrianas.com/api/webquotes/csv?"
        "search=&"
        "agent=&"
        "clistatus=&"
        f"fromDate={start}&"
        f"toDate={end}&"
        "limit=all&"
        "zone=&"
        "manager=true&"
        "workedAt=&"
        "theagent=&"
        "referer=&"
        "fromDateS=&"
        "toDateS=&"
        "excluded=&"
        "language=&"
        "fulldata=false&"
        "dialpadCallCenter=&"
        "office_worked=&"
        "state=&office="
    )

    # try:
    #     wqRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     rq.exceptions.ConnectionError, 
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     logger.error(f"Error in getWebquotes: {str(e)}")
    #     raise
    # else:
    #     if wqRequest.status_code != rq.codes.ok:
    #         logger.error(f"Status code {wqRequest.status_code} in getWebquotes from {start} to {end}.")
    #         return {}
    #     else:
    #         return wqRequest.json()

    try:
        wqRequest = rq.get(url=URL, timeout=TIMEOUT)

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

    URL = f"{BASE_LAE_API_URL}/Receipts/{id}"

    # try:
    #     receiptsRequest = rq.get(url=URL, timeout=TIMEOUT)
    # except (
    #     rq.exceptions.ConnectionError, 
    #     rq.exceptions.ReadTimeout,
    #     rq.exceptions.RequestException
    # ) as e:
    #     # ====================================================================================== revisar el flujo de las excepciones...
    #     logger.error(f"Error in getReceipt: {str(e)}")
    #     raise
    # else:
    #     if receiptsRequest.status_code != rq.codes.ok:
    #         logger.error(f"Error fetching receipt in getReceipt with id {id}. Response status {receiptsRequest.status_code}")
    #         return {}
    #     else:
    #         return receiptsRequest.json()

    try:
        receiptsRequest = rq.get(url=URL, timeout=TIMEOUT)

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

def fetchAgiReports(reportId: int) -> rq.Response:
    """ Fetch AGI reports from the SECURE2 API using provided credentials.

    Parameters
        - reportId {int} the ID of the report to fetch.

    Returns
        {requests.Response} the response object containing the fetched AGI 
            report in CSV format if the request is successful.
    """

    token = generateTokenForSecure2(USERNAME, PASSWORD)
    
    PARAMETERS = {
        "company:shortname": COMPANY_SHORTNAME
    }
    HEADERS = {
        "Accept": "text/csv",
        "Authentication": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    URL = f"{SECURE2_URL}/report/saved/{reportId}"

    try:
        response = rq.get(url=URL, headers=HEADERS, params=PARAMETERS)
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error fetching Agi Report in fetchReportOne: {str(e)}")
        # ====================================================================================== revisar el flujo de las excepciones...
        raise
    else:
        if response.status_code == rq.codes.unauthorized:
            print("Token expired, refreshing...")
            currentToken = generateTokenForSecure2(USERNAME, PASSWORD)
            if currentToken:
                return fetchAgiReports(reportId)
            
            print("Unable to refresh token...")
        
        return response

def generateTokenForSecure2(username: str, password: str) -> str:
    """ Call Secure2 endpoint to get user token for current session.

    Parameters
        - username {str}.
        - password {str} password of user.

    Returns
        {str} token for the session.
    """

    API_KEY = os.getenv("API_KEY")
    URL = f"{SECURE2_URL}/login"

    DATA = {
        "credentials": {
            "username": username,
            "password": password,
            "company": COMPANY_SHORTNAME
        }
    }
    HEADERS = {
        "Content-Type": "application/json",
        "Api-Key": API_KEY,
        "Accept": "application/json"
    }

    try:
        response = rq.post(url=URL, json=DATA, headers=HEADERS)
        # ====================================================================================== revisar el flujo de las excepciones...
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error generating token in generateTokenForSecure2: {str(e)}")
        return None
    else:
        if response.status_code == rq.codes.bad_request or response.status_code == rq.codes.unauthorized or response.status_code == rq.codes.not_found:
            logger.error(f"Error generating token in generateTokenForSecure2: Status code {response.status_code}. Message: {response.text}")
            return None
        if response.status_code == rq.codes.forbidden:
            logger.error("Error generating token in generateTokenForSecure2: Temporary token issued. Please change your password")
            return None
        
        currentToken = response.json().get("token")
        
        return currentToken
