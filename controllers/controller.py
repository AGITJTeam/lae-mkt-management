from config import Config
import requests as rq
import os, logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv('/home/berenice/Documents/python-dev.v3/.env')

LAE_URL = "http://50.18.96.65:8080"
N_LAE_URL = "http://lae.agibusinessgroup.com:8080"
WQ_NEXT_URL = "https://app.adrianas.com/api"
WQ_GO_URL = "https://wq-api.adrianas.com"
SECURE2_URL = "http://secure2.saashr.com/ta/rest/v1"
COMPANY_SHORTNAME = "AGI04"
TIMEOUT = 60

def generateTokenForLae() -> str | None:
    """ Call LAE to get Token for current session.
    """

    LAE_WQ_URL = "https://wq-api.adrianas.com/lae-token"
    KEY = os.getenv("LAE_WQ_KEY")

    HEADERS = {
        "Authorization": KEY
    }

    try:
        response = rq.get(url=LAE_WQ_URL, headers=HEADERS)
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error generating token in generateTokenForLae: {str(e)}")
        return None
    else:
        if response.status_code == rq.codes.bad_request or response.status_code == rq.codes.unauthorized or response.status_code == rq.codes.not_found:
            logger.error(f"Error generating token in generateTokenForLae: Status code {response.status_code}. Message: {response.text}")
            return None
        if response.status_code == rq.codes.forbidden:
            logger.error("Error generating token in generateTokenForLae: Temporary token issued. Please change your password")
            return None
        
        currentToken = response.json().get("data")
        
        return currentToken

def getEmployees() -> dict | None:
    """ Call Employees endpoint from LAE to get employee data.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = f"{N_LAE_URL}/Employees"
    TOKEN = generateTokenForLae()
    HEADERS = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        employeesRequest = rq.get(url=URL, headers=HEADERS)
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getEmployees: {str(e)}")
        raise
    else:
        if employeesRequest.status_code != rq.codes.ok:
            logger.error(f"Status code {employeesRequest.status_code} in getEmployees")
        return employeesRequest.json()

def getReceipt(id: int) -> dict | None:
    """ Call Receitps endpoint from LAE to get one Receipt data.

    Parameters
        - id {int} the id of the receipt.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = f"{N_LAE_URL}/Receipts/{id}"
    TOKEN = generateTokenForLae()
    HEADERS = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        receiptsRequest = rq.get(url=URL, headers=HEADERS, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError, 
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getReceipt: {str(e)}")
        raise
    else:
        if receiptsRequest.status_code != rq.codes.ok:
            logger.error(f"Error fetching receipt in getReceipt with id {id}. Response status {receiptsRequest.status_code}")
            logger.warning(f"response text: {receiptsRequest.text}")
            logger.warning(f"response content: {receiptsRequest.content}")
            return {}
        return receiptsRequest.json()

def getReceiptsPayroll(start: str, end: str) -> dict | None:
    """ Call Receipts/Payroll endpoint from LAE to get Receipt Payroll data.

    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = f"{N_LAE_URL}/Receipts/PayRoll?startDate={start}&endDate={end}"
    TOKEN = generateTokenForLae()
    HEADERS = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        rpRequest = rq.get(url=URL, headers=HEADERS, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getReceiptsPayroll: {str(e)}")
        raise
    else:
        if rpRequest.status_code != rq.codes.ok:
            logger.error(f"Status code {rpRequest.status_code} in getReceiptsPayroll from {start} to {end}.")
            logger.warning(f"response text: {rpRequest.text}")
            logger.warning(f"response content: {rpRequest.content}")
            return {}
        return rpRequest.json()

def getCustomer(id: int) -> dict | None:
    """ Call Customers endpoint from LAE to get Customer data.

    Parameters
        - id {int} the id of the customer.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = f"{N_LAE_URL}/Customers/{id}"
    TOKEN = generateTokenForLae()
    HEADERS = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        customersRequest = rq.get(url=URL, headers=HEADERS, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError,
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getCustomer: {str(e)}")
        raise
    else:
        if customersRequest.status_code != rq.codes.ok:
            logger.error(f"Error fetching customer in getCustomer with id {id}. Response status {customersRequest.status_code}")
            logger.warning(f"response text: {customersRequest.text}")
            logger.warning(f"response content: {customersRequest.content}")
            return {}
        return customersRequest.json()

def getPoliciesDetails(id: int) -> rq.Response:
    """ Call Policies/Details endpoint to get Id Policie Hdr and more data.

    Parameters
        - id {int} the id of the policy.

    Returns
        {requests.Response} api response in Json format.
    """

    URL = f"{LAE_URL}/Policies/Details/{id}"

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

def getWebquotes(start: str, end: str) -> dict | None:
    """ Call Webquotes endpoint from Next backend to get Webquotes data.

    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = (
        f"{WQ_NEXT_URL}/webquotes/csv?"
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
        "state=&"
        "office="
    )

    try:
        wqRequest = rq.get(url=URL, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError, 
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getWebquotes: {str(e)}")
        raise
    else:
        if wqRequest.status_code != rq.codes.ok:
            logger.error(f"Status code {wqRequest.status_code} in getWebquotes from {start} to {end}.")
            logger.warning(f"response text: {wqRequest.text}")
            logger.warning(f"response content: {wqRequest.content}")
            return {}
        return wqRequest.json()

def getWebquotesGo(start: str, end: str) -> dict | None:
    """ Call Webquotes endpoint from Go backend to get Webquotes data.

    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    API_KEY = os.getenv("LAE_WQ_KEY")
    URL = (
        f"{WQ_GO_URL}/webquotes/manager-dashboard-leads?"
        f"fromDate={start}&"
        f"toDate={end}&"
        "csv=true&"
        "limit=20000"
    )
    HEADERS = {
        "Authorization": API_KEY,
    }

    try:
        wqRequest = rq.get(url=URL, headers=HEADERS, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError, 
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getWebquotesGo: {str(e)}")
        raise
    else:
        if wqRequest.status_code != rq.codes.ok:
            logger.error(f"Status code {wqRequest.status_code} in getWebquotesGo from {start} to {end}.")
            logger.warning(f"response text: {wqRequest.text}")
            logger.warning(f"response content: {wqRequest.content}")
            return {}
        return wqRequest.json()

def getDynamicForm(start: str, end: str) -> dict | None:
    """ Call DynamicFroms endpoint from App.Adrianas to get Home Owners
    Dynamic Form data.

    Parameters
        - start {str} the beginning of the date range.
        - end {str} the end of the date range.

    Returns
        {dict | None} api response in Json format or None if exception raise.
    """

    URL = (
        f"{WQ_NEXT_URL}/dynamicforms/filtered?"
        "limit=2000&"
        "status=&"
        "purpose=&"
        "searchForm=&"
        f"fromDate={start}&"
        f"toDate={end}&"
        "agent=&"
        "agent_office=&"
        "idform=16"
    )

    try:
        wqRequest = rq.get(url=URL, timeout=TIMEOUT)
    except (
        rq.exceptions.ConnectionError, 
        rq.exceptions.ReadTimeout,
        rq.exceptions.RequestException
    ) as e:
        logger.error(f"Error in getDynamicForm: {str(e)}")
        raise
    else:
        if wqRequest.status_code != rq.codes.ok:
            logger.error(f"Status code {wqRequest.status_code} in getDynamicForm.")
        return wqRequest.json()

def generateTokenForSecure2(username: str, password: str) -> str | None:
    """ Call Secure2 to get user token for current session.

    Parameters
        - username {str}.
        - password {str} password of user.

    Returns
        {str} token for the session or None if exception raise or status
        code is different from 200.
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

def fetchAgiReports(reportId: int, username: str, password: str) -> rq.Response | None:
    """ Call Secure2 to get AGI reports.

    Parameters
        - reportId {int} the ID of the report to fetch.
        - username {str} tue username of the user.
        - password {str} the password of the user.

    Returns
        {requests.Response} fetched AGI report in CSV format or None if
        exception raise.
    """

    if username is None or password is None:
        username = Config.get_username()
        password = Config.get_password()
    token = generateTokenForSecure2(username, password)
    
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
        raise
    else:
        if response.status_code == rq.codes.unauthorized:
            logger.warning("Token expired, refreshing...")
            currentToken = generateTokenForSecure2(username, password)
            if currentToken:
                return fetchAgiReports(reportId)
            
            logger.warning("Unable to refresh token...")
        
        return response
