# ======================= INTERNAL PACKAGES ======================
from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.webquotes_repo import Webquotes
# from data.repository.calls.policies_details_repo import PoliciesDetails
from data.repository.calls.compliance_repo import Compliance
from data.repository.stats_dash.dash_carriers import dashCarriers
from data.repository.stats_dash.dash_final_sales import dashFinalSales
from data.repository.stats_dash.dash_offices import dashOffices
from data.repository.stats_dash.dash_projections import dashProjections
from data.repository.stats_dash.gmb_calls import generateGmbCallsReport
from data.repository.stats_dash.ot_run import otRun
from data.repository.stats_dash.out_of_state import outOfState
from data.repository.stats_dash.pvc import dashPvc
from data.repository.stats_dash.top_carriers import topCarriers
from data.repository.stats_dash.dash_os import dashOs
from data.repository.stats_dash.yelp_calls import generateYelpCallsReport
from data.repository.stats_dash.dialpad_calls import countDialpadCallsByDateRange
from service.dynamic_form import generateDynamicFormDf
from utils.validations import *
from logs.config import setupLogging
from config import Config
from redisCli import redisCli

# ======================== OTHER PACKAGES ========================
#
from flask_jwt_extended import create_access_token, jwt_required, JWTManager
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import logging, json

# ======================== FLASK CONFIGURATION ========================

setupLogging()
load_dotenv()

app = Flask(__name__)
app.config.from_object(Config)
jwt = JWTManager(app)
CORS(app)

logger = logging.getLogger(__name__)

# =========================== LAE ENDPOINTS ===========================

@app.route("/")
@jwt_required()
def home():
    return jsonify(f"Running on port 8000.")

@app.route("/Register/<string:username>", methods=["GET", "POST"])
def register(username: str):    
    token = create_access_token(identity=username)
    return jsonify({ "token": token })

@app.route("/ReceiptsPayroll", methods=["GET"])
@jwt_required()
def getDataBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"ReceiptsPayroll_{start}_{end}"

    # 3) Defines pre-made Redis keys and validators.
    validators = {
        "ReceiptsPayrollCurrentMonth": valCurrentMonthDates,
        "ReceiptsPayrollPreviousMonth": valPreviousMonthDates,
        "ReceiptsPayrollTwoMonths": valTwoMonthsDates
    }

    # 4) Check if the data is already in Redis.
    redisData = valPreMadeRedisData(start, end, redisKey, validators)
    if redisData:
        print("Webquotes recovered from Redis")
        return jsonify(redisData)

    # 5) Recover data from database.
    receiptsPayroll = ReceiptsPayroll()
    data = receiptsPayroll.getBetweenDates(start, end)

    # 6) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
    
    # 7) Return data.
    print("Receipts Payroll recovered from Database")
    return jsonify(data)

@app.route("/ReceiptsPayroll/<string:id>", methods=["GET"])
@jwt_required()
def getReceiptsByCustId(id: str):
    # 1) Validate parameter.
    if not validateStringNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid Customer ID"
        }), 400

    # 2) Recover data from database.
    receiptsPayroll = ReceiptsPayroll()
    data = receiptsPayroll.getByCustomerId(id)

    # 3) Return data.
    print("Recovered Receipts Payroll from Database")
    return jsonify(data)

@app.route("/Webquotes", methods=["GET"])
@jwt_required()
def getWebquotesFromDateRange():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
        
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"Webquotes_{start}_{end}"

    # 3) Defines pre-made Redis keys and validators.
    validators = {
        "WebquotesCurrentMonth": valCurrentMonthDates,
        "WebquotesPreviousMonth": valPreviousMonthDates,
        "WebquotesTwoMonths": valTwoMonthsDates,
        "AllWebquotes": valLastToCurrentYearDates
    }

    # 4) Check if the data is already in Redis.
    redisData = valPreMadeRedisData(start, end, redisKey, validators)
    if redisData:
        print("Webquotes recovered from Redis")
        return jsonify(redisData)

    # 5) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getPartialFromDateRange(start, end)

    # 6) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
    
    # 7) Return data.
    print("Webquotes recovered from Database")
    return jsonify(data)

@app.route("/Webquotes/Details", methods=["GET"])
@jwt_required()
def getWebquotesDetails():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"WebquotesDetails_{start}_{end}"

    # 3) Defines pre-made Redis keys and validators.
    validators = {
        "WebquotesDetailsCurrentMonth": valCurrentMonthDates,
        "WebquotesDetailsPreviousMonth": valPreviousMonthDates,
        "WebquotesDetailsTwoMonths": valTwoMonthsDates
    }

    # 4) Check if the data is already in Redis.
    redisData = valPreMadeRedisData(start, end, redisKey, validators)
    if redisData:
        print("Webquotes Details recovered from Redis")
        return jsonify(redisData)

    # 5) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getWebquotesFromDateRange(start, end)

    # 6) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 7) Return data.
    print("Webquotes Details recovered from Database")
    return jsonify(data)

@app.route("/DynamicForms", methods=["GET"])
@jwt_required()
def getHomeOwnersDF():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    
    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"DynamicForms_{start}_{end}"

    # 3) Defines pre-made Redis keys and validators.
    validators = {
        "DynamicFormsCurrentMonth": valCurrentMonthDates,
        "DynamicFormsPreviousMonth": valPreviousMonthDates,
        "DynamicFormsTwoMonths": valTwoMonthsDates
    }

    # 4) Check if the data is already in Redis.
    redisData = valPreMadeRedisData(start, end, redisKey, validators)
    if redisData:
        print("Dynamic Form recovered from Redis")
        return jsonify(redisData)

    # 5) Recover data from database.
    data = generateDynamicFormDf(start, end)

    # 6) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 7) Return data.
    print("Dynamic Form recovered from Database")
    return jsonify(data)

@app.route("/Lae", methods=["GET"])
@jwt_required()
def getLaeBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    
    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Recover data from database.
    lae = LaeData()
    data = lae.getBetweenDates(start=start, end=end)

    # 3) Return data.
    print("LAE Data recovered from Database")
    return jsonify(data)

@app.route("/Customers", methods=["GET"])
@jwt_required()
def getAllCustomers():
    # 1) Check if the data is already in Redis.
    redisKey = "AllCustomers"
    if redisCli.get(redisKey):
        print("All Customers recovered from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    customers = Customers()
    data = customers.getAllData()

    # 3) Save data in Redis.
    expirationTime = 60*60*10
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 4) Return data.
    print("All Customers recovered from Database")
    return jsonify(data)

@app.route("/Customers/<string:id>", methods=["GET"])
@jwt_required()
def getCustomerById(id: str):
    # 1) Validate parameter.
    if not validateStringNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid Customer ID"
        }), 400

    # 2) Recover data from database.
    customers = Customers()
    data = customers.getById(id)

    # 3) Return data.
    print("Customer recovered from Database")
    return jsonify(data)

@app.route("/Employees", methods=["GET"])
@jwt_required()
def getAllEmployees():
    # 1) Check if the data is already in Redis.
    redisKey = "AllEmployees"
    if redisCli.get(redisKey):
        print("All Employees recovered from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    employees = Employees()
    data = employees.getAllData()

    # 3) Save data in Redis.
    expirationTime = 60*60*10
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 4) Return data.
    print("All Employees recovered from Database")
    return jsonify(data)

@app.route("/FiduciaryReport", methods=["GET"])
@jwt_required()
def getReceiptsBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"Receipts_{start}_{end}"

    # 3) Defines pre-made Redis keys and validators.
    validators = {
        "ReceiptsCurrentMonth": valCurrentMonthDates,
        "ReceiptsPreviousMonth": valPreviousMonthDates
    }

    # 4) Check if the data is already in Redis.
    redisData = valPreMadeRedisData(start, end, redisKey, validators)
    if redisData:
        print("Receipts recovered from Redis")
        return jsonify(json.loads(redisData))

    # 5) Recover data from database.
    receipts = Receipts()
    data = receipts.getBetweenDates(start, end)

    # 6) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 7) Return data.
    print("Receipts recovered from Database")
    return jsonify(data)

@app.route("/RegionalsOffices", methods=["GET"])
@jwt_required()
def getRegionalsByOffice():
    # 1) Check if the data is already in Redis.
    redisKey = "RegionalsOfficesReport"
    if redisCli.get(redisKey):
        print("Recovered Regional Offices from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    offices = Compliance()
    data = offices.getRegionalsByOffices()

    # 3) Defines expiration time and save Redis key.
    expirationTime = 60*60*10
    redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)

    # 4) Return data.
    print("Recovered Regional Offices from Database")
    return jsonify(data)

@app.route("/CountDialpadCalls", methods=["GET"])
@jwt_required()
def countDialpadCalls():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Defines Redis key with date parameters.
    redisKey = f"DialpadCalls_{start}_{end}"

    # 3) Check if the data is already in Redis.
    if valCurrentMonthDates(start, end):
        redisKey = "DialpadCallsCurrentMonth"
        print("Dialpad Calls count recovered from Redis")
        allCalls = json.loads(redisCli.hget(redisKey, "allCalls"))
        uniqueCalls = json.loads(redisCli.hget(redisKey, "uniqueCalls"))

        return jsonify({
            "allCalls": allCalls,
            "uniqueCalls": uniqueCalls
        }), 200

    if valPreviousMonthDates(start, end):
        redisKey = "DialpadCallsPreviousMonth"
        print("Dialpad Calls count recovered from Redis")
        allCalls = json.loads(redisCli.hget(redisKey, "allCalls"))
        uniqueCalls = json.loads(redisCli.hget(redisKey, "uniqueCalls"))

        return jsonify({
            "allCalls": allCalls,
            "uniqueCalls": uniqueCalls
        }), 200

    if valTwoMonthsDates(start, end):
        redisKey = "DialpadCallsTwoMonths"
        print("Dialpad Calls count recovered from Redis")
        allCalls = json.loads(redisCli.hget(redisKey, "allCalls"))
        uniqueCalls = json.loads(redisCli.hget(redisKey, "uniqueCalls"))

        return jsonify({
            "allCalls": allCalls,
            "uniqueCalls": uniqueCalls
        }), 200

    if redisCli.get(redisKey):
        print("Dialpad Calls count recovered from Redis")
        allCalls = json.loads(redisCli.hget(redisKey, "allCalls"))
        uniqueCalls = json.loads(redisCli.hget(redisKey, "uniqueCalls"))

        return jsonify({
            "allCalls": allCalls,
            "uniqueCalls": uniqueCalls
        }), 200

    # 4) Recover data from database.
    allCalls, uniqueCalls = countDialpadCallsByDateRange(start, end)
    
    # 5) Saves new Redis key.
    redisCli.hset(
        name=redisKey,
        mapping={
            "allCalls": allCalls,
            "uniqueCalls": uniqueCalls
        }
    )
    
    # 6) Return data.
    return jsonify({
        "allCalls": allCalls,
        "uniqueCalls": uniqueCalls
    }), 200

# ======================== STATS DASH ENDPOINTS =======================

@app.route("/StatsDash/User/<string:username>", methods=["GET"])
@jwt_required()
def getUserData(username: str):
    # 1) Validate parameter.
    if not username.isalpha():
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid username format"
        }), 400

    try:
        # 2) Recover data from database.
        compliance = Compliance()
        response = compliance.searchUser(username)
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while searching the user: {str(e)}"
        }), 500
    
    # 3) Prepare data and return it.
    if response:
        userData = response[0]
    else:
        userData = {}

    return jsonify(userData), 200

@app.route("/StatsDash/User", methods=["POST"])
@jwt_required()
def postUser():
    # 1) Recover parameters from request.
    data = request.json
    fullname = data["fullname"]
    username = data["username"]
    location = data["location"]
    password = data["password"]
    email = data["email"]
    hired = data["hired"]
    position = data["position"]

    # 2) Validate parameters.
    alphaValidations = [
        { "fullname": fullname, "key": "fullname" },
        { "location": location, "key": "location" }
    ]

    for i, val in enumerate(alphaValidations):
        key  = val["key"]
        if not val[key].replace(" ", "").isalpha():
            return jsonify({
                "status": 400,
                "label": "Bad request",
                "error": f"{key} must contain only caracteres and spaces"
            }), 400

    if not validateEmail(email):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid email format"
        }), 400

    if not validateStringDate(hired):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    try:
        # 3) Recover positions and usernames from database and validate position.
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]
        usernames = [username["username"] for username in compliance.getAllUsernames()]

        if username in usernames:
            return jsonify({
                "status": 409,
                "label": "Bad request",
                "error": f"Username '{username}' already exist"
            }), 409

        if not position in positions:
            return jsonify({
                "status": 400,
                "label": "Bad request",
                "error": f"Position '{position}' do not exist"
            }), 400
    except Exception:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": "An error occurred while getting data for the user"
        }), 500
    
    try:
        # 4) Insert user in database.
        userCreated = compliance.insertUser(fullname, username, password, email, position, location, hired)
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while inserting the user: {str(e)}"
        }), 500
    
    if not userCreated:
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "User not created"
        }), 400
    
    return jsonify({
        "status": 201,
        "label": "OK",
        "message": f"User created successfully"
    }), 201

@app.route("/StatsDash/OtReports/Names", methods=["GET"])
@jwt_required()
def getOtReportByName():
    test = redisCli.ping()
    print(test)
    
    # 1) Check if the data is already in Redis.
    redisKey = "OtReportsNames"
    if redisCli.get(redisKey):
        return jsonify(json.loads(redisCli.get(redisKey)))

    try:
        # 2) Recover data from database.
        compliance = Compliance()
        response = compliance.getOtReportsNames()
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while fetching the report names: {str(e)}"
        }), 500

    # 3) Validate if db response has data.
    if len(response) == 0:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": "No OT Reports found"
        }), 404

    # 4) Defines expiration time and save Redis key.
    expirationTime = 60*60*3
    redisCli.set(name=redisKey, value=json.dumps(obj=response, default=str), ex=expirationTime)

    # 5) Return data.
    return jsonify(response), 200

@app.route("/StatsDash/OtReports/ID", methods=["GET"])
@jwt_required()
def getOtReportIdByName():
    # 1) Recover parameters from request.
    reportName = request.args.get("reportName")

    try:
        # 2) Recover data from database.
        compliance = Compliance()
        response = compliance.getOtReportIdByName(reportName)
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while fetching the report: {str(e)}"
        }), 500

    # 3) Validate if db response has data.
    if len(response) == 0:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": f"OT Report {reportName} do not exists"
        }), 404

    # 4) Return data.
    id = response[0]
    return jsonify(id), 200

@app.route("/StatsDash/OtReports/<string:id>", methods=["GET"])
@jwt_required()
def getOtReport(id: str):
    # 1) Validate parameter.
    if not validateStringNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid report ID"
        }), 400

    try:
        # 2) Recover data from database.
        compliance = Compliance()
        dateCreated, sales, weekSales = compliance.getOtReportById(id)
    except Exception:
        logger.error("An error occurred while processing the Projections Dash data")
        return jsonify({}), 500

    # 4) Return data.
    return jsonify({
        "data": sales,
        "weekdata": weekSales,
        "created": dateCreated,
    }), 200

@app.route("/StatsDash/OtReports", methods=["POST"])
@jwt_required()
def postOtReport():
    # 1) Recover parameters from request.
    data = request.json
    startDate = data["startDate"]
    endDate = data["endDate"]
    username = data["username"]
    encryptedPassword = data["password"]
    reportName = data["reportName"]
    dashUsername = data["dashUsername"]

    # 2) Validate string dates.
    if not validateStringDate(startDate) or not validateStringDate(endDate):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    otReportsNames = None
    compliance = Compliance()

    # 3) Recover Ot Reports names from Redis or database.
    if redisCli.get("OtReportsNames"):
        otReportsNames = json.loads(redisCli.get("OtReportsNames"))
    else:
        try:
            otReportsNames = compliance.getOtReportsNames()
        except Exception as e:
            return jsonify({
                "status": 500,
                "label": "Internal server error",
                "error": f"An error occurred while getting data for the report: {str(e)}"
            }), 500

    # 4) Recover usernames from database.
    try:
        usernames = compliance.getAllUsernames()
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while getting data for the report: {str(e)}"
        }), 500

    # 5) Validate if report exists and if username exists.
    reportNames = [reportName["report_name"] for reportName in otReportsNames]
    if reportName in reportNames:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": f"OT Report '{reportName}' already exists"
        }), 404

    if len(usernames) == 0:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": "No users found in database"
        }), 404

    if dashUsername not in [username["username"] for username in usernames]:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": f"User '{dashUsername}' not found in database"
        }), 404

    # 6) Save the report in database.
    try:
        return otRun(
            start=startDate,
            end=endDate,
            username=username,
            encryptedPassword=encryptedPassword,
            reportName=reportName,
            dashUsername=dashUsername
        )
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while posting the report: {str(e)}"
        }), 500

@app.route("/StatsDash/OtReports/<string:id>", methods=["DELETE"])
@jwt_required()
def delOtReport(id: str):
    # 1) Validate parameter.
    if not validateStringNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid report ID"
        }), 400

    try:
        # 2) Delete the report.
        compliance = Compliance()
        deleted = compliance.delOtReport(id)
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while deleting the report: {str(e)}"
        }), 500

    # 3) Validate if report was deleted.
    if not deleted:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": f"Report with id {id} not found"
        }), 404

    # 4) Return success message.
    return jsonify({
        "status": 200,
        "label": f"OK",
        "message": f"Report with id {id} deleted successfully"
    }), 200

@app.route("/StatsDash/FinalSales", methods=["GET"])
@jwt_required()
def processFinalSales():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    yesterday = request.args.get("yesterday")

    if not validateStringDate(start) or not validateStringDate(end) or not validateStringDate(yesterday):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Generate Final Sales data with parameters.
    try:
        yesterdayData, lastWeekData = dashFinalSales(start, end, yesterday)
    except Exception:
        logger.error(f"An error occurred while processing the Final Sales data")
        return jsonify({}), 500

    # 3) Return data.
    return jsonify({
        "yesterdayData": yesterdayData,
        "lastWeekData": lastWeekData
    }), 200

@app.route("/StatsDash/Pvc", methods=["GET"])
@jwt_required()
def processPvc():
    # 1) Generate PVC data.
    try:
        yesterdayData, lastWeekData = dashPvc()
    except Exception:
        logger.error(f"An error occurred while processing the Pvc data")
        return jsonify({}), 500

    # 2) Return data.
    return jsonify({
        "yesterdayData": yesterdayData,
        "lastWeekData": lastWeekData
    }), 200

@app.route("/StatsDash/DashProjections", methods=["GET"])
@jwt_required()
def processDashProjections():
    # 1) Retrieve parameters and validate fullname.
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    if not fullname.replace(" ", "").isalpha():
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Fullname must contain only caracteres and spaces"
        }), 400

    try:
        # 2) Recover positions from database and validate position.
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]
        
        if not position in positions:
            return jsonify({
                "status": 404,
                "label": "Not found",
                "error": f"{position} position do not exist"
            }), 404
    except Exception:
        logger.error("An error occurred while getting data for Projections Report")
        return jsonify({}), 500

    # 3) Generate Projections data with parameters.
    try:
        companySales, totalSums, startDate, endDate = dashProjections(position, fullname)
    except Exception:
        logger.error(f"An error occurred while processing the Projections Dash data")
        return jsonify({}), 500

    # 4) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums,
        "startDate": startDate,
        "endDate": endDate
    }), 200

@app.route("/StatsDash/DashOffices", methods=["GET"])
@jwt_required()
def processDashOffices():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    reportId = request.args.get("reportId")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    if not validatePayrollReportId(reportId):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid report ID"
        }), 400

    # 2) Generate Offices data with parameters.
    try:
        companySales, totalSums = dashOffices(start, end, reportId)
    except Exception:
        logger.error(f"An error occurred while processing the Offices Dash data")
        return jsonify({}), 500

    # 3) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/DashOs", methods=["GET"])
@jwt_required()
def processDashOs():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Generate Online Sales data with parameters.
    try:
        companySales, totalSums = dashOs(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Online Sales Dash data")
        return jsonify({}), 500

    # 3) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/DashCarriers", methods=["GET"])
@jwt_required()
def processDashCarriers():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    if not fullname.replace(" ", "").isalpha():
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Fullname must contain only caracteres and spaces"
        }), 400

    try:
        # 2) Recover positions from database and validate position.
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]

        if not position in positions:
            return jsonify({
                "status": 404,
                "label": "Not found",
                "error": f"{position} position do not exist"
            }), 404
    except Exception:
        logger.error("An error occurred while getting data for Carriers Report")
        return jsonify({}), 500

    # 3) Generate Carriers data with parameters.
    try:
        companySales, totalSums = dashCarriers(start, end, position, fullname)
    except Exception:
        logger.error(f"An error occurred while processing the Carrier Dash data")
        return jsonify({}), 500

    # 4) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/TopCarriers", methods=["GET"])
@jwt_required()
def processTopCarriers():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Generate Top Carriers data with parameters.
    try:
        companySales, companySalesOffices, totalSums = topCarriers(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Top Carrier Dash data")
        return jsonify({}), 500

    # 3) Return data.
    return jsonify({
        "daily_data": companySales,
        "daily_data_office": companySalesOffices,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/OutOfState", methods=["GET"])
@jwt_required()
def processOutOfState():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Generate Out Of State data with parameters.
    try:
        dailyData = outOfState(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Out Of State Dash data")
        return jsonify({}), 500

    return jsonify({
        "daily_data": dailyData
    }), 200

@app.route("/StatsDash/GmbReports", methods=["GET"])
@jwt_required()
def getGmbCallsReport():
    # 1) Retrieve parameters and validate them.
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    file = request.files.get("gmbCallsFile")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    if not validateTxtFile(file):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid format or file must be a .txt file"
        }), 400

    # 2) Generate GMB Calls data with parameters.
    try:
        excelReport = generateGmbCallsReport(start, end, file)
    except Exception:
        logger.error(f"An error occurred while generating the report.")
        return jsonify({}), 500

    # 3) Return data.
    dictReport = excelReport.to_dict(orient="records")
    return jsonify(dictReport), 200

@app.route("/StatsDash/YelpReports", methods=["GET"])
@jwt_required()
def getYelpCallsReport():
    # 1) Retrieve parameters and validate them.
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    yelpCalls = request.files.get("yelpCallsFile")
    yelpCodes = request.files.get("yelpCodesFile")

    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    if not validateXlsFile(yelpCalls):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid format or file must be a .xls file"
        }), 400

    if not validateXlsxFile(yelpCodes):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid format or file must be a .xlsx file"
        }), 400

    # 2) Generate Yelp Calls data with parameters.
    try:
        yelpReport = generateYelpCallsReport(start, end, yelpCalls, yelpCodes)
    except Exception as e:
        logger.error(f"An error occurred while generating the report.")
        return jsonify({}), 500

    # 3) Return data.
    dictReport = yelpReport.to_dict(orient="records")
    return jsonify(dictReport), 200

# ========================= FLASK EXECUTER =======================

if __name__ == "__main__":
    app.run(host="0.0.0.0", threaded=True)
