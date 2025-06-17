# ======================= INTERNAL PACKAGES ======================
from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.webquotes_repo import Webquotes
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
from utils.transformators import formatWebquotesLanguage
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
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"ReceiptsPayroll_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines pre-made Redis keys and validators.
        validators = {
            "ReceiptsPayrollCurrentMonth": valCurrentMonthDates,
            "ReceiptsPayrollPreviousMonth": valPreviousMonthDates,
            "ReceiptsPayrollTwoMonths": valTwoMonthsDates
        }

        # Check if the data is already in Redis.
        redisData = valPreMadeRedisData(start, end, redisKey, validators)
        if redisData:
            print("Receipts Payroll recovered from Redis")
            return jsonify(redisData)

    # 4) Recover data from database.
    receiptsPayroll = ReceiptsPayroll()
    data = receiptsPayroll.getBetweenDates(start, end)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*3
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("Receipts Payroll saved in Redis, recovered from Database")
    else:
        print("Receipts Payroll not found in Redis, recovered from Database")
    
    # 6) Return data.
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
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"Webquotes_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines pre-made Redis keys and validators.
        validators = {
            "WebquotesCurrentMonth": valCurrentMonthDates,
            "WebquotesPreviousMonth": valPreviousMonthDates,
            "WebquotesTwoMonths": valTwoMonthsDates,
            "AllWebquotes": valLastToCurrentYearDates
        }

        # Check if the data is already in Redis.
        redisData = valPreMadeRedisData(start, end, redisKey, validators)
        if redisData:
            print("Webquotes recovered from Redis")
            return jsonify(redisData)

    # 4) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getPartialFromDateRange(start, end)

    formattedData = formatWebquotesLanguage(data)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*3
        redisCli.set(name=redisKey, value=json.dumps(obj=formattedData, default=str), ex=expirationTime)
        print("Webquotes saved in Redis, recovered from Database")
    else:
        print("Webquotes not found in Redis, recovered from Database")
    
    # 6) Return data.
    return jsonify(formattedData)

@app.route("/Webquotes/Details", methods=["GET"])
@jwt_required()
def getWebquotesDetails():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"WebquotesDetails_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines pre-made Redis keys and validators.
        validators = {
            "WebquotesDetailsCurrentMonth": valCurrentMonthDates,
            "WebquotesDetailsPreviousMonth": valPreviousMonthDates,
            "WebquotesDetailsTwoMonths": valTwoMonthsDates
        }

        # Check if the data is already in Redis.
        redisData = valPreMadeRedisData(start, end, redisKey, validators)
        if redisData:
            print("Webquotes Details recovered from Redis")
            return jsonify(redisData)

    # 4) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getWebquotesFromDateRange(start, end)

    formattedData = formatWebquotesLanguage(data)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*3
        redisCli.set(name=redisKey, value=json.dumps(obj=formattedData, default=str), ex=expirationTime)
        print("Webquotes Details saved in Redis, recovered from Database")
    else:
        print("Webquotes Details not found in Redis, recovered from Database")

    # 6) Return data.
    return jsonify(formattedData)

@app.route("/DynamicForms", methods=["GET"])
@jwt_required()
def getHomeOwnersDF():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"DynamicForms_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines pre-made Redis keys and validators.
        validators = {
            "DynamicFormsCurrentMonth": valCurrentMonthDates,
            "DynamicFormsPreviousMonth": valPreviousMonthDates,
            "DynamicFormsTwoMonths": valTwoMonthsDates
        }

        # Check if the data is already in Redis.
        redisData = valPreMadeRedisData(start, end, redisKey, validators)
        if redisData:
            print("Dynamic Form recovered from Redis")
            return jsonify(redisData)

    # 4) Recover data from database.
    data = generateDynamicFormDf(start, end)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*3
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("Dynamic Form saved in Redis, recovered from Database")
    else:
        print("Dynamic Form not found in Redis, recovered from Database")
        
    # 6) Return data.
    return jsonify(data)

@app.route("/Lae", methods=["GET"])
@jwt_required()
def getLaeBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

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
    redisKey = "AllCustomers"
    
    # 1) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.get(redisKey):
            print("All Customers recovered from Redis")
            return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    customers = Customers()
    data = customers.getAllData()
    
    # 3) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*10
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("All Customers saved in Redis, recovered from Database")
    else:
        print("All Customers not found in Redis, recovered from Database")

    # 4) Return data.
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
    redisKey = "AllEmployees"

    # 1) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.        
        if redisCli.get(redisKey):
            print("All Employees recovered from Redis")
            return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    employees = Employees()
    data = employees.getAllData()
    
    # 3) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*10
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("All Employees saved in Redis, recovered from Database")
    else:
        print("All Employees not found in Redis, recovered from Database")

    # 4) Return data.
    return jsonify(data)

@app.route("/FiduciaryReport", methods=["GET"])
@jwt_required()
def getReceiptsBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    # 2) Defines pre-made Redis key with date parameters.
    redisKey = f"Receipts_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines pre-made Redis keys and validators.
        validators = {
            "ReceiptsCurrentMonth": valCurrentMonthDates,
            "ReceiptsPreviousMonth": valPreviousMonthDates
        }

        # Check if the data is already in Redis.
        redisData = valPreMadeRedisData(start, end, redisKey, validators)
        if redisData:
            print("Receipts recovered from Redis")
            return jsonify(json.loads(redisData))

    # 4) Recover data from database.
    receipts = Receipts()
    data = receipts.getBetweenDates(start, end)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*3
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("Receipts saved in Redis, recovered from Database")
    else:
        print("Receipts not found in Redis, recovered from Database")

    # 6) Return data.
    return jsonify(data)

@app.route("/RegionalsOffices", methods=["GET"])
@jwt_required()
def getRegionalsByOffice():
    redisKey = "RegionalsOfficesReport"

    # 1) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.get(redisKey):
            print("Recovered Regional Offices from Redis")
            return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    offices = Compliance()
    data = offices.getRegionalsByOffices()

    # 3) Check if Redis container is working for saving data.
    if redisCli:
        # Defines expiration time and save Redis key.
        expirationTime = 60*60*10
        redisCli.set(name=redisKey, value=json.dumps(obj=data, default=str), ex=expirationTime)
        print("Recovered Regional Offices saved in Redis, recovered from Database")
    else:
        print("Recovered Regional Offices not found in Redis, recovered from Database")

    # 4) Return data.
    return jsonify(data)

@app.route("/CountDialpadCalls", methods=["GET"])
@jwt_required()
def countDialpadCalls():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    # 2) Defines Redis key, validators and resulting Json keys.
    redisKey = f"DialpadCalls_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        validators = {
            "DialpadCallsCurrentMonth": valCurrentMonthDates,
            "DialpadCallsPreviousMonth": valPreviousMonthDates,
            "DialpadCallsTwoMonths": valTwoMonthsDates
        }
        hashKeys = ["allCalls", "uniqueCalls"]

        # Check if the data is already in Redis.
        redisData = valPreMadeHashData(start, end, redisKey, validators, hashKeys)
        if redisData:
            print("Dialpad Calls count recovered from Redis")
            return jsonify(redisData), 200

    # 4) Recover data from database.
    allCalls, uniqueCalls = countDialpadCallsByDateRange(start, end)
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "allCalls": json.dumps(allCalls),
                "uniqueCalls": json.dumps(uniqueCalls)
            }
        )
        print("Dialpad Calls count saved in Redis, recovered from Database")
    else:
        print("Dialpad Calls count not found in Redis, recovered from Database")
        
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
    try:
        data = request.json
        fullname = data["fullname"]
        username = data["username"]
        location = data["location"]
        password = data["password"]
        email = data["email"]
        hired = data["hired"]
        position = data["position"]
    except Exception as e:
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing User body arguments"
        }), 400

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
    try:
        # 1) Recover data from database.
        compliance = Compliance()
        response = compliance.getOtReportsNames()
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while fetching the report names: {str(e)}"
        }), 500

    # 2) Validate if db response has data.
    if len(response) == 0:
        return jsonify({
            "status": 404,
            "label": "Not found",
            "error": "No OT Reports found"
        }), 404

    # 3) Return data.
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
            "error": f"OT Report '{reportName}' do not exists"
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
    
    redisKey = f"OtReport_{id}"
    
    # 2) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.hgetall(redisKey):
            print("Ot Report recovered from Redis")
            data = json.loads(redisCli.hget(redisKey, "data"))
            weekData = json.loads(redisCli.hget(redisKey, "weekdata"))
            created = json.loads(redisCli.hget(redisKey, "created"))

            return jsonify({
                "data": data,
                "weekdata": weekData,
                "created": created
            }), 200

    # 3) Recover data from database.
    try:
        compliance = Compliance()
        dateCreated, sales, weekSales = compliance.getOtReportById(id)
    except Exception as e:
        logger.error(f"An error occurred while getting Ot Reports names: {str(e)}")
        return jsonify({}), 500
    
    # 4) Check if Redis container is working for saving data.
    if redisCli:
        # Save to Redis if available.
        redisCli.hset(
            redisKey,
            mapping={
                "data": json.dumps(sales),
                "weekdata": json.dumps(weekSales),
                "created": json.dumps(dateCreated)
            }
        )
        print("Ot Report not found in Redis, recovered from Database")
    else:
        print("Ot Report recovered from Database")

    # 5) Return data.
    return jsonify({
        "data": sales,
        "weekdata": weekSales,
        "created": dateCreated,
    }), 200

@app.route("/StatsDash/OtReports", methods=["POST"])
@jwt_required()
def postOtReport():
    # 1) Recover parameters from request.
    try:
        data = request.json
        startDate = data["startDate"]
        endDate = data["endDate"]
        username = data["username"]
        encryptedPassword = data["password"]
        reportName = data["reportName"]
        dashUsername = data["dashUsername"]
    except Exception as e:
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing Ot Report body arguments"
        }), 400

    # 2) Validate string dates.
    if not valDateRanges(startDate, endDate):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    usernames = None
    compliance = Compliance()

    # 3.1) Check if Redis container is working.
    if redisCli:
        if redisCli.get("AllUsernames"):
            usernames = json.loads(redisCli.get("AllUsernames"))
    # 3.2) Recover usernames from Redis or database.
    else:
        try:
            usernames = compliance.getAllUsernames()
        except Exception as e:
            return jsonify({
                "status": 500,
                "label": "Internal server error",
                "error": f"An error occurred while getting data for the report: {str(e)}"
            }), 500

    # 4) Recover Ot Reports names from database.
    try:
        otReportsNames = compliance.getOtReportsNames()
    except Exception as e:
        return jsonify({
            "status": 500,
            "label": "Internal server error",
            "error": f"An error occurred while getting data for the report: {str(e)}"
        }), 500

    # 5) Validate if report exists and if username exists.
    if reportName in [reportName["report_name"] for reportName in otReportsNames]:
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
def genFinalSales():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    yesterday = request.args.get("yesterday")
    redisKey = f"FinalSales_{start}_{end}"

    if not valIsNotNone(start, end, yesterday):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end) or not validateStringDate(yesterday):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if Redis container is working for retrieving data.
    if redisCli:
        # Defines validators and resulting Json keys.
        validators = {
            "FinalSalesCurrentMonth": valCurrentMonthDates
        }
        hashKeys = ["yesterdayData", "lastWeekData"]

        # Check if the data is already in Redis.
        redisData = valPreMadeHashData(start, end, redisKey, validators, hashKeys)
        if redisData:
            print("Final Sales Report recovered from Redis")
            return jsonify(redisData), 200

    # 3) Generate Final Sales data with parameters.
    try:
        yesterdayData, lastWeekData = dashFinalSales(start, end, yesterday)
    except Exception:
        logger.error(f"An error occurred while processing the Final Sales data")
        return jsonify({}), 500
    
    # 4) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "yesterdayData": json.dumps(yesterdayData),
                "lastWeekData": json.dumps(lastWeekData)
            }
        )
        print("Final Sales Report calculated in backend and saved to Redis")
    else:
        print("Final Sales Report calculated in backend")

    # 5) Return data.
    return jsonify({
        "yesterdayData": yesterdayData,
        "lastWeekData": lastWeekData
    }), 200

@app.route("/StatsDash/Pvc", methods=["GET"])
@jwt_required()
def genPvc():
    redisKey = f"PvcCurrentMonth"

    # 1) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.hgetall(redisKey):
            print("Pvc Report recovered from Redis")
            yesterdayData = json.loads(redisCli.hget(redisKey, "yesterdayData"))
            lastWeekData = json.loads(redisCli.hget(redisKey, "lastWeekData"))

            return jsonify({
                "yesterdayData": yesterdayData,
                "lastWeekData": lastWeekData
            }), 200

    # 2) Generate PVC data.
    try:
        yesterdayData, lastWeekData = dashPvc()
    except Exception:
        logger.error(f"An error occurred while processing the Pvc data")
        return jsonify({}), 500

    # 3) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "yesterdayData": json.dumps(yesterdayData),
                "lastWeekData": json.dumps(lastWeekData)
            }
        )
        print("Pvc Report calculated in backend and saved to Redis")
    else:
        print("Pvc Report calculated in backend")

    # 4) Return data.
    return jsonify({
        "yesterdayData": yesterdayData,
        "lastWeekData": lastWeekData
    }), 200

@app.route("/StatsDash/DashProjections", methods=["GET"])
@jwt_required()
def genProjections():
    # 1) Retrieve parameters and validate fullname.
    position = request.args.get("position")
    fullname = request.args.get("fullname")
    # 2) Defines Redis key with date parameters.
    redisKey = f"Projections_{fullname.replace(' ', '_')}_{position}"

    if not valIsNotNone(position, fullname):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not fullname.replace(" ", "").isalpha():
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Fullname must contain only characters and spaces"
        }), 400

    # 3) Recover positions from database and validate position.
    try:
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]
        
        if position not in positions:
            return jsonify({
                "status": 404,
                "label": "Not found",
                "error": f"Position '{position}' does not exist"
            }), 404
    except Exception as e:
        logger.error(f"An error occurred while getting data for Projections Report: {str(e)}")
        return jsonify({}), 500

    # 4) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.hgetall(redisKey):
            print("Projections Report recovered from Redis")
            dailyData = json.loads(redisCli.hget(redisKey, "daily_data"))
            totalData = json.loads(redisCli.hget(redisKey, "total_data"))
            startDate = json.loads(redisCli.hget(redisKey, "startDate"))
            endDate = json.loads(redisCli.hget(redisKey, "endDate"))

            return jsonify({
                "daily_data": dailyData,
                "total_data": totalData,
                "startDate": startDate,
                "endDate": endDate
            }), 200

    # 5) Generate Projections data with parameters.
    try:
        companySales, totalSums, startDate, endDate = dashProjections(position, fullname)
    except Exception:
        logger.error("An error occurred while processing the Projections Dash data")
        return jsonify({}), 500

    # 6) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(companySales),
                "total_data": json.dumps(totalSums),
                "startDate": json.dumps(startDate),
                "endDate": json.dumps(endDate)
            }
        )
        print("Projections Report calculated in backend and saved to Redis")
    else:
        print("Projections Report calculated in backend")

    # 7) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums,
        "startDate": startDate,
        "endDate": endDate
    }), 200

@app.route("/StatsDash/DashOffices", methods=["GET"])
@jwt_required()
def genOffices():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    position = request.args.get("position")
    fullname = request.args.get("fullname")
    # 2) Defines Redis key, validators and resulting Json keys.
    redisKey = f"Offices_{start}_{end}_{fullname.replace(" ", "_")}_{position}"

    if not valIsNotNone(start, end, position, fullname):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
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

    # 3) Recover positions from database and validate position.
    try:
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]

        if not position in positions:
            return jsonify({
                "status": 404,
                "label": "Not found",
                "error": f"Position '{position}' do not exist"
            }), 404
    except Exception:
        logger.error("An error occurred while getting data for Offices Report")
        return jsonify({}), 500
    
    # 4) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.hgetall(redisKey):
            print("Offices Report recovered from Redis")
            dailyData = json.loads(redisCli.hget(redisKey, "daily_data"))
            totalData = json.loads(redisCli.hget(redisKey, "total_data"))

            return jsonify({
                "daily_data": dailyData,
                "total_data": totalData
            }), 200

    # 5) Generate Offices data with parameters.
    try:
        companySales, totalSums = dashOffices(start, end, fullname, position)
    except Exception:
        logger.error(f"An error occurred while processing the Offices Dash data")
        return jsonify({}), 500
    
    # 6) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(companySales),
                "total_data": json.dumps(totalSums)
            }
        )
        print("Offices Report calculated in backend and saved to Redis")
    else:
        print("Offices Report calculated in backend")

    # 7) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/DashOs", methods=["GET"])
@jwt_required()
def genOnlineSales():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    # 2) Defines Redis key, validators and resulting Json keys.
    redisKey = f"OnlineSales_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        validators = {
            "OnlineSalesCurrentMonth": valCurrentMonthDates
        }
        hashKeys = ["daily_data", "total_data"]

        # Check if the data is already in Redis.
        redisData = valPreMadeHashData(start, end, redisKey, validators, hashKeys)
        if redisData:
            print("Online Sales Report recovered from Redis")
            return jsonify(redisData), 200
    
    # 4) Generate Online Sales data with parameters.
    try:
        companySales, totalSums = dashOs(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Online Sales Dash data")
        return jsonify({}), 500
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(companySales),
                "total_data": json.dumps(totalSums)
            }
        )
        print("Online Sales Report calculated in backend and saved to Redis")
    else:
        print("Online Sales Report calculated in backend")
        
    # 6) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/DashCarriers", methods=["GET"])
@jwt_required()
def genCarriers():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    position = request.args.get("position")
    fullname = request.args.get("fullname")
    # 2) Defines Redis key with date parameters.
    redisKey = f"Carriers_{start}_{end}_{fullname.replace(" ", "_")}_{position}"

    if not valIsNotNone(start, end, position, fullname):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
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

    # 3) Recover positions from database and validate position.
    try:
        compliance = Compliance()
        positions = [position["position"] for position in compliance.getPositions()]

        if not position in positions:
            return jsonify({
                "status": 404,
                "label": "Not found",
                "error": f"Position '{position}' do not exist"
            }), 404
    except Exception:
        logger.error("An error occurred while getting data for Carriers Report")
        return jsonify({}), 500
    
    # 4) Check if Redis container is working for retrieving data.
    if redisCli:
        # Check if the data is already in Redis.
        if redisCli.hgetall(redisKey):
            print("Carriers Report recovered from Redis")
            dailyData = json.loads(redisCli.hget(redisKey, "daily_data"))
            totalData = json.loads(redisCli.hget(redisKey, "total_data"))

            return jsonify({
                "daily_data": dailyData,
                "total_data": totalData
            }), 200

    # 5) Generate Carriers data with parameters.
    try:
        companySales, totalSums = dashCarriers(start, end, position, fullname)
    except Exception:
        logger.error(f"An error occurred while processing the Carrier Dash data")
        return jsonify({}), 500
    
    # 6) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(companySales),
                "total_data": json.dumps(totalSums)
            }
        )
        print("Carriers Report calculated in backend and saved to Redis")
    else:
        print("Carriers Report calculated in backend")

    # 7) Return data.
    return jsonify({
        "daily_data": companySales,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/TopCarriers", methods=["GET"])
@jwt_required()
def genTopCarriers():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    # 2) Defines Redis key, validators and resulting Json keys.
    redisKey = f"TopCarriers_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400
    
    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        validators = {
            "TopCarriersCurrentMonth": valCurrentMonthDates
        }
        hashKeys = ["daily_data", "daily_data_office", "total_data"]

        # Check if the data is already in Redis.
        redisData = valPreMadeHashData(start, end, redisKey, validators, hashKeys)
        if redisData:
            print("Top Carriers Report recovered from Redis")
            return jsonify(redisData), 200

    # 4) Generate Top Carriers data with parameters.
    try:
        companySales, companySalesOffices, totalSums = topCarriers(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Top Carrier Dash data")
        return jsonify({}), 500

    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(companySales),
                "daily_data_office": json.dumps(companySalesOffices),
                "total_data": json.dumps(totalSums)
            }
        )
        print("Top Carriers Report calculated in backend and saved to Redis")
    else:
        print("Top Carriers Report calculated in backend")

    # 6) Return data.
    return jsonify({
        "daily_data": companySales,
        "daily_data_office": companySalesOffices,
        "total_data": totalSums
    }), 200

@app.route("/StatsDash/OutOfState", methods=["GET"])
@jwt_required()
def genOutOfState():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    # 2) Defines Redis key, validators and resulting Json keys.
    redisKey = f"OutOfState_{start}_{end}"

    if not valIsNotNone(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 3) Check if Redis container is working for retrieving data.
    if redisCli:
        validators = {
            "OutOfStateCurrentMonth": valCurrentMonthDates,
        }
        hashKeys = ["daily_data"]

        # Check if the data is already in Redis.
        redisData = valPreMadeHashData(start, end, redisKey, validators, hashKeys)
        if redisData:
            print("Out Of State Report recovered from Redis")
            return jsonify(redisData), 200

    # 4) Generate Out Of State data with parameters.
    try:
        dailyData = outOfState(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Out Of State Dash data")
        return jsonify({}), 500
    
    # 5) Check if Redis container is working for saving data.
    if redisCli:
        # Saves new Redis key.
        redisCli.hset(
            name=redisKey,
            mapping={
                "daily_data": json.dumps(dailyData)
            }
        )
    else:
        print("Out Of State Report calculated in backend")

    # 6) Return data.
    return jsonify({
        "daily_data": dailyData
    }), 200

@app.route("/StatsDash/GmbReports", methods=["GET"])
@jwt_required()
def genGmbCallsReport():
    # 1) Retrieve parameters and validate them.
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    file = request.files.get("gmbCallsFile")

    if not valIsNotNone(start, end, file):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
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
def genYelpCallsReport():
    # 1) Retrieve parameters and validate them.
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    yelpCalls = request.files.get("yelpCallsFile")
    yelpCodes = request.files.get("yelpCodesFile")

    if not valIsNotNone(start, end, yelpCalls, yelpCodes):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Missing arguments"
        }), 400

    if not valDateRanges(start, end):
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
