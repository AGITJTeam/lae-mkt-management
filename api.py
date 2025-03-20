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

# ======================== OTHER PACKAGES ========================

from flask_jwt_extended import create_access_token, jwt_required, JWTManager
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv
import logging, redis, json

# ======================== FLASK CONFIGURATION ========================

setupLogging()
load_dotenv()

app = Flask(__name__)
app.config.from_object(Config)
jwt = JWTManager(app)
#CORS(app)

logger = logging.getLogger(__name__)

redisCli = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ======================== FLASK API ENDPOINTS ========================

@app.route("/")
@jwt_required()
def home():
    return jsonify(f"Running on port 8000.")

@app.route("/Register/<string:username>", methods=["GET", "POST"])
def register(username: str):    
    token = create_access_token(identity=username)
    return jsonify({ "token": token })

@app.route("/Test", methods=["GET"])
def tester():
    return jsonify({ "hola": "hola" })

@app.route("/ReceiptsPayroll", methods=["GET"])
@jwt_required()
def getDataBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    
    if not validateStringDate(start) or not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"ReceiptsPayroll_{start}_{end}"
    
    if redisCli.get(redisKey):
        print("Recovered Receipts Payroll from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    receiptsPayroll = ReceiptsPayroll()
    data = receiptsPayroll.getBetweenDates(start, end)
    
    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))
    
    # 5) Return data.
    print("Recovered Receips Payroll from Database")
    return jsonify(data)

@app.route("/ReceiptsPayroll/<string:id>", methods=["GET"])
@jwt_required()
def getReceiptsByCustId(id: str):
    # 1) Validate parameter.
    if not validateNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid Customer ID"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"ReceiptsPayroll_from_cust_id_{id}"
    if redisCli.get(redisKey):
        print("Recovered Receipts Payroll from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    receiptsPayroll = ReceiptsPayroll()
    data = receiptsPayroll.getByCustomerId(id)
    
    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered Receipts Payroll from Database")
    return jsonify(data)

@app.route("/Webquotes", methods=["GET"])
@jwt_required()
def getWebquotesFromDateRange():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")

    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"Webquotes_{start}_{end}"
    if redisCli.get(redisKey):
        print("Recovered Webquotes from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getPartialFromDateRange(start, end)
    
    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))
    
    # 5) Return data.
    print("Recovered Webquotes from Database")
    return jsonify(data)

@app.route("/Webquotes/Details", methods=["GET"])
@jwt_required()
def getWebquotesDetails():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    
    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"WebquotesDetails_{start}_{end}"
    if redisCli.get(redisKey):
        print("Recovered Webquotes Details from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    webquotes = Webquotes()
    data = webquotes.getWebquotesFromDateRange(start, end)
    
    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered Webquotes Detailsfrom Database")
    return jsonify(data)

@app.route("/DynamicForms", methods=["GET"])
@jwt_required()
def getHomeOwnersDF():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    
    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"DynamicForms_{start}_{end}"
    if redisCli.get(redisKey):
        print("Recovered Dynamic Form from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    data = generateDynamicFormDf(start, end)

    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered Dynamic Form from Database")
    return jsonify(data)

@app.route("/Lae", methods=["GET"])
@jwt_required()
def getLaeBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    
    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"Lae_{start}_{end}"
    if redisCli.get(redisKey):
        print("Recovered LAE data from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    lae = LaeData()
    data = lae.getBetweenDates(start=start, end=end)

    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered LAE Data from Database")
    return jsonify(data)

@app.route("/Customers", methods=["GET"])
@jwt_required()
def getAllCustomers():
    # 1) Check if the data is already in Redis.
    redisKey = "AllCustomers"
    if redisCli.get(redisKey):
        print("Recovered all Customers from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    customers = Customers()
    data = customers.getAllData()

    # 3) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 4) Return data.
    print("Recovered all Cusotmers from Database")
    return jsonify(data)

@app.route("/Customers/<string:id>", methods=["GET"])
@jwt_required()
def getCustomerById(id: str):
    # 1) Validate parameter.
    if not validateNumber(id):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid Customer ID"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"Customer_with_id_{id}"
    if redisCli.get(redisKey):
        print("Recovered Customer from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    customers = Customers()
    data = customers.getById(id)

    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered Customer from Database")
    return jsonify(data)

@app.route("/Employees", methods=["GET"])
@jwt_required()
def getAllEmployees():
    # 1) Check if the data is already in Redis.
    redisKey = "AllEmployees"
    if redisCli.get(redisKey):
        print("Recovered all Employees from Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 2) Recover data from database.
    employees = Employees()
    data = employees.getAllData()

    # 3) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 4) Return data.
    print("Recovered all Employeesfrom Database")
    return jsonify(data)

@app.route("/FiduciaryReport", methods=["GET"])
@jwt_required()
def getReceiptsBetweenDates():
    # 1) Retrieve parameters and validate them.
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    # 2) Check if the data is already in Redis.
    redisKey = f"FiduciaryReport_{start}_{end}"
    if redisCli.get(redisKey):
        print("Recovered Fiduciary Reportfrom Redis")
        return jsonify(json.loads(redisCli.get(redisKey)))

    # 3) Recover data from database.
    receipts = Receipts()
    data = receipts.getBetweenDates(start, end)

    # 4) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 5) Return data.
    print("Recovered Fiduciary Reportfrom Database")
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

    # 3) Save data in Redis.
    redisCli.set(redisKey, json.dumps(obj=data, default=str))

    # 4) Return data.
    print("Recovered Regional Offices from Database")
    return jsonify(data)


# ========= pendiente de ver como lo guardaré en redis

@app.route("/CountDialpadCalls", methods=["GET"])
@jwt_required()
def countDialpadCalls():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    
    if not validateStringDate(start) and not validateStringDate(end):
        return jsonify({
            "status": 400,
            "label": "Bad request",
            "error": "Invalid date format"
        }), 400

    allCalls, uniqueCalls = countDialpadCallsByDateRange(start, end)
    return jsonify({
        "allCalls": allCalls,
        "uniqueCalls": uniqueCalls
    }), 200

# ======================== STATS DASH ENDPOINTS =======================

@app.route("/StatsDash/User/<string:username>", methods=["GET"])
@jwt_required()
def getUserData(username: str):
    compliance = Compliance()
    response = compliance.searchUser(username)
    if response:
        userData = response[0]
    else:
        userData = {}

    return jsonify(userData)

@app.route("/StatsDash/User", methods=["POST"])
@jwt_required()
def postUser():
    compliance = Compliance()
    data = request.json

    fullname = data["fullname"]
    username = data["username"]
    password = data["password"]
    email = data["email"]
    position = data["position"]
    location = data["location"]
    hired = data["hired"]

    try:
        if compliance.insertUser(
            fullname,
            username,
            password,
            email,
            position,
            location,
            hired
        ):
            return jsonify({"message": f"User {username} created successfully."}), 200
        else:
            return jsonify({"error": f"User {username} already exists."}), 400
    except Exception as e:
        return jsonify({"error": f"An error occurred while posting the user: {str(e)}"}), 500

@app.route("/StatsDash/OtReports/Names", methods=["GET"])
@jwt_required()
def getOtReportByName():
    compliance = Compliance()

    try:
        response = compliance.getOtReportsNames()
    except Exception as e:
        return jsonify({"error": f"An error occurred while fetching the report names: {str(e)}"}), 500
    else:
        if len(response) == 0:
            return jsonify({"error": "No OT Reports found"}), 404

        return jsonify(response), 200

@app.route("/StatsDash/OtReports/ID", methods=["GET"])
@jwt_required()
def getOtReportIdByName():
    compliance = Compliance()
    reportName = request.args.get("reportName")

    try:
        response = compliance.getOtReportIdByName(reportName)
    except Exception as e:
        return jsonify({"error": f"An error occurred while fetching the report: {str(e)}"}), 500
    else:
        if len(response) == 0:
            return jsonify({"error": f"OT Report {reportName} do not exists"}), 404

        id = response[0]
        return jsonify(id), 200

@app.route("/StatsDash/OtReports/<int:id>", methods=["GET"])
@jwt_required()
def getOtReport(id: int):
    compliance = Compliance()

    try:
        dateCreated, sales, weekSales = compliance.getOtReportById(id)
    except Exception:
        logger.error("An error occurred while processing the Projections Dash data")
        return jsonify({}), 500
    else:
        return jsonify({
            "data": sales,
            "weekdata": weekSales,
            "created": dateCreated,
        }), 200

@app.route("/StatsDash/OtReports", methods=["POST"])
@jwt_required()
def postOtReport():
    data = request.json

    username = data["username"]
    password = data["password"]
    startDate = data["startDate"]
    endDate = data["endDate"]
    reportName = data["reportName"]
    dashUsername = data["dashUsername"]

    try:
        return otRun(
            start=startDate,
            end=endDate,
            username=username,
            encryptedPassword=password,
            reportName=reportName,
            dashUsername=dashUsername
        )
    except Exception as e:
        return jsonify({"error": f"An error occurred while posting the report: {str(e)}"}), 500

@app.route("/StatsDash/OtReports/<int:id>", methods=["DELETE"])
@jwt_required()
def delOtReport(id: int):
    compliance = Compliance()

    try:
        if compliance.delOtReport(id):
            return jsonify({"message": f"Report with id {id} deleted successfully."}), 200
        else:
            return jsonify({"error": f"Report with id {id} not found."}), 404
    except Exception as e:
        return jsonify({"error": f"An error occurred while deleting the report: {str(e)}"}), 500

@app.route("/StatsDash/FinalSales", methods=["GET"])
@jwt_required()
def processFinalSales():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    yesterday = request.args.get("yesterday")

    try:
        yesterdayData, lastWeekData = dashFinalSales(start, end, yesterday)
    except Exception:
        logger.error(f"An error occurred while processing the Final Sales data")
        return jsonify({}), 500
    else:
        return jsonify({
            "yesterdayData": yesterdayData,
            "lastWeekData": lastWeekData
        }), 200

@app.route("/StatsDash/Pvc", methods=["GET"])
@jwt_required()
def processPvc():
    try:
        #yesterdayData = dashPvc()
        yesterdayData, lastWeekData = dashPvc()
    except Exception:
        logger.error(f"An error occurred while processing the Pvc data")
        return jsonify({}), 500
    else:
        return jsonify({
            "yesterdayData": yesterdayData,
            "lastWeekData": lastWeekData
        }), 200

@app.route("/StatsDash/DashProjections", methods=["GET"])
@jwt_required()
def processDashProjections():
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    try:
        companySales, totalSums, startDate, endDate = dashProjections(position, fullname)
    except Exception:
        logger.error(f"An error occurred while processing the Projections Dash data")
        return jsonify({}), 500
    else:
        return jsonify({
            "daily_data": companySales,
            "total_data": totalSums,
            "startDate": startDate,
            "endDate": endDate
        }), 200

@app.route("/StatsDash/DashOffices", methods=["GET"])
@jwt_required()
def processDashOffices():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    reportId = request.args.get("reportId")

    try:
        companySales, totalSums = dashOffices(start, end, reportId)
    except Exception:
        logger.error(f"An error occurred while processing the Offices Dash data")
        return jsonify({}), 500
    else:
        return jsonify({"daily_data": companySales, "total_data": totalSums}), 200

@app.route("/StatsDash/DashOs", methods=["GET"])
@jwt_required()
def processDashOs():
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    try:
        companySales, totalSums = dashOs(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Online Sales Dash data")
        return jsonify({}), 500
    else:
        return jsonify({"daily_data": companySales, "total_data": totalSums}), 200

@app.route("/StatsDash/DashCarriers", methods=["GET"])
@jwt_required()
def processDashCarriers():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    try:
        companySales, totalSums = dashCarriers(start, end, position, fullname)
    except Exception:
        logger.error(f"An error occurred while processing the Carrier Dash data")
        return jsonify({}), 500
    else:
        return jsonify({"daily_data": companySales, "total_data": totalSums}), 200

@app.route("/StatsDash/TopCarriers", methods=["GET"])
@jwt_required()
def processTopCarriers():
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    try:
        companySales, companySalesOffices, totalSums = topCarriers(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Top Carrier Dash data")
        return jsonify({}), 500
    else:
        return jsonify({
            "daily_data": companySales,
            "daily_data_office": companySalesOffices,
            "total_data": totalSums
        }), 200

@app.route("/StatsDash/OutOfState", methods=["GET"])
@jwt_required()
def processOutOfState():
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    try:
        dailyData = outOfState(start, end)
    except Exception:
        logger.error(f"An error occurred while processing the Out Of State Dash data")
        return jsonify({}), 500
    else:
        return jsonify({"daily_data": dailyData}), 200

@app.route("/StatsDash/GmbReports", methods=["GET"])
@jwt_required()
def getGmbCallsReport():
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    file = request.files.get("gmbCallsFile")

    try:
        excelReport = generateGmbCallsReport(start, end, file)
    except Exception:
        logger.error(f"An error occurred while generating the report.")
        return jsonify({}), 500
    else:
        dictReport = excelReport.to_dict(orient="records")
        return jsonify(dictReport), 200

@app.route("/StatsDash/YelpReports", methods=["GET"])
@jwt_required()
def getYelpCallsReport():
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    yelpCalls = request.files.get("yelpCallsFile")
    yelpCodes = request.files.get("yelpCodesFile")

    try:
        yelpReport = generateYelpCallsReport(start, end, yelpCalls, yelpCodes)
    except Exception as e:
        logger.error(f"An error occurred while generating the report.")
        return jsonify({}), 500
    else:
        dictReport = yelpReport.to_dict(orient="records")
        return jsonify(dictReport), 200

# ========================= FLASK EXECUTER =======================

if __name__ == "__main__":
    app.run(host="0.0.0.0")
