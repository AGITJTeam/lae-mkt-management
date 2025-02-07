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
from data.repository.stats_dash.dash_offices import dashOffices
from data.repository.stats_dash.dash_projections import dashProjections
from data.repository.stats_dash.gmb_calls import generateGmbCallsReport
from data.repository.stats_dash.ot_run import otRun
from data.repository.stats_dash.out_of_state import outOfState
from data.repository.stats_dash.top_carriers import topCarriers
from data.repository.stats_dash.dash_os import dashOs
from service.dynamic_form import generateDynamicFormDf
from logs.config import setupLogging

# ======================== OTHER PACKAGES ========================

from flask_jwt_extended import create_access_token, jwt_required, JWTManager
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import timedelta
from dotenv import load_dotenv
import os, logging

# ===================== FLASK CONFIGURATION ======================

setupLogging()
load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=9)
jwt = JWTManager(app)
CORS(app)

logger = logging.getLogger(__name__)

@app.route("/")
@jwt_required()
def home():
    return jsonify(f"Running on port 8000.")

# ======================== FLASK API ENDPOINTS ========================

@app.route("/Register/<string:username>", methods=["GET", "POST"])
def register(username: str):    
    token = create_access_token(identity=username)
    return jsonify({ "token": token })

@app.route("/ReceiptsPayroll", methods=["GET"])
@jwt_required()
def getDataBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getBetweenDates(start, end))

@app.route("/ReceiptsPayroll/<int:id>", methods=["GET"])
@jwt_required()
def getReceiptsByCustId(id: int):
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getByCustomerId(id))

@app.route("/Webquotes", methods=["GET"])
@jwt_required()
def getWebquotesFromDateRange():
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    webquotes = Webquotes()

    if start == None:
        return jsonify(webquotes.getPartialFromDateRange("2024-01-01", end))
    
    return jsonify(webquotes.getPartialFromDateRange(start, end))

@app.route("/Webquotes/Details", methods=["GET"])
@jwt_required()
def getWebquotesDetails():
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    webquotes = Webquotes()
    
    return jsonify(webquotes.getWebquotesFromDateRange(start, end))

@app.route("/DynamicForms", methods=["GET"])
@jwt_required()
def getHomeOwnersDF():
    return jsonify(generateDynamicFormDf())

@app.route("/Lae", methods=["GET"])
@jwt_required()
def getLaeBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    lae = LaeData()
    return jsonify(lae.getBetweenDates(start=start, end=end))

@app.route("/Customers", methods=["GET"])
@jwt_required()
def getAllCustomers():
    customers = Customers()
    return jsonify(customers.getAllData())

@app.route("/Customers/<int:id>", methods=["GET"])
@jwt_required()
def getCustomerById(id: int):
    customers = Customers()
    return jsonify(customers.getById(id))

@app.route("/Employees", methods=["GET"])
@jwt_required()
def getAllEmployees():
    employees = Employees()
    return jsonify(employees.getAllData())

@app.route("/FiduciaryReport", methods=["GET"])
@jwt_required()
def getReceiptsBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    receipts = Receipts()
    return jsonify(receipts.getBetweenDates(start, end))

@app.route("/RegionalsOffices", methods=["GET"])
@jwt_required()
def getRegionalsByOffice():
    offices = Compliance()
    return jsonify(offices.getRegionalsByOffices())

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
def postGmbCallsReport():
    start = request.form.get("startDate")
    end = request.form.get("endDate")
    file = request.files.get("gmbCallsFile")

    try:
        excelReport = generateGmbCallsReport(start, end, file)
    except Exception as e:
        return jsonify({"error": f"An error occurred while generating the report: {str(e)}"}), 500
    else:
        dictReport = excelReport.to_dict(orient="records")
        return jsonify(dictReport), 200

# ========================= FLASK EXECUTER =======================

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
