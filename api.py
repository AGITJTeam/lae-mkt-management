from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.receipts_repo import Receipts
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.policies_details_repo import PoliciesDetails
from data.repository.calls.webquotes_repo import Webquotes

from data.repository.calls.compliance_repo import Compliance
#from data.repository.stats_dash.register import processRegister

from data.repository.stats_dash.dash_carriers import dashCarriers
from data.repository.stats_dash.dash_offices import dashOffices
from data.repository.stats_dash.dash_projections import dashProjections
from data.repository.stats_dash.top_carriers import topCarriers
from data.repository.stats_dash.dash_os import dashOs

from logs.config import setupLogging

from flask_jwt_extended import create_access_token, jwt_required, JWTManager
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import timedelta
from dotenv import load_dotenv
import os

setupLogging()
load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=9)
jwt = JWTManager(app)
CORS(app)

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
    offices = Compliance
    return jsonify(offices.getRegionalsByOffices())

# ======================== STATS DASH ENDPOINTS =======================

# @app.route("/StatsDash/Users/<string:username>", methods=["GET"])
# @jwt_required()
# def processLogin(username: str):
#     compliance = Compliance()
#     return jsonify(compliance.searchUser(username))

# @app.route("/StatsDash/Users/", methods=["POST"])
# @jwt_required()
# def processLogin():
#     return jsonify(processRegister())

@app.route("/StatsDash/DashCarriers", methods=["POST"])
@jwt_required()
def processDashCarriers():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    return dashCarriers(start, end, position, fullname)

@app.route("/StatsDash/DashOffices", methods=["POST"])
@jwt_required()
def processDashOffices():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    reportId = request.args.get("reportId")

    return dashOffices(start, end, reportId)

@app.route("/StatsDash/DashProjections", methods=["POST"])
@jwt_required()
def processDashProjections():
    position = request.args.get("position")
    fullname = request.args.get("fullname")

    return dashProjections(position, fullname)

@app.route("/StatsDash/TopCarriers", methods=["POST"])
@jwt_required()
def processTopCarriers():
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    return topCarriers(start, end)

@app.route("/StatsDash/DashOs", methods=["POST"])
@jwt_required()
def processDashOs():
    start = request.args.get("startAt")
    end = request.args.get("endAt")

    return dashOs(start, end)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
