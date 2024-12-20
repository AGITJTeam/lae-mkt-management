from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.policies_details_repo import PoliciesDetails
from data.repository.calls.webquotes_repo import Webquotes
from allowed_access import allowedIps
from flask import Flask, jsonify, request
from flask_jwt_extended import create_access_token, jwt_required, JWTManager
from dotenv import load_dotenv
from datetime import timedelta
import os

load_dotenv()
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=9)
jwt = JWTManager(app)

@app.route('/Register/<string:username>', methods=["GET", "POST"])
def register(username: str):    
    token = create_access_token(identity=username)
    return jsonify({ "token": token })

@app.route('/')
@jwt_required()
def home():
    return jsonify(f"Running on port 8000.")

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

if __name__ == "__main__":
    app.run(host="0.0.0.0")
