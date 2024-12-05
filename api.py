from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.customers_repo import Customers
from data.repository.calls.lae_data_repo import LaeData
from data.repository.calls.policies_details_repo import PoliciesDetails
from data.repository.calls.webquotes_repo import Webquotes
from allowed_access import allowedIps
from flask import Flask, abort, jsonify, request

app = Flask(__name__)
 
@app.before_request
def restrictIp():
    clientIp = request.remote_addr
    if clientIp not in allowedIps:
        abort(403)  # Forbidden if IP is not allowed

@app.route('/')
def home():
    return "Server is running on port 5000"


@app.route("/ReceiptsPayroll", methods=["GET"])
def getDataBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getBetweenDates(start, end))

@app.route("/ReceiptsPayroll/<int:id>", methods=["GET"])
def getReceiptsByCustId(id: int):
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getByCustomerId(id))

@app.route("/Webquotes", methods=["GET"])
def getWebquotesFromDateRange():
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    webquotes = Webquotes()

    if start == None:
        return jsonify(webquotes.getPartialFromDateRange("2024-01-01", end))
    
    return jsonify(webquotes.getPartialFromDateRange(start, end))

@app.route("/Webquotes/Details", methods=["GET"])
def getWebquotesDetails():
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    webquotes = Webquotes()
    
    return jsonify(webquotes.getWebquotesFromDateRange(start, end))

@app.route("/Lae", methods=["GET"])
def getLaeBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    lae = LaeData()
    return jsonify(lae.getBetweenDates(start=start, end=end))

@app.route("/Customers/<int:id>", methods=["GET"])
def getCustomerById(id: int):
    customers = Customers()
    return jsonify(customers.getById(id))

@app.route("/Employees", methods=["GET"])
def getAllEmployees():
    employees = Employees()
    return jsonify(employees.getAllData())

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
