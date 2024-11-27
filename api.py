from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.customers_repo import Customers
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
    return jsonify(receiptsPayroll.getDataBetweenDates(start, end))

@app.route("/ReceiptsPayroll/<int:id>", methods=["GET"])
def getReceiptsByCustId(id: int):
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getReceiptsByCustId(id))

@app.route("/Webquotes", methods=["GET"])
def getWebquotes():
    start = request.args.get("fromDate")
    end = request.args.get("toDate")
    webquotes = Webquotes()
    return jsonify(webquotes.getByBetweenDates(start, end))

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
