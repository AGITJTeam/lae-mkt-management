from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from data.repository.calls.customers_repo import Customers
from data.repository.calls.policies_details_repo import PoliciesDetails

from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route("/ReceiptsPayroll", methods=["GET"])
def getReceiptsPayroll():
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getAllData())

@app.route("/ReceiptsPayroll/<int:id>", methods=["GET"])
def getReceiptsByCustId(id: int):
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getReceiptsByCustId(id))

@app.route("/ReceiptsPayroll", methods=["GET"])
def getDataBetweenDates():
    start = request.args.get("startAt")
    end = request.args.get("endAt")
    receiptsPayroll = ReceiptsPayroll()
    return jsonify(receiptsPayroll.getDataBetweenDates(start, end))

if __name__ == '__main__':
    app.run(port=5000)
