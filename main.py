# from data.repository.repository import generateAllTablesAndPostThem, receiptsPayrollDailyUpdate

# receiptsPayrollDailyUpdate()


from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll

t = ReceiptsPayroll()

print(t.getReceiptsByCustId(140733))