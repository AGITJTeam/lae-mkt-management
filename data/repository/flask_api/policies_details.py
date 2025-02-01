from data.repository.calls.helpers import postDataframeToDb
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
from service.policies_details import deleteColumnWithListValues, generatePoliciesDf
from service.policies_dtl import generatePoliciesDtlDf
from service.vehicles_insured import generateVehiclesDf
import pandas as pd

def updatePoliciesTables(start: str, end: str) -> None:
    """ Updates Policies and all sub-tables in vm.

    Parameters
        - start {str} beginning of the range.
        - end {str} end of the range.
    """

    receipts = ReceiptsPayroll()
    receiptsJson = receipts.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(receiptsJson)
    receiptsNoDuplicates = receiptsDf.drop_duplicates("customer_id")
    print("ReceiptsPayroll table generated..")

    policiesDf = generatePoliciesDf(receiptsNoDuplicates)
    policiesNoDuplicates = policiesDf.drop_duplicates("vehicle_insureds")

    vehiclesInsured = policiesNoDuplicates["vehicle_insureds"]
    vehiclesDf = generateVehiclesDf(vehiclesInsured)
    print("VehiclesInsured table generated")

    policiesDtl = policiesNoDuplicates["policies_dtl"]
    policiesDtlDf = generatePoliciesDtlDf(policiesDtl)
    print("PoliciesDTL table generated...")

    newPoliciesDf = deleteColumnWithListValues(policiesDf)
    print("Policies Details table generated...")

    postDataframeToDb(vehiclesDf, "vehicles_insured", "append")
    postDataframeToDb(policiesDtlDf, "policies_dtl", "append")
    postDataframeToDb(newPoliciesDf, "policies_details", "append")
    print("All tables posted...")