from data.repository.calls.employees_repo import Employees
from data.repository.calls.receipts_payroll_repo import ReceiptsPayroll
import pandas as pd

def fetchReceipts(start: str, end: str) -> pd.DataFrame:
    """ 
    
    Parameters
        - start {end} beginning of date range.
        - end {end} end of date range.
    Returns
        {pandas.DataFrame} 
    """
    
    receiptsPayroll = ReceiptsPayroll()
    response = receiptsPayroll.getBetweenDates(start, end)
    receiptsDf = pd.DataFrame(response)
    receiptsDf.rename(columns={"amount_ii": "gi"}, inplace=True)
    officeFiltered = filterOfficeRecColumn(receiptsDf)
    forFiltered = filterForColumn(officeFiltered)
    rangeDf = addEmailColumns(forFiltered)

    return rangeDf

# def generateReceiptsDf(start: str, end: str) -> pd.DataFrame:
#     """ Create Receipts DataFrame with renamed columns with API response.

#     Parameters
#         - start {end} beginning of date range.
#         - end {end} end of date range.

#     Returns
#         {pandas.DataFrame} resulting DataFrame.
#     """
#     receipts = []
#     receiptsJson = getReceipts(start, end)

#     if not receiptsJson:
#         raise Exception("There is no receipt yet...")
    
#     for receipt in receiptsJson:
#         for_value = receipt["for"]
#         receipt.pop("for")
#         receipt.update({"for_t": for_value})
#         receiptsModel = ReceiptsModel(**receipt)
#         receipts.append(receiptsModel)
    
#     receiptsDf = pd.DataFrame(receipts)
#     receiptsDf["date"] = pd.to_datetime(receiptsDf["date"])
#     receiptsDf.rename(columns={"for_t": "for"}, inplace=True)
    
#     return receiptsDf

def filterOfficeRecColumn(df: pd.DataFrame) -> pd.DataFrame:
    """ Filter 'office_rec' column from a DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to filter.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    UNWANTED_OFFICES = ["Training Center", "Rancho","Refunds"]

    filteredDf = df[~df["office_rec"].str.contains("|".join(UNWANTED_OFFICES))]

    return filteredDf

def filterForColumn(df: pd.DataFrame) -> pd.DataFrame:
    """ Filter 'for' column from a DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to filter.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    FILTERS = {
        "bf_inv": "BF",
        "pay_inv": "Payment Fee",
        "invoice_inv": "Invoice",
        "NB_inv": "NewB",
        "Dis_inv": "Discount",
        "Imm_inv": "Immigration"
    }

    filteredForDf = {key: df[df["for"].str.contains(value)].copy() for key, value in FILTERS.items()}
    filteredForDf["NB_inv"]["gi"] = 0
    rangeDf = pd.concat(filteredForDf.values(), ignore_index=True)

    return rangeDf

def addEmailColumns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add 'usr_email' and 'csr_email' column to DataFrame.

    Parameters
        - df {pandas.DataFrame} DataFrame to filter.
        
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    employeesDf = employeesDfToLower()

    usernameAndEmail = { row["username"]: row["email_work"] for index, row in employeesDf.iterrows() }
    df["usr_email"] = df["usr"].map(usernameAndEmail).fillna("No Email")
    df["csr_email"] = df["csr"].map(usernameAndEmail).fillna("No Email")

    return df

def employeesDfToLower() -> pd.DataFrame:
    """ Transform Employees DataFrame columns to lowercase.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    employees = Employees()
    response = employees.getAllData()
    employeesDf = pd.DataFrame(response)

    for col in employeesDf.select_dtypes(include="object").columns:
        employeesDf[col] = employeesDf[col].apply(lambda x: x.lower() if isinstance(x, str) else x)
    
    return employeesDf