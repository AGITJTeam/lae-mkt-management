from controllers.controller import fetchAgiReports
import pandas as pd, csv, io

def generateAgiReport(reportId: int, username: str = None, password: str = None) -> pd.DataFrame:
    """ Generates a DataFrame from Secure2 report.

    Parameters
        - reportId {int} id of the report.
        - username {str} username of Secure2 platform.
        - password {str} password of Secure2 platform.
    
    Returns
        {pandas.DataFrame} resulting DataFrame.
    """
    response = fetchAgiReports(reportId, username, password)
    reportDecoded = response.content.decode("utf-8")

    reportFile = io.StringIO(reportDecoded)
    csvReader = csv.reader(reportFile)
    payrollData = list(csvReader)
    payrollDf = pd.DataFrame(payrollData[1:], columns=payrollData[0])
    reportFile.close()

    return payrollDf
