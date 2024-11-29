from data.repository.interfaces.i_webquotes import IWebquotes
from data.repository.calls.helpers import getData, parseWebquotesSubmissionDate, parseWebquoteSubmissionTime, executeOperation

class Webquotes(IWebquotes):
    """
    Handles every GET petition of webquotes table from database.

    Methods
        - getById {list[dict]} get webquote by id.
        - getPartialFromDateRange {list[dict]} get some webquotes colums
          from a date range
        - getFullFromDateRange {list[dict]} get all webquotes columns
          from a date range.
        - getWebquotesFromDateRange {list[dict]} get almost every webquote
          column from a date range.
        - getLastRecord {list[dict]} get the last date from 'date' column.
        - deleteLastMonthData execute DELETE operation that erase rows
          between a date range.
    """

    def getById(self, id: int) -> list[dict]:
        """
        Parameters
            id {id} the id of the webquote.
        """
        query = f"SELECT * FROM webquotes WHERE id == {id};"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime
    
    def getPartialFromDateRange(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT name, email, phone, submission_date, status, agent, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate

    def getFullFromDateRange(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT * FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime

    def getWebquotesFromDateRange(self, start: str, end: str) -> list[dict]:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"SELECT name, email, phone, submission_date, birthday, model_year, make, model, status, agent, state, marital_status, gender, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate
    
    def getLastRecord(self) -> list[dict]:
        query = "SELECT date FROM webquotes ORDER BY date DESC LIMIT 1;"

        return getData(query)
    
    def deleteLastMonthData(self, start: str, end: str) -> None:
        """
        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """
        query = f"DELETE FROM webquotes WHERE date BETWEEN \'{start} 00:00:00.000000\' AND \'{end} 23:59:00.000000\';"

        executeOperation(query)