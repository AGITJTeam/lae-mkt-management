from data.repository.interfaces.i_webquotes import IWebquotes
from data.repository.calls.helpers import getData, parseWebquotesSubmissionDate, parseWebquoteSubmissionTime, executeOperation

class Webquotes(IWebquotes):
    """
    Handles every petition of webquotes table from database.

    Methods
        - getById.
        - getPartialFromDateRange.
        - getFullFromDateRange.
        - getWebquotesFromDateRange.
        - getLastRecord.
        - deleteLastMonthData.
    """

    def getById(self, id):
        """ {list[dict]} get webquote by id.

        Parameters
            - id {int} the id of the webquote.
        """

        query = f"SELECT * FROM webquotes WHERE id == {id};"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime
    
    def getPartialFromDateRange(self, start, end):
        """ {list[dict]} get some webquotes colums from a date range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT name, email, phone, submission_date, status, agent, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate

    def getFullFromDateRange(self, start, end):
        """ {list[dict]} get all webquotes columns from a date range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT * FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime

    def getWebquotesFromDateRange(self, start, end):
        """ {list[dict]} get almost every webquote column from a
        date range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"SELECT name, email, phone, submission_date, birthday, model_year, make, model, status, agent, state, marital_status, gender, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate
    
    def getLastRecord(self):
        """ {list[dict]} get the last date from 'date' column. """

        query = "SELECT submission_date FROM webquotes ORDER BY submission_date DESC LIMIT 1;"

        return getData(query)
    
    def deleteLastMonthData(self, start, end):
        """ execute DELETE operation that erase rows between a
        date range.

        Parameters
            - start {str} the beginning of the range of dates.
            - end {str} the end of the range of dates.
        """

        query = f"DELETE FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"

        executeOperation(query)