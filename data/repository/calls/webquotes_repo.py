from data.repository.interfaces.i_webquotes import IWebquotes
from data.repository.calls.helpers import getData, parseWebquotesSubmissionDate, parseWebquoteSubmissionTime

class Webquotes(IWebquotes):
    def getById(self, id: int) -> list[dict]:
        query = f"SELECT * FROM webquotes WHERE id == {id};"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime
    
    def getPartialFromDateRange(self, start: str, end: str) -> list[dict]:
        query = f"SELECT name, email, phone, submission_date, status, agent, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate

    def getFullFromDateRange(self, start: str, end: str) -> list[dict]:
        query = f"SELECT * FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)
        parsedWebquoteTime = parseWebquoteSubmissionTime(parsedWebquoteDate)

        return parsedWebquoteTime

    def getWebquotesFromDateRange(self, start: str, end: str) -> list[dict]:
        query = f"SELECT name, email, phone, submission_date, birthday, model_year, make, model, status, agent, state, marital_status, gender, referer, campaign_id FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquoteDate = parseWebquotesSubmissionDate(webquotes)

        return parsedWebquoteDate