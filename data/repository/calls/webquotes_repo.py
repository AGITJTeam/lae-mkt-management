from data.repository.interfaces.i_webquotes import IWebquotes
from data.repository.calls.helpers import getData, parseWebquotesDates

class Webquotes(IWebquotes):
    def getById(self, id: int) -> list[dict]:
        query = f"SELECT * FROM webquotes WHERE id == {id};"

        return getData(query)

    def getByBetweenDates(self, start: str, end: str) -> list[dict]:
        query = f"SELECT * FROM webquotes WHERE submission_date BETWEEN \'{start}\' AND \'{end}\';"
        webquotes = getData(query)
        parsedWebquotes = parseWebquotesDates(webquotes)

        return parsedWebquotes

