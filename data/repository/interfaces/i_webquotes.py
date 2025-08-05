class IWebquotes:
    def getPartialFromDateRange(self, start: str, end: str) -> list[dict]:
        pass
    def getWebquotesFromDateRange(self, start: str, end: str) -> list[dict]:
        pass
    def getExtendedFromDateRange(self, start: str, end: str) -> list[dict]:
        pass
    def getLastRecord(self) -> list[dict]:
        pass    
    def deleteLastMonthData(self, start: str, end: str) -> None:
        pass
