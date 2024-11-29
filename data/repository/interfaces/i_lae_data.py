class ILaeData:
    def getAllData(self) -> list[dict]:
        pass
    def getBetweenDates(self, start: str, end: str) -> list[dict]:
        pass
    def getByCustomerId(self, id: int) -> list[dict]:
        pass
    def getLastRecord(self) -> list[dict]:
        pass    
    def deleteLastMonthData(self, start: str, end: str) -> None:
        pass
