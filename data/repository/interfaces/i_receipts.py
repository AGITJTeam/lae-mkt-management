class IReceipts:
    def getLastRecord(self) -> list[dict]:
        pass
    def getBetweenDates(self, start: str, end: str) -> list[dict]:
        pass
    def deleteByIds(self, ids: list) -> None:
        pass
