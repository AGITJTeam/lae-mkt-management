class ICustomers:
    def getAllData(self):
        pass
    def getById(self, id: int) -> list[dict]:
        pass
    def getByIds(self, ids: list) -> list[dict]:
        pass
    def deleteByIds(self, ids: list) -> None:
        pass
