from datetime import datetime

class ICompliance:
    def getRegionalsByOffices() -> list[dict]:
        pass
    def getUserEmailById(self, id: int) -> list[dict]:
        pass
    def searchUser(self, username: str) -> list[dict]:
        pass
    def insertUser(self, fullname: str, username: str, password: str, email: str, position: str, location: str, hired: datetime.date) -> None:
        pass
    def getOtReportsNames(self) -> list[dict]:
        pass
    def getOtReportByName(self, reportName: str) -> list[dict]:
        pass
    def getLastOtReportId(self) -> list[dict]:
        pass
    def delOtReport(self, reportName: str) -> None:
        pass
