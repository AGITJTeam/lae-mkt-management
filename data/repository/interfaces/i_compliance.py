from datetime import datetime

class ICompliance:
    def getPositions(self) -> list[dict]:
        pass
    def getRegionalsByOffices(self) -> list[dict]:
        pass
    def getUserEmailById(self, id: int) -> list[dict]:
        pass
    def getAllUsernames(self) -> list[dict]:
        pass
    def searchUser(self, username: str) -> list[dict]:
        pass
    def insertUser(self, fullname: str, username: str, password: str, email: str, position: str, location: str, hired: datetime.date) -> None:
        pass
    def getOtReportsNames(self) -> list[dict]:
        pass
    def getOtReportIdByName(self, reportName: str) -> list[dict]:
        pass
    def getLastOtReportId(self) -> list[dict]:
        pass
    def getOtReportById(self, id: str) -> list[dict]:
        pass
    def delOtReport(self, reportName: str) -> None:
        pass
    def getNumberOfOtReports(self) -> list[dict]:
        pass
