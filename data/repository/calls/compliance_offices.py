from data.repository.interfaces.i_compliance_offices import IComplianceOffices
from data.repository.calls.helpers import getData

class ComplianceOffices(IComplianceOffices):
    def getRegionalsByOffices():
        query = "SELECT * FROM office_info_updated"

        return getData(query=query, filename="k_db.ini")
