from data.repository.interfaces.i_main_data import IMainData
from data.repository.calls.helpers import getData

class MainData(IMainData):
    """
    Handles every petition of different tables from Main Data database.

    Methods
        - getUniqueDialpadCalls.
        - getAllDialpadCalls.
    """

    def getUniqueDialpadCalls(self, start, end):
        """ Retrieve region, office and count of unique dialpad calls
        in a given date range.
        
        Parameters
            - start {str} the beginning of the date range.
            - end {str} the end of the date range.
        
        Returns
            {list[dict]} unique dialpad calls count by region and office.
        """

        query = (
            "SELECT ao.region as regionName, ao.name as officeName, COUNT(dc.phone) as callsCount "
            'FROM "Aoffices" ao '
            "LEFT JOIN users_offices uo ON ao.id = uo.office_id "
            "LEFT JOIN users u ON uo.user_id = u.id "
            "LEFT JOIN dialpad_calls dc ON u.user_email = dc.agent_email "
            "WHERE "
            f"(dc.created_at between '{start} 00:00:00.000000' and '{end} 23:59:00.000000') and "
            "dc.direction = 'inbound' and "
            "dc.is_answered = true and "
            "dc.state in ('hangup') "
            "GROUP BY regionName, officeName;"
        )

        return getData(query=query, filename="main_data.ini")

    def getAllDialpadCalls(self, start, end):
        """ Retrieve region, office and count of all dialpad calls
        in a given date range.
        
        Parameters
            - start {str} the beginning of the date range.
            - end {str} the end of the date range.
        
        Returns
            {list[dict]} all dialpad calls count by region and office.
        """

        query = (
            "SELECT ao.region as regionName, ao.name as officeName, COUNT(dc.phone) as callsCount "
            'FROM "Aoffices" ao '
            "LEFT JOIN users_offices uo ON ao.id = uo.office_id "
            "LEFT JOIN users u ON uo.user_id = u.id "
            "LEFT JOIN dialpad_calls dc ON u.user_email = dc.agent_email "
            "WHERE "
            f"(dc.created_at between '{start} 00:00:00.000000' and '{end} 23:59:00.000000') and "
            "dc.direction = 'inbound' and "
            "dc.state in ('hangup', 'missed') "
            "GROUP BY regionName, officeName;"
        )

        return getData(query=query, filename="main_data.ini")
