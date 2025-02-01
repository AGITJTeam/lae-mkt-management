from data.repository.interfaces.i_compliance import ICompliance
from data.repository.calls.helpers import executeOperation, getData

class Compliance(ICompliance):
    """
    Handles every petition of different tables from Compliance database.

    Methods
        - getRegionalsByOffices.
        - getUserEmailById.
        - searchUser.
        - insertUser.
        - getOtReportsNames.
        - getOtReportIdByName.
        - getLastOtReportId.
        - delOtReport.
    """

    def getRegionalsByOffices():
        """ Retrieve the regional, manager, office and region related
        data.
        
        Returns
            {list[dict]} all data from its table.
        """


        query = "SELECT * FROM office_info_updated;"

        return getData(query=query, filename="k_db.ini")
    
    def getUserEmailById(self, id):
        """ Retrieve the email of a user by their ID.

        Parameters
            - id {int} the ID of the user.

        Returns
            {list[dict]} a list containing the email of the user if found.
        """

        query = f"SELECT email FROM users WHERE id = {id};"
        
        return getData(query, filename="k_db.ini")
    
    def searchUser(self, username):
        """ Search a user by their username.

        Parameters
            - username {str} the username of the user to search for.

        Returns
            {list[dict]} a list containing the user's data if found.
        """

        query = f"SELECT * FROM users WHERE username = '{username}';"

        return getData(query, filename="k_db.ini")
    
    def insertUser(self, fullname, username, password, email, position, location, hired):
        """ Create query and parameters for inserting a new user.

        Parameters
            - fullname {str} the full name of the user.
            - username {str} the username of the user.
            - password {str} the hashed password of the user.
            - email {str} the email of the user.
            - position {str} the position of the user in the company.
            - location {str} the location of the user.
            - hired {datetime.date} the hire date of the user.

        Returns
            {None} executes the insertion operation in the database.
        """

        query = """
            INSERT INTO users (fullname, username, password, email, position, location, hired)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        params = (fullname, username, password, email, position, location, hired)

        return executeOperation(query, params, "k_db.ini")

    def getOtReportsNames(self):
        """ Retrieve all ot reports names in descending order.
        
        Returns
            {list[dict]} a list containing the names of the ot reports.
        """

        query = "SELECT report_name FROM ot_reports ORDER BY date_created DESC;"

        return getData(query, filename="k_db.ini")
    
    def getOtReportIdByName(self, reportName):
        """ Retrieve the ot report id by its name.
        
        Parameters
            - reportName {str} the name of the report to search for.
        
        Returns
            {list[dict]} a list containing the id of the report.
        """

        query = f"SELECT id FROM ot_reports WHERE report_name = '{reportName}';"

        return getData(query, filename="k_db.ini")
    
    def getLastOtReportId(self):
        """ Retrieve the las report id of the ot reports table.
        
        Returns
            {list[dict]} a list of table names ordered by date created.
        """

        query = "SELECT id FROM ot_reports ORDER BY date_created DESC LIMIT 1;"

        return getData(query, filename="k_db.ini")
    
    def delOtReport(self, id):
        """ Deletes an ot report by its id.
        
        Parameters
            - id {int} the id of the ot report to delete.

        Returns
            {None} executes the deletion operation in the database.        
        """


        query = f"DELETE FROM ot_reports WHERE id = {id};"

        return executeOperation(query, filename="k_db.ini")
