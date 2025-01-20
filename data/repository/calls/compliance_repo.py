from data.repository.interfaces.i_compliance import ICompliance
from data.repository.calls.helpers import executeOperation, getData

class Compliance(ICompliance):
    def getRegionalsByOffices():
        query = "SELECT * FROM office_info_updated"

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
        """ Search for a user by their username.

        Parameters
            - username {str} the username of the user to search for.

        Returns
            {list[dict]} a list containing the user's data if found.
        """

        query = f"SELECT * FROM users WHERE username = '{username}';"

        return getData(query, filename="k_db.ini")
    
    def insertUser(self, fullname, username, password, email, position, location, hired):
        """ Create a query and prepare the parameters for inserting a
        new user.

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

        return executeOperation(query, params)
    
    def getAllTableNames(self):
        """ Retrieve all table names from the `saved_immdata` table.

        Returns
            {list[dict]} a list containing all table names.
        """
        query = "SELECT table_name FROM saved_immdata;"

        return getData(query, filename="k_db.ini")
    
    def insertTableName(self, table, data):
        """ Create a query based on the number of columns and prepare
        the parameters to insert a table name in a particular table
        (saved_data or saved_immdata).

        Parameters
            - table {str} the name of the table to query.
            - data {dict} the data to be inserted.
        """

        columns = ", ".join(data)
        params = tuple(data.values())
        replacements = ", ".join(["%s"] * len(data))

        query = f"INSERT INTO {table} ({columns}) VALUES ({replacements});"

        return executeOperation(query, params)
    
    def getTableName(self, table, tableName):
        """ Retrieves a table name from a particular table (saved_data
        or saved_immdata).

        Parameters
            - table {str} the name of the table to query.
            - tableName {str} the name of the specific entry in the table.

        Returns
            {list[dict]} a list containing the data from the specified table.
        """

        query = f"SELECT * FROM {table} WHERE table_name = '{tableName}';"

        return getData(query, filename="k_db.ini")
    
    def deleteTableName(self, table, tableName):
        """ Delete a table name from a particular table (saved_data
        or saved_immdata).

        Parameters
            - table {str} the name of the table to delete from.
            - tableName {str} the name of the table entry to delete.

        Returns
            {None} executes the deletion operation in the database.
        """

        query = f"DELETE FROM {table} WHERE table_name = '{tableName}';"

        return executeOperation(query)

    def getAllOfficesInfo(self):
        """ Retrieve all office information from the `office_info` table.

        Returns
            {list[dict]} a list containing all office information.
        """

        query = f"SELECT * FROM office_info_updated;"

        return getData(query, filename="k_db.ini")
    
    def getSavedDataByDateCreated(self):
        """ Retrieve table names ordered by the date created from the
        'saved_data' table.

        Returns
            {list[dict]} a list of table names ordered by date created.
        """

        query = "SELECT table_name FROM saved_data ORDER BY datecreated DESC;"

        return getData(query, filename="k_db.ini")

