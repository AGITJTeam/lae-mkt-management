from db.config.conn_string import generateConnString
from psycopg2.extensions import register_type, UNICODE
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta, datetime
from pandas import DataFrame
import psycopg2, logging

logger = logging.getLogger(__name__)

def getData(query: str, filename: str) -> list[dict] | None:
    """ Handle GET operation from Python Psycopg to Postgres database.

    Parameters
        - query {str} the Sql query.
        - filename {str} the name of the .ini file to get the connection.

    Returns
        {list[dict] | None} the result of the query in python objects or
        None if exception raise an exception.
    """

    connString = generateConnString(filename)
    conn = psycopg2.connect(connString)
    register_type(UNICODE, conn)

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        response = [dict((cursor.description[i][0], value) \
                for i, value in enumerate(row)) for row in cursor.fetchall()]
        
        return response
    except Exception as e:
        logger.error(f"Error getting data from database in getData: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def executeOperation(query: str, filename: str, params: tuple = None) -> bool | None:
    """ Handle other Sql operations from Python Psycopg to Postgres
    database.

    Parameters
        - query {str} the Sql query.
        - filename {str} the name of the .ini file to get the connection.
        - params {tuple} the parameters to be used in the query. Default
        value is None and it can be a tuple of values.

    Returns
        {bool | None} bool after execution or None if exception raise
        an exception.
    """

    conn_string = generateConnString(filename)
    conn = psycopg2.connect(conn_string)
    register_type(UNICODE, conn)
    cursor = conn.cursor()

    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        rowsAffected = cursor.rowcount
        conn.commit()

        return rowsAffected > 0
    except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
        logger.error(f"Error executing Sql query in executeOperation: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def postDataframeToDb(data: DataFrame, table: str, mode: str, filename: str) -> bool | None:
    """ Handle POST operation from Python SqlAlchemy to PostgreSql database.

    Parameters
        - data {pandas.DataFrame} table to post to PostgreSql.
        - table {str} name of the table.
        - mode {str} mode to add data to table: 'append' to add data to
        end of table, 'replace' to replace all data with new one.
        - filename {str} the name of the .ini file to get the connection.
    
    Returns
        bool | None} bool after execution or None if exception raise
        an exception.
    """

    conn_string = generateConnString(filename)
    db = create_engine(conn_string)
    connection = db.connect()

    try:
        data.to_sql(table, con=connection, if_exists=mode, index=False)
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error posting DataFrame to table '{table}': {str(e)}")
        raise
    finally:
        connection.close()

def parseWebquotesSubmissionDate(webquotesJson: list[dict]) -> list[dict]:
    """ Parse every record of 'submission_date' column from sql date
    type to string type.

    Parameters
        - webquotesJson {list[dict]} the Json that will be parsed.

    Returns
        {list[dict]} the parsed Json.
    """

    for webquote in webquotesJson:
        date = webquote["submission_date"]
        parsedDate = date.isoformat()
        webquote["submission_date"] = parsedDate
    
    return webquotesJson

def generateOneWeekDateRange(lastDate: datetime.date) -> dict[str, str]:
    """ Creates 1 week date range to use in sql query.

    Parameters
        - lastDate {datetime.date} the last date recovered from database table.

    Returns
        {dict[str, str]} both date ranges in a dictionary.
    """

    sixDays = timedelta(days=6)
    startDate = lastDate - sixDays
    start = startDate.isoformat()
    end = lastDate.isoformat()

    dateRanges = {
        "start": start,
        "end": end
    }

    return dateRanges

def generateTwoMonthsDateRange(lastDate: datetime.date) -> list[dict[str, str]]:
    """ Creates 2 months date range to use in sql calls.

    Parameters
        - lastDate {datetime.date} the last date recovered from database table.

    Returns
        {list[dict[str, str]]} both date ranges in a dictionary.
    """

    firstDayCurrentMonth = lastDate.replace(day=1)
    lastDayPreviousMonth = firstDayCurrentMonth - timedelta(days=1)
    firstDayPreviousMonth = lastDayPreviousMonth.replace(day=1)

    startPreviousMonth = firstDayPreviousMonth.isoformat()
    endPreviousMonth = lastDayPreviousMonth.isoformat()
    startCurrentMonth = firstDayCurrentMonth.isoformat()
    endCurrentMonth = lastDate.isoformat()

    dateRanges = [
        {
            "start": startPreviousMonth,
            "end": endPreviousMonth
        },
        {
            "start": startCurrentMonth,
            "end": endCurrentMonth
        }
    ]

    return dateRanges

def generateDateTimeUpdated() -> tuple[str, str]:
    """ Get the current date and time for record the finish of automated
    scripts with crontabs.
    
    Returns
        {tuple[str, str]} the current date and time in isoformat.
    """

    now = datetime.now()
    date = now.date().isoformat()
    time = now.time().isoformat(timespec="seconds")

    return date, time
