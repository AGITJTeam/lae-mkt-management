from db.config.conn_string import generateConnString
from sqlalchemy import create_engine
from pandas import DataFrame
import psycopg2
from psycopg2.extensions import register_type, UNICODE
from datetime import timedelta, datetime

def getData(query: str, filename="main_data.ini") -> list[dict]:
    """ Handle GET operation from Python Psycopg to Postgres database.

    Parameters
        - query {str} the Sql query.

    Returns
        {list[dict]} the result of the query in python objects format.
    """

    conn_string = generateConnString(filename)
    conn = psycopg2.connect(conn_string)
    register_type(UNICODE, conn)
    cursor = conn.cursor()
    cursor.execute(query)
    
    response = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    return response

def postData(data: DataFrame, table: str, mode: str) -> None:
    """ Handle POST operation from Python SqlAlchemy to PostgreSql database.

    Parameters
        - data {pandas.DataFrame} table to post to PostgreSql.
        - table {str} name of the table.
        - mode {str} mode to add data to table: 'append' to add data to
        end of table, 'replace' to replace all data with new one.
    """

    conn_string = generateConnString(filename="main_data.ini")
    db = create_engine(conn_string)
    connection = db.connect()
    data.to_sql(table, con=connection, if_exists=mode, index=False)
    connection.close()

def executeOperation(query: str) -> None:
    """ Handle DELETE, PUT or other Sql operations from Python Psycopg to
    Postgres database.

    Parameters
        - query {str} the Sql query.
    """

    conn_string = generateConnString(filename="main_data.ini")
    conn = psycopg2.connect(conn_string)
    register_type(UNICODE, conn)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    cursor.close()
    conn.close()

def parseWebquotesSubmissionDate(webquotesJson: list[dict]) -> list[dict]:
    """ Parse every record of 'submission_date' column to Sql date type
    from a Json object.

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

def parseWebquoteSubmissionTime(webquoteJson: list[dict]) -> list[dict]:
    """ Parse every record of 'submission_time' column to Sql time type
    from a Json object.

    Parameters
        - webquotesJson {list[dict]} the Json that will be parsed.

    Returns
        {list[dict]} the parsed Json.
    """

    for webquote in webquoteJson:
        time = webquote["submission_on_time"]
        parsedTime = time.isoformat(timespec="minutes")
        webquote["submission_on_time"] = parsedTime

    return webquoteJson

def generateStartAndEndDates(lastDate: datetime.date) -> tuple[str, str]:
    """ Creates the start and end date that will be used for deleting last
    and current month data of a database table.

    Parameters
        - lastDate {date} the last date recovered from database table.

    Returns
        {str, str} the start and end date.
    """

    firstDayOfLastDay = lastDate.replace(day=1)
    lastDayOfPreviousMonth = firstDayOfLastDay - timedelta(days=1)
    firstDayOfPreviousMonth = lastDayOfPreviousMonth.replace(day=1)

    start = firstDayOfPreviousMonth.isoformat()
    end = lastDate.isoformat()

    return start, end

def generateOneWeekDateRange(lastDate: datetime.date) -> dict[str, str]:
    """ Creates 1 week date range to use in LAE API calling.

    Parameters
        - lastDate {date} the last date recovered from database table.

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
    """ Creates 2 months date range to use in LAE API calling.

    Parameters
        - lastDate {date} the last date recovered from database table.

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
    now = datetime.now()
    date = now.date().isoformat()
    time = now.time().isoformat(timespec="seconds")

    return date, time
