from db.config.conn_string import generateConnString
from sqlalchemy import create_engine
from pandas import DataFrame
import psycopg2
from psycopg2.extensions import register_type, UNICODE
from datetime import timedelta, date

""" Handle GET operation from Python Psycopg to Postgres database.

Parameters
    query {str} the Sql query.

Returns
    {list[dict]} the result of the query in python objects format.

"""
def getData(query: str) -> list[dict]:
    conn_string = generateConnString()
    conn = psycopg2.connect(conn_string)
    register_type(UNICODE, conn)
    cursor = conn.cursor()
    cursor.execute(query)
    
    response = [dict((cursor.description[i][0], value) \
               for i, value in enumerate(row)) for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()

    return response

""" Handle POST operation from Python SqlAlchemy to Postgres database.

Parameters
    query {str} the Sql query.

"""
def postData(data: DataFrame, table: str, mode: str) -> None:
    conn_string = generateConnString()
    db = create_engine(conn_string)
    connection = db.connect()
    data.to_sql(table, con=connection, if_exists=mode, index=False)
    connection.close()

""" Handle DELETE, PUT or other Sql operations from Python Psycopg to
    Postgres database.

Parameters
    query {str} the Sql query.

"""
def executeOperation(query: str) -> None:
    conn_string = generateConnString()
    conn = psycopg2.connect(conn_string)
    register_type(UNICODE, conn)
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    
    cursor.close()
    conn.close()

""" Parse every record of 'submission_date' column to Sql date type
    from a Json object.

Parameters
    webquotesJson {list[dict]} the Json that will be parsed.

Returns
    {list[dict]} the parsed Json.

"""
def parseWebquotesSubmissionDate(webquotesJson: list[dict]) -> list[dict]:
    for webquote in webquotesJson:
        date = webquote["submission_date"]
        parsedDate = date.isoformat()
        webquote["submission_date"] = parsedDate
    
    return webquotesJson

""" Parse every record of 'submission_time' column to Sql time type
    from a Json object.

Parameters
    webquotesJson {list[dict]} the Json that will be parsed.

Returns
    {list[dict]} the parsed Json.

"""
def parseWebquoteSubmissionTime(webquoteJson: list[dict]) -> list[dict]:
    for webquote in webquoteJson:
        time = webquote["submission_on_time"]
        parsedTime = time.isoformat(timespec="minutes")
        webquote["submission_on_time"] = parsedTime

    return webquoteJson

""" Creates the start and end date that will be used for deleting last
    and current month data of a database table.

Parameters
    lastDate {date} the last date recovered from database table.

Returns
    {str, str} the start and end date.


"""
def generateStartAndEndDates(lastDate: date) -> tuple[str, str]:
    firstDayOfLastDay = lastDate.replace(day=1)
    lastDayOfPreviousMonth = firstDayOfLastDay - timedelta(days=1)
    firstDayOfPreviousMonth = lastDayOfPreviousMonth.replace(day=1)

    start = firstDayOfPreviousMonth.isoformat()
    end = lastDate.isoformat()

    return start, end

""" Creates 1 month date range to use in LAE API calling.

Parameters
    lastDate {date} the last date recovered from database table.

Returns
    {dict[str, str]} both date ranges in a dictionary.

"""
def generateOneMonthsDateRange(lastDate: date) -> dict[str, str]:
    firstDayCurrentMonth = lastDate.replace(day=1)
    startCurrentMonth = firstDayCurrentMonth.isoformat()
    endCurrentMonth = lastDate.isoformat()

    dateRanges = {
        "start": startCurrentMonth,
        "end": endCurrentMonth
    }

    return dateRanges

""" Creates 2 months date range to use in LAE API calling.

Parameters
    lastDate {date} the last date recovered from database table.

Returns
    {list[dict[str, str]]} both date ranges in a dictionary.

"""
def generateTwoMonthsDateRange(lastDate: date) -> list[dict[str, str]]:
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
