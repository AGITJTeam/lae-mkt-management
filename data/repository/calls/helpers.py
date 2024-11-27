from db.config.conn_string import generateConnString
from sqlalchemy import create_engine
from pandas import DataFrame
import psycopg2
from psycopg2.extensions import register_type, UNICODE
import json

def getData(query: str):
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

def postData(data: DataFrame, table: str, mode: str) -> None:
    conn_string = generateConnString()
    db = create_engine(conn_string)
    connection = db.connect()
    data.to_sql(table, con=connection, if_exists=mode, index=False)
    connection.close()

def parseWebquotesDates(webquotesJson):
    for webquote in webquotesJson:
        date = webquote["submission_date"]
        time = webquote["submission_on_time"]

        parsedDate = date.isoformat()
        parsedTime = time.isoformat(timespec="minutes")

        webquote["submission_date"] = parsedDate
        webquote["submission_on_time"] = parsedTime
    
    return webquotesJson