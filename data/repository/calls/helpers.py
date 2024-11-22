from db.config.conn_string import generateConnString
from sqlalchemy import create_engine
from pandas import DataFrame
import psycopg2
import json

def getData(query: str):
    conn_string = generateConnString()
    conn = psycopg2.connect(conn_string)
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