""" Generate connection string to PostgreSql with config file

Returns
    {str} connection string.

"""
from db.config.config import loadConfig

def generateConnString() -> str:
    config = loadConfig()

    return f"postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}"
