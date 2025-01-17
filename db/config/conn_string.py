from db.config.config import loadConfig

def generateConnString(filename: str) -> str:
    """ Generate connection string to PostgreSql with config file

    Returns
        {str} connection string.
    """

    config = loadConfig(filename)

    return f"postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}"
