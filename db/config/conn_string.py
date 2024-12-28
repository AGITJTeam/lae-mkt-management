from db.config.config import loadConfig

def generateConnString() -> str:
    """ Generate connection string to PostgreSql with config file

    Returns
        {str} connection string.
    """

    config = loadConfig()

    return f"postgresql://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}"
