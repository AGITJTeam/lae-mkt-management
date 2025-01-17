from configparser import ConfigParser
import os

def loadConfig(filename: str, section: str ="postgresql") -> dict | None:
    """ Load PostgreSql credentials from database.ini

    Parameters
        - filename {str} file where the credentials are stored.
        - section {str} PostgreSql database server.

    Returns
        {dict | None} database.ini credentials in dict format.
    """

    path = os.path.dirname(os.path.abspath(__file__))
    fullPath = os.path.join(path, filename)

    parser = ConfigParser()
    parser.read(fullPath)

    config = {}
    try:
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                config[param[0]] = param[1]
        else:
            print(f"back.internal.config.config.py. loadConfig() - config from {fullPath} do not have sections.")

        return config
    except Exception as e:
        print(f"back.internal.config.config.py. loadConfig() - Error al conectarse a la db: {e}")
