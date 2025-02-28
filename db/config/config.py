from configparser import ConfigParser
import os, logging

logger = logging.getLogger(__name__)

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
            return config
        else:
            logger.error(f"Error in loadConfig. Config from {fullPath} do not have sections.")
    except Exception as e:
        logger.error(f"Error in loadConfig. {str(e)}")
