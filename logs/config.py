from pathlib import Path
import logging

def setupLogging() -> None:
    """ Set up logging configuration.
    
    - Basic level is INFO.
    - Format is: [time] - [name] - [level] - [message]
    - Handlers are: FileHandler and StreamHandler.
    """

    filePath = Path(__file__).parent.parent
    path = filePath.joinpath("logs")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(filename=f"{path}/app.log"),
            logging.StreamHandler()
        ]
    )
