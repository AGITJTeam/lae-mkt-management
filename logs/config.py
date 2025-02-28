import logging

def setupLogging() -> None:
    """ Set up logging configuration.
    
    - Basic level is INFO.
    - Format is: [time] - [name] - [level] - [message]
    - Handlers are: FileHandler and StreamHandler.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/app.log"),
            logging.StreamHandler()
        ]
    )
