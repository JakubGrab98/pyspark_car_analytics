import logging
import logging.config
import json
from pathlib import Path


def setup_logging() -> None:
    """Function setups logging confguration using config.json file.
    """
    config_file = Path("logging_configs/config.json")
    try:
        with open(config_file) as file:
            config = json.load(file)
        logging.config.dictConfig(config)
    except FileNotFoundError:
        logging.error("Logging configuration file not found.")
    except json.JSONDecodeError:
        logging.error("Invalid JSON in the logging configuration file.")
