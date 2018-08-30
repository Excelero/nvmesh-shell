import logging
import os

logging.basicConfig(filename=os.path.expanduser('~/.nvmeshcli.log'),
                    format="%(asctime)s - %(levelname)-8s - %(message)s", datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)


def log(level, message):
    if level == "info":
        logging.info(str(message))
    elif level == "warning":
        logging.warning(str(message))
    elif level == "critical":
        logging.critical(str(message))
    elif level == "debug":
        logging.debug(str(message))
