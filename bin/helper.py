"""

main script for logging purpose.
script can be imported and will generate log file in
directory defined below

"""

import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler


FORMATTER = logging.Formatter(
    "%(asctime)s:%(name)s:%(pathname)s:%(lineno)d:%(levelname)s:::%(message)s"
)
LOGGING_PATH = os.environ.get("ARCHIVING_LOGGING_PATH", "automated-archiving.log")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = TimedRotatingFileHandler(LOGGING_PATH, when="midnight")
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())

    logger.propagate = False
    return logger
