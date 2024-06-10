import logging.handlers
import os


def setup_logging(level=logging.DEBUG):
    format = "%(asctime)s - %(levelname)s: %(message)s"
    logging.basicConfig(format=format)
    logger = logging.getLogger()
    logger.setLevel(level)
    return logger
