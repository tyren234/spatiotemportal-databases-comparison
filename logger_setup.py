import logging.handlers
import os


def setup_logger(log_file_name: str = "data/logs/runtime.log", level=logging.DEBUG):
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

    handler = logging.handlers.RotatingFileHandler(log_file_name, mode='w', backupCount=10)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    should_roll_over = os.path.isfile(log_file_name)
    handler = logging.handlers.RotatingFileHandler(log_file_name, mode='w', backupCount=5)
    if should_roll_over:  # log already exists, roll over!
        handler.doRollover()
