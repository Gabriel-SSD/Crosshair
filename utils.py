import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging(log_file_path="logs/app.log", level=logging.ERROR):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = RotatingFileHandler(
        log_file_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding='utf-8'
    )

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(filename)s:%(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(stream_handler)


def safe_init(cls, name, *args, **kwargs):
    try:
        return cls(*args, **kwargs)
    except Exception as ecp:
        logging.error(f"[safe_init] Failed to initialize '{name}': {ecp}")
        return None
