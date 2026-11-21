import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str, log_path: str, max_bytes: int = 1_048_576, backup_count: int = 3) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False
    return logger
