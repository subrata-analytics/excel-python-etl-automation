import logging
from logging.config import dictConfig


def get_logger(name, level=logging.INFO):

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    
    logger = logging.getLogger(name)

    return logger