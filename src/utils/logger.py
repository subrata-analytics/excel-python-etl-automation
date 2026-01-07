import sys
import logging


def get_logger(name, log_path, log_shell=False, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.handlers:
        return logger
    
    # file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | ==> %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # console handler
    console_handler = logging.StreamHandler(sys.stdout)
    if log_shell: 
        console_handler.setLevel(logging.INFO)
    else:
        console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter(
        "==> %(levelname)s | %(name)s | %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger