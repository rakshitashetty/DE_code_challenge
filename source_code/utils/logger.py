import logging
import os
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a logger instance with a standard configuration.

    This logger outputs messages to the console with a consistent format
    that includes the timestamp, logger name, log level, and message.
    It ensures that duplicate handlers are not added on repeated calls.

    Args:
        name (str): The name of the logger, typically __name__ of the calling module.

    Returns:
        logging.Logger: A configured logger instance for the given name.
    """

    # Create logs directory if it doesn't exist
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)

    # Generate run date format: YYYYMMDD_HHMMSS
    run_date = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(log_dir, f"run_{run_date}.log")

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Adjust if you want INFO level globally

    # Avoid duplicate handlers if logger is reused
    if not logger.handlers:
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)

        # Console handler (optional)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
