import logging
import os
from datetime import datetime

# Global variable to track current session log file
_current_log_file = None


def setup_logging():
    """Initialize logging system."""
    global _current_log_file

    log_dir = os.path.join(os.getcwd(), "log", "sessions")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(
        log_dir, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    )

    # Store the current log file path
    _current_log_file = log_file

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.CRITICAL)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.handlers = [file_handler, stream_handler]

    logging.debug("Logging initialized. Writing to %s", log_file)


def get_current_log_file():
    """Get the path to the current session's log file."""
    return _current_log_file
