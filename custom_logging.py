import logging
import os
import config

def setup_logger(name, log_config):
    """Set up a logger with the given name and log file path."""
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_config['level'])

    # Create file handler for the log file
    handler = logging.FileHandler(log_config['file_path'])
    handler.setLevel(log_config['level'])

    # Create log format
    formatter = logging.Formatter(log_config['format'])
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    return logger

# Ensure the log directory exists
os.makedirs(config.LOCAL_LOG_DIR, exist_ok=True)

# Set up monitor and error loggers using config settings
monitor_logger = setup_logger('monitor', config.MONITOR_LOG)
error_logger = setup_logger('error', config.ERROR_LOG)

# Example usage of logging
#monitor_logger.debug("This is a debug message, logged to monitor.log only if level is DEBUG.")
#monitor_logger.info("This is an info message, logged to monitor.log if level is INFO or lower.")
#monitor_logger.warning("This is a warning message, logged to monitor.log if level is WARNING or lower.")
#error_logger.error("This is an error message, logged to error.log only.")