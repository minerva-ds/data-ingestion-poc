# Batch settings
BATCH_SIZE = 10  # Number of sources to process in each batch

# Parallel processing settings
MAX_PARALLEL_PROCESSES = 4  # Number of child processes to run in parallel

# Child process settings
CHILD_PROCESS = {
    "timeout": 300,  # Timeout for each child process in seconds
    "max_concurrent_tasks": 3,  # Max concurrent downloads within a single child process, placeholder for future functionality
    # Optional ToDo settings
    # "retries": 3,    # Number of retries for a failed child process
    # "retry_delay": 5,  # Delay between retries in seconds
}

# Directory settings
LOCAL_DOWNLOAD_DIR = "downloads"  
LOCAL_LOG_DIR = "log"  

# Azure Storage settings
AZURE_STORAGE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNoGVz4e3tNfgCclwLFbA==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)

#must be all lower case and avoid most special characters
AZURE_CONTAINER_NAME = "your-azure-container-name"

# Verbosity and logging - Separate configs for monitor and error logs
MONITOR_LOG = {
    "level": "INFO",  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    "file_path": "log/monitor.log",  # Path to the monitor log file
    "format": "%(asctime)s - %(levelname)s - %(message)s",  # Log format for monitoring
}

ERROR_LOG = {
    "level": "ERROR",  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
    "file_path": "log/error.log",  # Path to the error log file
    "format": "%(asctime)s - %(levelname)s - %(message)s",  # Log format for errors
}

# Optional ToDo: Add settings for retries, backoff, or error thresholds as needed