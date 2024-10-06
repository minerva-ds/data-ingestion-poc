# Data Ingestion Solution

Ingest files of various types (ex. zip) using python as the scripting language and Azure as the data storage endpoint.  Sources will be known but configurable S/FTP servers.  Priorities are on robustness (ex. Handling special characters), optimizing compute time (ex. Run in parallel), error logging, monitoring, data preservation, extensibility and scalability.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Setup](#setup)
   - [Environment Setup](#environment-setup)
   - [Azurite Setup for Testing Azure Blob Storage](#azurite-setup-for-testing-azure-blob-storage)
   - [FTP Server Setup for Testing](#ftp-server-setup-for-testing)
   - [Configuring Sources](#configuring-sources)
3. [Running the Script](#running-the-script)
4. [Scheduling for Automation](#scheduling-for-automation)
   - [Using a Cron Job](#using-a-cron-job)
   - [Using a Task Scheduler](#using-a-task-scheduler)
5. [Monitoring and Logging](#monitoring-and-logging)
6. [Extending the Solution](#extending-the-solution)

## Prerequisites

- **Conda**: Used for managing the Python environment.

### Local Testing Requirements

Optional if you don't have actual services to run with, these will let you test locally

- **Node.js and npm**: Required if you wish to use Azurite (Azure Blob Storage emulator).
- **FTP server(s)**: Either real FTP/SFTP servers or the local FTP server for testing.

## Setup

### Environment Setup

1. **Create a Conda environment**  
   A `.yml` file is provided to set up the necessary Python environment:
   ```bash
   conda env create -f data-ingestion.yml
   conda activate data-ingestion
   ```

### Azurite Setup for Testing Azure Blob Storage

If you are testing without an actual Azure Blob Storage account, use **Azurite**.  

1. **Install Azurite**:
   ```bash
   npm install -g azurite
   ```

2. **Run Azurite**:
   Start Azurite for Blob service in another terminal or in the background:
   ```bash
   export AZURITE_ACCOUNTS="devstoreaccount1:Eby8vdM02xNoGVz4e3tNfgCclwLFbA=="
   azurite --silent --location azurite --debug log/azurite.log
   ```
### FTP Server Setup for Testing

Run this in another terminal too, or in the background

1. **Run the FTP Server**:
   - Navigate to the project directory.
   - Start the test FTP server:
     ```bash
     python ftp_server.py
     ```
   The server will run on `ftp://localhost:2121` with credentials `user:password`. It will provide access to a ZIP file `test_file.zip` located in the FTP root directory.

2. **Add the FTP Server to `sources.py`**  
   You can use this test FTP server as a source for testing by adding or modifying entries in `sources.py`. The `ftp` directory is a place you can put additional files for testing.  A 5MB zip file is created automatically for you.

### Configuring Sources

Configure your sources in `sources.py`:
```python
SOURCES = {
    # Example: Local FTP server
    "ftp://user:password@localhost:2121": [
        "/test_file.zip"
    ],
    # Add more sources as needed
}
```

### Checking Azurite Uploads
`list_blobs.py` will let you return a list of what was uploaded to azurite.  It is truncated though so you can stop the azurite service and delete the folder and start the service again if you want to test something specific.  `azurite --silent --location azurite --debug log/azurite.log` to start it again.

---

## Running the Script

1. **Ensure Configuration**:  
   Confirm that `config.py` is set up properly with the correct settings for batch size, parallelism, and Azure storage.

2. **Ingest Files**:  
   To manually run the ingestion process:
   ```bash
   python main.py
   ```

---

## Scheduling for Automation

You can automate the script using either a cron job (Linux/Mac) or a task scheduler (Windows).

### Using a Cron Job (Linux/Mac)

1. **Edit Crontab**:  
   Open the crontab editor:
   ```bash
   crontab -e
   ```

2. **Add a Cron Job**:  
   Add an entry to schedule the script, e.g., to run every hour:
   ```bash
   0 * * * * /path/to/your/conda/env/bin/python /path/to/your/project/main.py >> /path/to/your/logfile.log 2>&1
   ```
   Make sure to replace `/path/to/your/conda/env/bin/python` with the path to the Python interpreter within your Conda environment, and `/path/to/your/project/main.py` with the full path to your script.

### Using a Task Scheduler (Windows)

1. **Open Task Scheduler**.
2. **Create a New Task**:
   - Set the trigger to the desired schedule (e.g., every hour).
   - Set the action to run the Python script:
     ```
     [Path to your Conda environment's python.exe] [Path to main.py]
     ```
3. **Save the Task**.

---

## Monitoring and Logging

- **Monitor Log**: Captures general operations and system health.  
  Path: `log/monitor.log`
  
- **Error Log**: Captures any errors or exceptions encountered during the process.  
  Path: `log/error.log`

Logging levels and formats can be adjusted in `config.py`.

---

## Extending the Solution

To add new sources or file types:
1. **Add the Source**: Update `sources.py` to include new FTP/SFTP servers or local shares.
2. **Adjust Configuration**: If needed, adjust `config.py` for batch size, parallel processes, or other settings.
3. **Code Extensibility**: If in the future you need additional extraction of archive types that will need to be added via code.  However, it currently handles any kind of file you give it if the goal is just to upload to Azure.

---

## Notes

- **Testing**: This solution uses a test FTP server and Azurite for local testing. Make sure to adjust your setup based on your real-world FTP servers and Azure environment.  It also includes public FTP and SFTP servers that should be around a long time to test it.  A lot of logging happens to both monitor.log and error.log as well as azurite.log.  Given the time constraints, a basic set of unit tests is in `tests/test_basic.py`
- **Data Integrity**: All downloaded and uploaded files are verified for size and modified timestamps.

Feel free to contribute to or extend the project as needed.