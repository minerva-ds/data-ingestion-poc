import os
import pycurl
import shutil
from pathlib import Path
import config
import custom_logging as cl
from azure.storage.blob import BlobServiceClient, ContentSettings
from urllib.parse import urlparse
import time
import re
import datetime

# Azure connection setup
blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)

def get_server_folder_name(server):
    """Generate a normalized folder name from the server URL."""
    parsed = urlparse(server)
    return f"{parsed.hostname}_{parsed.port or (21 if parsed.scheme == 'ftp' else 22)}"

def sanitize_filename(filename):
    """Sanitize filename for both local and Azure compatibility."""
    sanitized = re.sub(r'[^A-Za-z0-9\-\.]', '_', filename).strip('-.')
    sanitized = re.sub(r'[-_]+', '_', sanitized)
    return sanitized

def download_file_with_pycurl(url, local_path):
    """Download a file from FTP or SFTP using pycurl."""
    with open(local_path, 'wb') as f:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, f)
        c.setopt(c.NOPROGRESS, True)  # Disable progress meter
        c.perform()
        c.close()

def get_remote_file_modified_time(server, remote_path):
    """Fetch the remote file's modified time and convert it to a timestamp."""
    # Here we should implement a method to fetch the modified time of the remote file.
    # Unfortunately, this is highly dependent on the protocol (FTP/SFTP) and server setup.
    # For the sake of this example, we'll just return the current time as a placeholder.
    return time.time()

def set_file_metadata(local_path, modified_time):
    """Set the local file's metadata to match the remote file's modified time."""
    os.utime(local_path, (modified_time, modified_time))

def download_and_handle_file(server, remote_path):
    """Download a file and trigger post-download events."""
    try:
        server_folder = get_server_folder_name(server)
        file_name = sanitize_filename(remote_path.split('/')[-1])
        file_type = file_name.split('.')[-1] if '.' in file_name else 'none'

        local_dir = os.path.join(config.LOCAL_DOWNLOAD_DIR, server_folder, file_type)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, file_name)

        # Download file using pycurl
        download_url = f"{server}{remote_path}"
        cl.monitor_logger.info(f"Downloading {download_url} to {local_path}")
        download_file_with_pycurl(download_url, local_path)

        # Retrieve the modified time of the remote file
        modified_time = get_remote_file_modified_time(server, remote_path)

        # Set the local file's modified time
        set_file_metadata(local_path, modified_time)

        # Trigger post-download event for file handling
        handle_file(local_path, server_folder, file_name, file_type)

    except Exception as e:
        cl.error_logger.error(f"Error downloading {remote_path} from {server}: {e}")

def handle_file(local_path, server_folder, file_name, file_type):
    """Handle a file after download, including setting metadata, upload, and cleanup."""
    try:
        # Set file metadata and upload
        modified_time = os.path.getmtime(local_path)
        file_size = os.path.getsize(local_path)
        upload_file(local_path, server_folder, file_name, file_type, file_size, modified_time)
    finally:
        # Cleanup local files after processing
        cleanup_file(local_path)

def upload_file(local_path, server_folder, file_name, file_type, file_size, modified_time):
    """Upload a file to Azure Blob Storage."""
    try:
        container_name = config.AZURE_CONTAINER_NAME
        server_folder_sanitized = sanitize_filename(server_folder)
        file_name_sanitized = sanitize_filename(file_name)
        blob_path = f"{server_folder_sanitized}/{file_type}/{int(time.time())}/{file_name_sanitized}"

        cl.monitor_logger.info(f"Uploading {local_path} to Azure as {blob_path}")

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                content_settings=ContentSettings(content_type="application/octet-stream"),
                overwrite=True
            )
        cl.monitor_logger.info(f"Successfully uploaded {local_path} to Azure as {blob_path}")
    except Exception as e:
        cl.error_logger.error(f"Error uploading {local_path} to Azure: {e}")

def cleanup_file(local_path):
    """Clean up local files or directories after processing."""
    try:
        if os.path.isfile(local_path):
            os.remove(local_path)
            cl.monitor_logger.info(f"Cleaned up file {local_path}")
        elif os.path.isdir(local_path):
            shutil.rmtree(local_path)
            cl.monitor_logger.info(f"Cleaned up directory {local_path}")
    except Exception as e:
        cl.error_logger.error(f"Error cleaning up {local_path}: {e}")

def process_batch(batch):
    """Process a batch of files using pycurl for downloads."""
    for server, remote_path in batch:
        download_and_handle_file(server, remote_path)
