import os
import pycurl
import shutil
import zipfile
from pathlib import Path
import config
import custom_logging as cl
from azure.storage.blob import BlobServiceClient, ContentSettings
from urllib.parse import urlparse
import time
import re

# Azure connection setup
blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)

def get_server_folder_name(server):
    parsed = urlparse(server)
    return f"{parsed.hostname}_{parsed.port or (21 if parsed.scheme == 'ftp' else 22)}"

def sanitize_filename(filename):
    # Replace any Unicode characters with hyphens
    filename = re.sub(r'[^\x00-\x7F]', '-', filename)
    
    # Replace disallowed characters (except for dots, hyphens, underscores, and spaces) with a hyphen
    filename = re.sub(r'[^A-Za-z0-9\.\-_ ]', '-', filename)
    
    # Remove control characters (range \x00-\x1F and \x7F), replacing with hyphens
    filename = re.sub(r'[\x00-\x1F\x7F]', '-', filename)
    
    # Strip leading or trailing spaces without leaving hyphens at the ends
    filename = filename.strip(' ')
    
    return filename

def get_remote_file_size(url):
    """Get the file size from the remote server before downloading."""
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.NOBODY, True)  # We only want the headers
    c.perform()
    remote_file_size = c.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD)
    c.close()
    
    if remote_file_size < 0:
        cl.error_logger.error(f"Could not get the file size for {url}")
        raise Exception(f"Could not get the file size for {url}")
    
    return remote_file_size

def get_remote_file_timestamp(url):
    """Get the last modified timestamp of a file from the remote server."""
    c = pycurl.Curl()
    c.setopt(pycurl.URL, url)
    c.setopt(pycurl.NOBODY, True)  # We only want the headers
    c.setopt(pycurl.OPT_FILETIME, True)  # This enables retrieval of the file's timestamp
    c.perform()
    # Get the remote file's last modified time (in seconds since epoch)
    remote_timestamp = c.getinfo(pycurl.INFO_FILETIME)
    c.close()
    
    if remote_timestamp == -1:
        cl.error_logger.error(f"Could not get the last modified time for {url}")
        raise Exception(f"Could not get the last modified time for {url}")
    
    return remote_timestamp

def download_file_with_pycurl(url, local_path):
    """Download a file from FTP or SFTP using pycurl and verify integrity by size."""
    # Get the expected size and timestamp of the file from the server
    expected_size = get_remote_file_size(url)
    remote_timestamp = get_remote_file_timestamp(url)

    # Download the file
    with open(local_path, 'wb') as f:
        c = pycurl.Curl()
        c.setopt(pycurl.URL, url)
        c.setopt(pycurl.WRITEDATA, f)
        c.setopt(pycurl.NOPROGRESS, True)
        c.perform()
        c.close()
    
    # Check if the downloaded file size matches the expected size
    downloaded_size = os.path.getsize(local_path)
    cl.monitor_logger.info(f"Downloaded file {local_path}, size: {downloaded_size} bytes")

    if downloaded_size != expected_size:
        cl.error_logger.error(f"Incomplete download for {local_path}: expected size {expected_size} bytes, got {downloaded_size} bytes")
        raise Exception(f"Incomplete download for {local_path}: expected size {expected_size} bytes, got {downloaded_size} bytes")
    
    # Set the file's modified time to match the remote timestamp
    os.utime(local_path, (remote_timestamp, remote_timestamp))
    cl.monitor_logger.info(f"Successfully downloaded and verified file {local_path} with original timestamp")

def handle_zip_file(local_path, destination_folder, server_folder, file_type):
    """Extract zip file contents while preserving their own metadata and uploading them."""
    # Create a folder for the extracted contents
    extracted_dir = os.path.join(destination_folder, f"extracted_{Path(local_path).stem}")
    os.makedirs(extracted_dir, exist_ok=True)

    with zipfile.ZipFile(local_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            # Extract each file individually
            extracted_path = zip_ref.extract(file_info, extracted_dir)
            # Preserve the original modified time of the file inside the zip
            original_modified_time = time.mktime(file_info.date_time + (0, 0, -1))
            set_file_metadata(extracted_path, original_modified_time)
            cl.monitor_logger.info(f"Extracted {extracted_path} with original timestamp")

            # Sanitize and determine the file name
            extracted_file_name = sanitize_filename(file_info.filename.split('/')[-1])
            extracted_file_type = extracted_file_name.split('.')[-1] if '.' in extracted_file_name else 'none'
            
            # Handle each extracted file as if it were individually downloaded
            handle_file(extracted_path, server_folder, extracted_file_name, extracted_file_type)

    # Delete the original zip file
    try:
        os.remove(local_path)
        cl.monitor_logger.info(f"Deleted original zip file: {local_path}")
    except Exception as e:
        cl.error_logger.error(f"Error deleting zip file {local_path}: {e}")

def set_file_metadata(local_path, modified_time):
    """Set the file's access and modified time to the original timestamp."""
    os.utime(local_path, (modified_time, modified_time))
    cl.monitor_logger.info(f"Set original timestamp for {local_path}")

def download_and_handle_file(server, remote_path):
    try:
        server_folder = get_server_folder_name(server)
        file_name = sanitize_filename(remote_path.split('/')[-1])
        file_type = file_name.split('.')[-1] if '.' in file_name else 'none'

        local_dir = os.path.join(config.LOCAL_DOWNLOAD_DIR, server_folder, file_type)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, file_name)

        download_url = f"{server}{remote_path}"
        cl.monitor_logger.info(f"Downloading {download_url} to {local_path}")
        download_file_with_pycurl(download_url, local_path)

        # Extract zip files or handle regular files
        if file_type.lower() == 'zip':
            handle_zip_file(local_path, local_dir, server_folder, file_type)
        else:
            handle_file(local_path, server_folder, file_name, file_type)

    except Exception as e:
        cl.error_logger.error(f"Error downloading {remote_path} from {server}: {e}")

def handle_file(local_path, server_folder, file_name, file_type):
    """Handle a file after download by uploading and cleaning up."""
    try:
        upload_file(local_path, server_folder, file_name, file_type)
    except Exception as e:
        cl.error_logger.error(f"Error while handling file {local_path}: {e}")
    finally:
        cleanup_file(local_path)

def upload_file(local_path, server_folder, file_name, file_type):
    """Upload a file to Azure Blob Storage while preserving metadata and verifying upload integrity."""
    try:
        modified_time = os.path.getmtime(local_path)
        creation_time = os.path.getctime(local_path)
        file_size = os.path.getsize(local_path)
        container_name = config.AZURE_CONTAINER_NAME
        server_folder_sanitized = sanitize_filename(server_folder)
        file_name_sanitized = sanitize_filename(file_name)

        # Determine blob path and check for duplicates
        base_name, ext = os.path.splitext(file_name_sanitized)
        blob_path = f"{server_folder_sanitized}/{file_type}/{base_name}{ext}"

        # Check for potential duplicates in Azure storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        try:
            # Fetch blob properties to check for duplicates
            existing_blob_properties = blob_client.get_blob_properties()
            existing_metadata = existing_blob_properties.metadata
            
            # Compare file size and modified time
            if (str(file_size) == existing_metadata.get("file_size") and
                str(int(modified_time)) == existing_metadata.get("modified_time")):
                # If a duplicate, append Unix timestamp to file name
                timestamp = int(time.time())
                blob_path = f"{server_folder_sanitized}/{file_type}/{base_name}_{timestamp}{ext}"
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        except Exception:
            # Blob does not exist, continue with original blob_path
            pass
        
        cl.monitor_logger.info(f"Uploading {local_path} to Azure as {blob_path}")

        # Upload the blob with metadata
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                content_settings=ContentSettings(content_type="application/octet-stream"),
                metadata={
                    "creation_time": str(int(creation_time)),
                    "modified_time": str(int(modified_time)),
                    "file_size": str(file_size)
                },
                overwrite=True
            )
        
        cl.monitor_logger.info(f"Successfully uploaded {local_path} to Azure as {blob_path}")

        # Integrity check: Verify upload
        uploaded_blob_properties = blob_client.get_blob_properties()
        uploaded_size = uploaded_blob_properties.size
        
        if uploaded_size != file_size:
            cl.error_logger.error(f"Upload failed for {local_path}: size mismatch (local: {file_size}, uploaded: {uploaded_size})")
            raise Exception(f"Upload failed for {local_path}: size mismatch")

        cl.monitor_logger.info(f"Upload verified for {local_path}: size matches")

    except Exception as e:
        cl.error_logger.error(f"Error uploading {local_path} to Azure: {e}")

def cleanup_file(local_path):
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