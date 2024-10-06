import os
import asyncio
import aioftp
import asyncssh
import zipfile
import shutil
import time
import re
from azure.storage.blob import BlobServiceClient, ContentSettings
import custom_logging as cl
import config
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

# Azure connection setup
blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)

def get_server_folder_name(server):
    """Generate a normalized folder name from the server URL."""
    parsed = urlparse(server)
    return f"{parsed.hostname}_{parsed.port or (21 if parsed.scheme == 'ftp' else 22)}"

def sanitize_filename(filename):
    """Sanitize filename for both local and Azure compatibility."""
    # Replace any characters that are not alphanumeric, dash, or period with underscores
    sanitized = re.sub(r'[^A-Za-z0-9\-\.]', '_', filename)
    
    # Ensure the name does not start or end with a period or dash
    sanitized = sanitized.strip('-.')
    
    # Avoid consecutive dashes or underscores
    sanitized = re.sub(r'[-_]+', '_', sanitized)
    
    return sanitized

def set_file_metadata(local_path, modified_time):
    """Set file metadata to preserve original modified time."""
    os.utime(local_path, (modified_time, modified_time))

def flatten_directory_structure(base_path):
    """Flatten nested directory structure."""
    for root, dirs, files in os.walk(base_path):
        for file in files:
            current_path = os.path.join(root, file)
            # Move the file to the base_path if it's nested
            new_path = os.path.join(base_path, file)
            if current_path != new_path:
                shutil.move(current_path, new_path)
                cl.monitor_logger.info(f"Flattened file from {current_path} to {new_path}")
        # Remove empty directories
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                cl.monitor_logger.info(f"Removed empty directory: {dir_path}")

async def handle_file(local_path, server_folder, file_name, file_type, file_size, modified_time):
    """Handle a file after download, including metadata setting and uploading."""
    try:
        set_file_metadata(local_path, modified_time)

        # If the file is a zip, extract it
        if local_path.endswith(".zip"):
            extracted_dir = extract_zip(local_path, server_folder)
            # Handle each extracted file individually
            flatten_directory_structure(extracted_dir)  # Flatten the structure first
            for root, _, files in os.walk(extracted_dir):
                for extracted_file in files:
                    extracted_file_path = os.path.join(root, extracted_file)
                    extracted_file_type = extracted_file.split('.')[-1]
                    set_file_metadata(extracted_file_path, modified_time)
                    await upload_file(extracted_file_path, server_folder, extracted_file, extracted_file_type, os.path.getsize(extracted_file_path), modified_time)
            # Ensure cleanup of extracted directory
            cleanup_file(extracted_dir)
        else:
            await upload_file(local_path, server_folder, file_name, file_type, file_size, modified_time)
    finally:
        # Ensure cleanup of downloaded or extracted file regardless of success
        cleanup_file(local_path)

def extract_zip(zip_path, server_folder):
    """Extract a zip file to a temporary local directory."""
    extracted_dir = os.path.join(config.LOCAL_DOWNLOAD_DIR, f"extracted_{Path(zip_path).stem}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_dir)
    return extracted_dir

async def download_file(server, remote_path, semaphore):
    async with semaphore:
        cl.monitor_logger.info(f"Connecting to server: {server}")
        try:
            pass

        except FileNotFoundError as e:
            # Log specific file not found error
            cl.error_logger.error(f"File not found on server {server}. Remote path: {remote_path}. Error: {e}")
        except Exception as e:
            # Log general connection or other errors
            cl.error_logger.error(f"Failed to connect to server {server}. Remote path: {remote_path}. Error: {e}")

async def download_file(server, remote_path, semaphore):
    """Download a file from a server and initiate handling."""
    async with semaphore:
        cl.monitor_logger.info(f"Connecting to server: {server} and path {remote_path}")

        try:
            server_folder = get_server_folder_name(server)
            file_name = sanitize_filename(remote_path.split('/')[-1])
            file_type = file_name.split('.')[-1]
            if not file_type:
                file_type = "none"

            cl.monitor_logger.info(f"Remote filename: {file_name} and file_type {file_type}")

            # Ensure the base directory is only named by server folder and type
            local_dir = os.path.join(config.LOCAL_DOWNLOAD_DIR, server_folder, file_type)
            local_path = os.path.join(local_dir, file_name)
            cl.monitor_logger.info(f"Local dir: {local_dir} and local file_name {file_name} make {local_path}")
            os.makedirs(local_dir, exist_ok=True)

            # Parse server details
            parsed = urlparse(server)
            host, port = parsed.hostname, parsed.port or (21 if parsed.scheme == "ftp" else 22)
            username, password = parsed.username, parsed.password

            cl.monitor_logger.info(f"Parsed server details: host={host}, port={port}, username={username}")

            # Download file based on protocol
            if parsed.scheme == "ftp":
                client = aioftp.Client()
                await client.connect(host, port)
                await client.login(user=username, password=password)
                cl.monitor_logger.info(f"FTP connection established to {server}")
                
                # Fetch file_info and log it for debugging
                file_info = await client.stat(remote_path)
                cl.monitor_logger.debug(f"FTP file info: {file_info}")

                # Check if it's actually a file
                if file_info['type'] != 'file':
                    cl.error_logger.error(f"{remote_path} is not a file on the server. Detected type: {file_info['type']}")
                    return
                
                cl.monitor_logger.info(f"ftp: {remote_path} => {local_path} for {file_name}")

                await client.download(remote_path, local_path)
                modified_str = file_info['modify']
                modified_time = datetime.strptime(modified_str, "%Y%m%d%H%M%S").timestamp()
                file_size = file_info['size']
                await client.quit()

            elif parsed.scheme == "sftp":
                async with asyncssh.connect(host, port=port, username=username, password=password, known_hosts=None) as conn:
                    async with conn.start_sftp_client() as sftp:
                        cl.monitor_logger.info(f"SFTP connection established to {server}")
                        file_info = await sftp.stat(remote_path)
                        await sftp.get(remote_path, local_path)
                        file_size, modified_time = file_info.size, file_info.mtime
            else:
                cl.error_logger.error(f"Unsupported protocol for server: {server}")
                return

            # Detect and fix any issue where local_path is a directory instead of a file
            if os.path.isdir(local_path):
                # List the files in the directory
                files_inside_dir = os.listdir(local_path)
                
                # If the directory contains exactly one file and matches the expected file_name
                if len(files_inside_dir) == 1 and files_inside_dir[0] == file_name:
                    actual_file_path = os.path.join(local_path, files_inside_dir[0])
                    
                    # Move the file to the correct `local_dir`
                    os.rename(actual_file_path, local_path)
                    cl.monitor_logger.info(f"Moved file {actual_file_path} to {local_path}")
                    
                    # Remove the empty directory
                    os.rmdir(local_path)
                else:
                    cl.error_logger.error(f"Unexpected directory structure at {local_path}. Skipping further processing.")
                    return

            # Verification step: check file size and ensure it's a regular file
            if not os.path.isfile(local_path):
                cl.error_logger.error(f"{local_path} is not a file. Skipping further processing.")
                return

            if os.path.getsize(local_path) != file_size:
                cl.error_logger.error(f"{local_path} is not a complete or valid file. Expected size: {file_size}, Actual size: {os.path.getsize(local_path)}")
                os.remove(local_path)  # Clean up incomplete download
                return

            # Log successful download details
            cl.monitor_logger.info(f"Downloaded {remote_path} from {parsed.scheme.upper()} server {server} to {local_path}")

            # Pass the downloaded file for further handling
            await handle_file(local_path, server_folder, file_name, file_type, file_size, modified_time)

        except FileNotFoundError as e:
            # Log specific file not found error
            cl.error_logger.error(f"File not found on server {server}. Remote path: {remote_path}. Error: {e}")
        except Exception as e:
            # Log general connection or other errors
            cl.error_logger.error(f"Failed to connect to server {server}. Remote path: {remote_path}. Error: {e}")


async def upload_file(local_path, server_folder, file_name, file_type, file_size, modified_time):
    """Upload a file to Azure Blob Storage and perform integrity checks."""
    try:
        container_name = config.AZURE_CONTAINER_NAME
        
        # Sanitize each part of the blob path for Azure compatibility
        server_folder_sanitized = sanitize_filename(server_folder)
        file_name_sanitized = sanitize_filename(file_name)
        
        blob_path = f"{server_folder_sanitized}/{file_type}/{int(time.time())}/{file_name_sanitized}"

        # Log the full details of the blob path for debugging
        cl.monitor_logger.info(f"Preparing to upload: {local_path}")
        cl.monitor_logger.info(f"Sanitized Azure blob path: {blob_path}, Original: server_folder={server_folder}, file_name={file_name}")
        cl.monitor_logger.info(f"File size: {file_size}, Modified time: {modified_time}")

        # Check if the file already exists in Azure
        if await is_duplicate_in_azure(server_folder, file_type, file_name, file_size, modified_time):
            cl.monitor_logger.info(f"Skipping upload for {local_path} as duplicate already exists in Azure.")
            return

        cl.monitor_logger.info(f"Uploading {local_path} to Azure as {blob_path}")

        # Ensure the local_path is a file
        if not os.path.isfile(local_path):
            cl.error_logger.error(f"{local_path} is not a file. Skipping upload.")
            return

        # Upload the file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                content_settings=ContentSettings(),
                overwrite=True
            )

        cl.monitor_logger.info(f"Successfully uploaded {local_path} to Azure as {blob_path}")

        # Perform data integrity check after upload
        await verify_integrity_in_azure(blob_client, local_path, file_size)

    except Exception as e:
        cl.error_logger.error(f"Error uploading file {local_path} to Azure: {e}")

def cleanup_file(local_path):
    """Clean up a local file or directory after processing."""
    try:
        # Check if the path is a file
        if os.path.isfile(local_path):
            cl.monitor_logger.info(f"Cleaning up local file {local_path}")
            os.remove(local_path)
        # If the path is a directory (in case of extracted files)
        elif os.path.isdir(local_path):
            cl.monitor_logger.info(f"Cleaning up local directory {local_path}")
            shutil.rmtree(local_path)
        else:
            cl.error_logger.error(f"Cannot clean up {local_path} as it is neither a file nor a directory.")
    except Exception as e:
        cl.error_logger.error(f"Error cleaning up {local_path}: {e}")

async def verify_integrity_in_azure(blob_client, local_path, file_size):
    """Verify the integrity of the uploaded file by checking its size."""
    try:
        # Retrieve properties without using 'await'
        properties = blob_client.get_blob_properties()
        if properties.size == file_size:
            cl.monitor_logger.info(f"Integrity check passed for {local_path}")
        else:
            cl.error_logger.error(f"Integrity check failed for {local_path}")
    except Exception as e:
        cl.error_logger.error(f"Error verifying integrity of {local_path} in Azure: {e}")

async def is_duplicate_in_azure(server_folder, file_type, file_name, file_size, modified_time):
    """Check Azure storage to see if a duplicate file already exists based on metadata."""
    try:
        container_name = config.AZURE_CONTAINER_NAME
        blobs = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=f"{server_folder}/{file_type}/")

        # Compare blob metadata to detect duplicates
        for blob in blobs:
            blob_name = os.path.basename(blob.name)
            blob_properties = blob_service_client.get_blob_client(container=container_name, blob=blob.name).get_blob_properties()
            if (blob_name == file_name and blob_properties.size == file_size and 
                blob_properties.last_modified.timestamp() == modified_time):
                cl.monitor_logger.info(f"Duplicate file found in Azure: {blob.name}")
                return True

        return False
    except Exception as e:
        cl.error_logger.error(f"Failed to check for duplicates in Azure: {e}")
        return False

def process_batch(batch):
    """Process a batch of files concurrently with controlled concurrency."""
    asyncio.run(process_files_concurrently(batch, asyncio.Semaphore(config.CHILD_PROCESS["max_concurrent_tasks"])))

async def process_files_concurrently(batch, semaphore):
    """Process all files in a batch concurrently using asyncio."""
    await asyncio.gather(*[download_file(server, file, semaphore) for server, file in batch])
