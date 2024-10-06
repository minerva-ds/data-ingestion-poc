import unittest
from azure.storage.blob import BlobServiceClient
from unittest.mock import patch 
import os
import shutil
import child
import config
from sources import SOURCES

# Change to the parent directory to ensure paths are consistent
os.chdir(os.path.dirname(os.path.abspath(__file__)) + "/..")

# Directory where test files will be created, using config value
DOWNLOAD_DIR = config.LOCAL_DOWNLOAD_DIR
FTP_URL = "ftp://user:password@localhost:2121"  # Use config value if different
FTP_ZIP_FILE = "/test_file.zip"  # Use config value if different

class TestSanitizeFilename(unittest.TestCase):
    def test_special_characters(self):
        test_str = 'file@name!.zip'
        expected = 'file-name-.zip'  # Special characters replaced by hyphens
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_spaces(self):
        test_str = '   file name with spaces.txt   '
        expected = 'file name with spaces.txt'  # Spaces are preserved
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_multiple_dots(self):
        test_str = 'file.name.with.dots.zip'
        expected = 'file.name.with.dots.zip'  # Preserve dots within name
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_trailing_dashes_and_underscores(self):
        test_str = '___filename--.txt'
        expected = '___filename--.txt'  # Preserve leading underscores and hyphens as is
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_disallowed_characters(self):
        test_str = 'file/name\\with?illegal%chars*here:too|and"quotes<and>more.txt'
        expected = 'file-name-with-illegal-chars-here-too-and-quotes-and-more.txt'
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_control_characters(self):
        test_str = 'filename\x00with\x1Fcontrolchars.txt'
        expected = 'filename-with-controlchars.txt'  # Control characters replaced by hyphens
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_unicode_characters(self):
        test_str = 'fileñame😀with_unicode_chars.txt'
        expected = 'file-ame-with_unicode_chars.txt'  # replace non-ASCII characters with -
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_leading_trailing_hyphens(self):
        test_str = '-filename-.txt-'
        expected = '-filename-.txt-'  # Keep leading/trailing hyphens as is
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)

    def test_multiple_hyphens(self):
        test_str = 'file--name---with--multiple---hyphens.txt'
        expected = 'file--name---with--multiple---hyphens.txt'  # Keep multiple hyphens as is
        result = child.sanitize_filename(test_str)
        print(f"{test_str} => {result} | Expected: {expected}")
        self.assertEqual(result, expected)
class TestDownloadFileWithPycurl(unittest.TestCase):
    def setUp(self):
        # Extract the first server and file from the SOURCES dictionary
        self.server = next(iter(SOURCES))  # Get the first key (server) from SOURCES
        self.remote_file = SOURCES[self.server][0]  # Get the first file path from that server

        # Create downloads directory if it doesn't exist
        os.makedirs(config.LOCAL_DOWNLOAD_DIR, exist_ok=True)

        # Define the local path for the downloaded file
        self.local_path = os.path.join(config.LOCAL_DOWNLOAD_DIR, os.path.basename(self.remote_file))

    def tearDown(self):
        # Cleanup: remove the downloaded file
        if os.path.exists(self.local_path):
            os.remove(self.local_path)

    def test_download_from_sources(self):
        # Construct the full download URL
        download_url = self.server + self.remote_file
        try:
            # Download the file from the FTP/SFTP server
            child.download_file_with_pycurl(download_url, self.local_path)

            # Assert that the file was downloaded successfully
            self.assertTrue(os.path.exists(self.local_path))
            self.assertGreater(os.path.getsize(self.local_path), 0, "Downloaded file is empty")

        except Exception as e:
            # If any error occurs, fail the test with an appropriate message
            self.fail(f"Failed to download from {download_url}: {e}")

class TestHandleZipFile(unittest.TestCase):
    def setUp(self):
        # Create downloads directory if it doesn't exist
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        # Define the local path for the downloaded zip file
        self.zip_path = os.path.join(DOWNLOAD_DIR, os.path.basename(FTP_ZIP_FILE))
        self.extracted_dir = os.path.join(DOWNLOAD_DIR, f"extracted_{os.path.splitext(os.path.basename(self.zip_path))[0]}")

        # Download the zip file from the FTP server if not exists
        download_url = FTP_URL + FTP_ZIP_FILE
        if not os.path.exists(self.zip_path):
            child.download_file_with_pycurl(download_url, self.zip_path)

    def tearDown(self):
        # Cleanup: remove the zip file and extracted directory
        if os.path.exists(self.zip_path):
            os.remove(self.zip_path)
        if os.path.exists(self.extracted_dir):
            shutil.rmtree(self.extracted_dir)

    @patch('child.handle_file')
    def test_zip_extraction(self, mock_handle_file):
        # Extract the zip file
        child.handle_zip_file(self.zip_path, DOWNLOAD_DIR, 'server_folder', 'zip')
        
        # Check that files were extracted and handled correctly
        self.assertTrue(os.path.exists(self.extracted_dir))
        mock_handle_file.assert_called()

class TestUploadFile(unittest.TestCase):
    def setUp(self):
        # Create downloads directory if it doesn't exist
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        
        # Create a dummy file in the downloads folder
        self.local_path = os.path.join(DOWNLOAD_DIR, "test_upload.txt")
        with open(self.local_path, "w") as f:
            f.write("Test content for upload")

        # Azure Blob Service client
        self.blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)
        self.container_name = config.AZURE_CONTAINER_NAME
        self.server_folder = 'test_server_folder'
        self.file_name = 'test_upload.txt'
        self.file_type = 'txt'

    def tearDown(self):
        # Cleanup: remove the created file locally and in Azure Blob
        if os.path.exists(self.local_path):
            os.remove(self.local_path)
        
        # Delete the blob from Azure container
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, 
                                                                   blob=f"{self.server_folder}/{self.file_type}/{self.file_name}")
            blob_client.delete_blob()
        except Exception as e:
            print(f"Cleanup warning: Failed to delete blob - {e}")

    def test_upload_file(self):
        # Upload the file to Azure
        child.upload_file(self.local_path, self.server_folder, self.file_name, self.file_type)

        # Check if the file was uploaded successfully
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, 
                                                               blob=f"{self.server_folder}/{self.file_type}/{self.file_name}")
        blob_exists = blob_client.exists()

        self.assertTrue(blob_exists, "The file was not uploaded to Azure Blob Storage as expected. If testing make sure the service is running locally also check config.py for proper connection settings.")



if __name__ == '__main__':
    unittest.main()
