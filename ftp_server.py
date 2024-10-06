import os
import zipfile
import pyftpdlib.authorizers
import pyftpdlib.handlers
import pyftpdlib.servers
from pathlib import Path

# Configuration
ftp_dir = './ftp'
zip_file_name = 'test_file.zip'
zip_file_size_mb = 5
ftp_user = 'user'
ftp_password = 'password'
ftp_port = 2121

# Step 1: Create the FTP directory if it doesn't exist
os.makedirs(ftp_dir, exist_ok=True)

# Step 2: Create a 5MB zip file in the FTP directory
zip_file_path = os.path.join(ftp_dir, zip_file_name)
if not os.path.exists(zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        # Create a file with random content
        temp_file_path = os.path.join(ftp_dir, 'temp_file.txt')
        with open(temp_file_path, 'wb') as tempf:
            tempf.write(os.urandom(zip_file_size_mb * 1024 * 1024))  # 5MB file
        zipf.write(temp_file_path, arcname='temp_file.txt')
        os.remove(temp_file_path)  # Clean up temporary file

# Step 3: Set up and start an FTP server
def start_ftp_server():
    # Define an authorizer for handling authentication
    authorizer = pyftpdlib.authorizers.DummyAuthorizer()
    # Add a user with full permissions to the FTP server
    authorizer.add_user(ftp_user, ftp_password, ftp_dir, perm='elradfmw')

    # Instantiate an FTP handler and associate it with the authorizer
    handler = pyftpdlib.handlers.FTPHandler
    handler.authorizer = authorizer

    # Create the FTP server using the handler and specify the port
    address = ('0.0.0.0', ftp_port)
    server = pyftpdlib.servers.FTPServer(address, handler)

    print(f"Starting FTP server on port {ftp_port}, serving directory: {ftp_dir}")
    print(f"Use FTP credentials: user = '{ftp_user}', password = '{ftp_password}'")
    
    # Start the FTP server
    server.serve_forever()

if __name__ == '__main__':
    start_ftp_server()