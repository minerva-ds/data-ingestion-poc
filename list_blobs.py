# list_blobs.py

from azure.storage.blob import BlobServiceClient
import config

def list_blobs():
    # Connect to the BlobServiceClient using the connection string from config
    blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)

    # Get container client for specified container
    container_client = blob_service_client.get_container_client(config.AZURE_CONTAINER_NAME)

    # List and print blobs in the container
    print(f"Listing blobs in container: {config.AZURE_CONTAINER_NAME}")
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        print(f"- {blob.name}")

if __name__ == "__main__":
    list_blobs()
