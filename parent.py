from multiprocessing import Pool
import os
import time

# Azure imports
from azure.storage.blob import BlobServiceClient

# Local imports
from sources import SOURCES
import config
import custom_logging as cl
import child

def ensure_container_exists():
    """Ensure the Azure container exists."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(config.AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(config.AZURE_CONTAINER_NAME)

        # Check if the container exists
        if not container_client.exists():
            container_client.create_container()
            cl.monitor_logger.info(f"Created Azure container: {config.AZURE_CONTAINER_NAME}")
        else:
            cl.monitor_logger.info(f"Azure container already exists: {config.AZURE_CONTAINER_NAME}")

    except Exception as e:
        cl.error_logger.error(f"Error ensuring Azure container exists: {e}")
        raise

def process_batch_completed(result):
    """Callback function to be executed when a batch process completes."""
    cl.monitor_logger.info(f"Batch process completed with result: {result}")

def process_batch_with_logging(batch, batch_number):
    """Wrapper around child.process_batch to add logging for start and end times."""
    cl.monitor_logger.info(f"Batch {batch_number + 1} started processing.")
    start_time = time.time()

    try:
        # Process the batch using the existing function
        child.process_batch(batch)
        success = True
    except Exception as e:
        cl.error_logger.error(f"Error in batch {batch_number + 1}: {e}")
        cl.monitor_logger.error(f"Batch {batch_number + 1} failed due to error.")
        success = False

    # Calculate elapsed time for processing
    elapsed_time = time.time() - start_time
    if success:
        cl.monitor_logger.info(f"Batch {batch_number + 1} completed successfully in {elapsed_time:.2f} seconds.")
    else:
        cl.monitor_logger.error(f"Batch {batch_number + 1} failed in {elapsed_time:.2f} seconds.")
    
    return success

def ingest_files():
    # Ensure the log directory exists
    os.makedirs(config.LOCAL_DOWNLOAD_DIR, exist_ok=True)

    # Ensure the Azure container exists
    ensure_container_exists()

    batches = [[] for _ in range(config.BATCH_SIZE)]
    batch_index = 0

    # Iterate over each server and their files
    for server, file_list in SOURCES.items():
        for file in file_list:
            # Add file to the current batch
            batches[batch_index].append((server, file))

            # Add batches round robin style
            batch_index = (batch_index + 1) % config.BATCH_SIZE

    # Filter out empty batches
    batches = [batch for batch in batches if batch]
    
    # Log the total number of batches to be processed
    total_batches = len(batches)
    cl.monitor_logger.info(f"Total batches to process: {total_batches}")

    # Track number of successful and failed batches
    successful_batches = 0
    failed_batches = 0

    # Use multiprocessing Pool, automatically handles creating a queue and running waiting batches
    with Pool(processes=config.MAX_PARALLEL_PROCESSES) as pool:
        results = []
        for batch_number, batch in enumerate(batches):
            results.append(pool.apply_async(
                process_batch_with_logging, 
                args=(batch, batch_number), 
                callback=process_batch_completed
            ))

        pool.close()
        pool.join()

        # Count successes and failures
        for result in results:
            if result.get():
                successful_batches += 1
            else:
                failed_batches += 1

    # Log summary at the end of the entire process
    cl.monitor_logger.info(f"Batch processing complete. {successful_batches} succeeded, {failed_batches} failed out of {total_batches} total batches.")

if __name__ == "__main__":
    cl.monitor_logger.info(f"Started ingesting files with pid {os.getpid()}")
    ingest_files()