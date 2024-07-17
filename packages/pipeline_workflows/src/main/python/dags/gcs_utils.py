from os import listdir
from os.path import isfile, join
from azure.storage.blob import BlobServiceClient

# Initialize BlobServiceClient
connection_string = "your_connection_string"  # Replace with your Azure connection string
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def upload_files(container_name, srcFolderPath, containerFolder):
    """Upload files to Azure Blob Storage container."""
    files = [f for f in listdir(srcFolderPath) if isfile(join(srcFolderPath, f))]
    container_client = blob_service_client.get_container_client(container_name)
    
    for file in files:
        srcFile = join(srcFolderPath, file)
        print("file path: ", srcFile)
        blob_client = container_client.get_blob_client(containerFolder + file)
        
        with open(srcFile, "rb") as data:
            blob_client.upload_blob(data)
        
        print("Uploaded to: ", containerFolder + file)
    
    return f'Uploaded {files} to "{container_name}" container.'

def download_blob(container_name, blob_name, destination_file_name):
    """Downloads a blob from the container."""
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    with open(destination_file_name, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    print(f"Blob {blob_name} from container {container_name} downloaded to {destination_file_name}.")

def list_blobs(container_name, prefix=None):
    """Lists all the blobs in the container."""
    container_client = blob_service_client.get_container_client(container_name)
    
    blob_list = container_client.list_blobs(name_starts_with=prefix)
    for blob in blob_list:
        print(blob.name)

def rename_blob(container_name, blob_name, new_name):
    """Renames a blob."""
    copy_blob(container_name, blob_name, container_name, new_name)
    delete_blob(container_name, blob_name)

def copy_blob(src_container_name, src_blob_name, dest_container_name, dest_blob_name):
    """Copies a blob from one container to another with a new name."""
    src_blob_client = blob_service_client.get_blob_client(src_container_name, src_blob_name)
    dest_blob_client = blob_service_client.get_blob_client(dest_container_name, dest_blob_name)

    dest_blob_client.start_copy_from_url(src_blob_client.url)
    print(f"Blob {src_blob_name} in container {src_container_name} copied to blob {dest_blob_name} in container {dest_container_name}.")

def delete_blob(container_name, blob_name):
    """Deletes a blob from the container."""
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.delete_blob()
    print(f"Blob {blob_name} deleted from container {container_name}.")

if __name__ == "__main__":
    pass
