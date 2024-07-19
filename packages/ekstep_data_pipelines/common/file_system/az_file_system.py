from azure.storage.blob import BlobServiceClient
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("AzureFileSystem")


class AzureFileSystem:
    def __init__(self, connection_string):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def ls(self, container_name, dir_path):
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=dir_path)
        return [blob.name for blob in blob_list]

    def mv(self, container_name, source_dir, target_dir):
        Logger.info("Moving path %s --> %s", source_dir, target_dir)
        files = self.ls(container_name, source_dir)
        for file in files:
            self.mv_file(container_name, file, target_dir)

    def mv_file(self, container_name, file, target_dir):
        paths = file.split("/")
        paths.pop()
        source_dir = "/".join(paths)
        destination_blob_name = file.replace(source_dir, target_dir, 1)
        Logger.info("Moving file %s --> %s", file, destination_blob_name)
        self.copy_file(container_name, file, destination_blob_name)
        self.delete_file(container_name, file)

    def copy_file(self, container_name, file, target_dir):
        container_client = self.blob_service_client.get_container_client(container_name)
        source_blob = container_client.get_blob_client(file)
        destination_blob_name = file.replace(source_dir, target_dir, 1)
        destination_blob = container_client.get_blob_client(destination_blob_name)

        Logger.info("Copying file %s --> %s", file, destination_blob_name)

        destination_blob.start_copy_from_url(source_blob.url)

        copy_props = destination_blob.get_blob_properties().copy
        while copy_props['status'] != 'success':
            copy_props = destination_blob.get_blob_properties().copy

    def delete_file(self, container_name, file):
        container_client = self.blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(file)
        Logger.info("Deleting file %s", file)
        blob_client.delete_blob()
