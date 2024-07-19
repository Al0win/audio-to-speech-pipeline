import os
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from tqdm import tqdm
from ekstep_data_pipelines.common.infra_commons.storage import BaseStorageInterface
from ekstep_data_pipelines.common.infra_commons.storage.exceptions import (
    FileNotFoundException,
)
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("AzureStorage")


class AzureStorage(BaseStorageInterface):
    def __init__(self, **kwargs):
        self._client = None
        self.connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')  # Ensure this is set in your environment

    def get_container_from_path(self, path) -> str:
        if not path:
            return None

        splitted_path = list(filter(None, path.split("/")))

        if len(splitted_path) < 1:
            return None

        return splitted_path[0]

    def get_path_without_container(self, path_with_container: str) -> str:
        if not path_with_container:
            return None

        splitted_path = list(filter(None, path_with_container.split("/")))

        if len(splitted_path) < 1:
            return None

        return "/".join(splitted_path[1:])

    @property
    def client(self):
        if self._client:
            return self._client

        self._client = BlobServiceClient.from_connection_string(self.connection_string)
        return self._client

    def list_files(self, source_path: str, include_folders=True):
        container_name = self.get_container_from_path(source_path)
        actual_path = self.get_path_without_container(source_path)

        if actual_path and actual_path[-1] != "/":
            actual_path = actual_path + "/"

        blob_name_set = set()
        is_folder = False
        container_client = self.client.get_container_client(container_name)
        
        for blob in container_client.list_blobs(name_starts_with=actual_path):
            if ".wav" in blob.name:
                content = blob.name.replace(actual_path, "")

                if not content:
                    continue

                if "/" in content[:-1]:
                    if include_folders:
                        content = content.split("/")[0] + "/"
                        is_folder = True
                    else:
                        continue

                if not is_folder:
                    content = content.replace("/", "")

                if content.replace("/", "") in blob_name_set:
                    blob_name_set.remove(content.replace("/", ""))

                blob_name_set.add(content)

        return list(blob_name_set)

    def download_to_location(self, source_path: str, destination_path: str):
        return self.download_file_to_location(source_path, destination_path)

    def download_folder_to_location(self, source_path: str, destination_path: str, max_workers=5):
        container = self.get_container_from_path(source_path)
        source = "/".join(source_path.split("/")[1:])
        Logger.info("container:%s", container)
        Logger.info("source:%s", source)
        source_files = self._list_blobs_in_a_path(container, source)
        Logger.info("file:%s", str(source_files))
        curr_executor = ThreadPoolExecutor(max_workers)

        for remote_file in tqdm(source_files):
            remote_file_path = remote_file.name
            remote_file_name = remote_file_path.split("/")[-1]

            if remote_file_name == "" or remote_file.size <= 0:
                continue

            curr_executor.submit(
                self.download_to_location,
                f"{container}/{remote_file_path}",
                f"{destination_path}/{remote_file_name}",
            )

        curr_executor.shutdown(wait=True)

    def upload_to_location(self, local_source_path: str, destination_path: str):
        container = self.client.get_container_client(self.get_container_from_path(destination_path))
        remote_file_path = self.get_path_without_container(destination_path)
        blob_client = container.get_blob_client(remote_file_path)

        try:
            with open(local_source_path, "rb") as data:
                blob_client.upload_blob(data)
        except Exception as exception:
            return False

        return True

    def upload_folder_to_location(self, source_path: str, destination_path: str):
        files_for_upload = [f for f in listdir(source_path) if isfile(join(source_path, f))]
        curr_executor = ThreadPoolExecutor(max_workers=5)

        for upload_file in files_for_upload:
            curr_executor.submit(
                self.upload_to_location,
                f"{source_path}/{upload_file}",
                f"{destination_path}/{upload_file}",
            )

        curr_executor.shutdown(wait=True)
        return True

    def download_file_to_location(self, source_path: str, download_location: str):
        container = self.client.get_container_client(self.get_container_from_path(source_path))
        file_path = self.get_path_without_container(source_path)
        blob_client = container.get_blob_client(file_path)
        if self.path_exists(source_path):
            with open(download_location, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

    def move(self, source_path: str, destination_path: str) -> bool:
        copied = self.copy(source_path, destination_path)

        if not copied:
            return False

        return self.delete(source_path)

    def copy(self, source_path: str, destination_path: str) -> bool:
        if not self.path_exists(source_path):
            raise FileNotFoundException(f"File at {source_path} not found")

        source_container = self.client.get_container_client(self.get_container_from_path(source_path))
        source_actual_path = self.get_path_without_container(source_path)
        source_blob_client = source_container.get_blob_client(source_actual_path)

        destination_container = self.client.get_container_client(self.get_container_from_path(destination_path))
        destination_actual_path = self.get_path_without_container(destination_path)
        destination_blob_client = destination_container.get_blob_client(destination_actual_path)

        copy_source = source_blob_client.url
        destination_blob_client.start_copy_from_url(copy_source)
        return True

    def delete(self, path: str) -> bool:
        container_name = self.get_container_from_path(path)
        container_client = self.client.get_container_client(container_name)
        actual_path = self.get_path_without_container(path)

        all_files = self._list_blobs_in_a_path(container_name, actual_path)

        for file in all_files:
            blob_client = container_client.get_blob_client(file.name)
            blob_client.delete_blob()
            print(f"Blob {file.name} deleted.")

        return True

    def path_exists(self, path: str) -> bool:
        container = self.client.get_container_client(self.get_container_from_path(path))
        actual_path = self.get_path_without_container(path)
        try:
            path_exists = container.get_blob_client(actual_path).exists()
        except BaseException:
            return False
        return path_exists

    def _list_blobs_in_a_path(self, container, file_prefix, delimiter=None):
        container_client = self.client.get_container_client(container)
        blobs = container_client.list_blobs(name_starts_with=file_prefix)
        return blobs

    def list_blobs_in_a_path(self, full_path, delimiter=None):
        container = self.client.get_container_client(self.get_container_from_path(full_path))
        file_prefix = self.get_path_without_container(full_path)
        blobs = container.list_blobs(name_starts_with=file_prefix, delimiter=delimiter)
        return blobs
