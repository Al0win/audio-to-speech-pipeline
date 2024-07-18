import datetime
import glob
import multiprocessing
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join
from azure.storage.blob import BlobServiceClient

class CloudStorageOperations:
    @staticmethod
    def get_instance(config_dict, **kwargs):
        return CloudStorageOperations(config_dict, **kwargs)

    def __init__(self, config_dict, **kwargs):
        self.config_dict = config_dict
        self._container_name = None
        self._client = None

    @property
    def client(self):
        if self._client:
            return self._client
        self._client = BlobServiceClient.from_connection_string(self.config_dict["common"]["azure_config"]["connection_string"])
        return self._client

    @property
    def container_name(self):
        if self._container_name:
            return self._container_name
        self._container_name = self.config_dict.get("common", {}).get("azure_config", {}).get("container_name")
        return self._container_name

    def check_path_exists(self, path):
        """Check if a blob exists."""
        blob_client = self.client.get_blob_client(container=self.container_name, blob=path)
        return blob_client.exists()

    def copy_all_files(self, src, dest, audio_extn):
        src_files = glob.glob(src + "/*." + audio_extn)
        for file_name in src_files:
            meta_file_name = (
                "/".join(file_name.split("/")[:-1])
                + "/"
                + file_name.split("/")[-1].split(".")[0]
                + ".csv"
            )
            full_meta_file_name = os.path.join(src, meta_file_name)
            full_file_name = os.path.join(src, file_name)
            if os.path.isfile(full_file_name) and os.path.isfile(full_meta_file_name):
                destination = dest + "/" + self.get_audio_id()
                self.make_directories(destination)
                shutil.copy(full_file_name, destination)
                shutil.copy(full_meta_file_name, destination)

    def get_audio_id(self):
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-2]

    def make_directories(self, path):
        os.makedirs(path, exist_ok=True)

    def download_to_local(self, source_blob_name, destination, is_directory, exclude_extn=None):
        """Downloads a blob from Azure Blob Storage."""
        if is_directory:
            os.makedirs(destination, exist_ok=True)
            blobs = self.client.get_container_client(self.container_name).list_blobs(name_starts_with=source_blob_name)
            for blob in blobs:
                if not blob.name.endswith("/") and (exclude_extn is None or not blob.name.endswith(exclude_extn)):
                    blob.download_to_file(open(os.path.join(destination, os.path.basename(blob.name)), "wb"))
        else:
            blob_client = self.client.get_blob_client(container=self.container_name, blob=source_blob_name)
            with open(destination, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())

    def upload_to_az(self, local_source_path, destination_blob_name, upload_directory=True):
        """Uploads a file or directory to Azure Blob Storage."""
        container_client = self.client.get_container_client(self.container_name)
        
        if not upload_directory:
            blob_client = container_client.get_blob_client(destination_blob_name)
            blob_client.upload_blob(open(local_source_path, "rb"), overwrite=True)
            return True

        files = [f for f in listdir(local_source_path) if isfile(join(local_source_path, f))]
        estimated_cpu_share = 0.05
        concurrency = multiprocessing.cpu_count() / estimated_cpu_share
        executor = ThreadPoolExecutor(max_workers=concurrency)

        futures = []
        for file in files:
            src_file = os.path.join(local_source_path, file)
            blob_client = container_client.get_blob_client(destination_blob_name + "/" + file)
            futures.append(executor.submit(blob_client.upload_blob, open(src_file, "rb"), overwrite=True))

        executor.shutdown(wait=True)
        for upload_future in futures:
            upload_future.result()
        return True

    def list_blobs(self, bucket_name, prefix, delimiter=None):
        """Lists all blobs in the specified container."""
        blobs = self.client.get_container_client(bucket_name).list_blobs(name_starts_with=prefix)
        for blob in blobs:
            print(blob.name)

    def rename_blob(self, blob_name, new_name):
        """Renames a blob."""
        source_blob = self.client.get_blob_client(container=self.container_name, blob=blob_name)
        new_blob = self.client.get_blob_client(container=self.container_name, blob=new_name)
        new_blob.start_copy_from_url(source_blob.url)
        source_blob.delete_blob()

    def copy_blob(self, blob_name, destination_blob_name, destination_bucket_name=None):
        """Copies a blob within the same container or to a different one."""
        source_blob = self.client.get_blob_client(container=self.container_name, blob=blob_name)
        destination_blob = self.client.get_blob_client(container=destination_bucket_name or self.container_name, blob=destination_blob_name)
        destination_blob.start_copy_from_url(source_blob.url)

    def list_blobs_in_a_path(self, file_prefix, delimiter=None):
        """Lists blobs in a specific path."""
        return list(self.client.get_container_client(self.container_name).list_blobs(name_starts_with=file_prefix))

    def move_blob(self, blob_name, destination_blob_name, destination_bucket_name=None):
        """Moves a blob by copying and then deleting it."""
        self.copy_blob(blob_name, destination_blob_name, destination_bucket_name)
        self.client.get_blob_client(container=self.container_name, blob=blob_name).delete_blob()

    def copy_blob_file(self, blob_name, destination_blob_name, destination_bucket_name=None):
        """Copies a blob file."""
        self.copy_blob(blob_name, destination_blob_name, destination_bucket_name)

    @staticmethod
    def copy_blob_for_move(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
        """Copies a blob from one container to another."""
        source_blob_client = BlobServiceClient.from_connection_string(config_dict["common"]["azure_config"]["connection_string"]).get_blob_client(container=bucket_name, blob=blob_name)
        destination_blob_client = BlobServiceClient.from_connection_string(config_dict["common"]["azure_config"]["connection_string"]).get_blob_client(container=destination_bucket_name, blob=destination_blob_name)
        destination_blob_client.start_copy_from_url(source_blob_client.url)
        return source_blob_client

    def delete_object(self, dir_path):
        """Deletes blobs in a specified path."""
        blobs = self.list_blobs_in_a_path(dir_path)
        for blob in blobs:
            self.client.get_blob_client(container=self.container_name, blob=blob.name).delete_blob()

    def download_blob(self, source_blob_name, destination_file_name):
        """Downloads a blob from the container."""
        blob_client = self.client.get_blob_client(container=self.container_name, blob=source_blob_name)
        with open(destination_file_name, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
