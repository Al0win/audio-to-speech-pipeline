import sys
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common import BaseProcessor
from ekstep_data_pipelines.audio_embedding.create_embeddings import (
    encode_each_batch
)

LOGGER = get_logger("AudioEmbeddingProcessor")

ESTIMATED_CPU_SHARE = 0.1

class AudioEmbedding(BaseProcessor):
    """
    Class to identify speaker for each utterance in a source
    """

    local_txt_path = "./audio_speaker_cluster/file_path/"
    local_audio_path = "./audio_speaker_cluster/audio_files/"
    embed_file_path = "./audio_speaker_cluster/embed_file_path/"

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return AudioEmbedding(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.audio_analysis_config = None
        self.blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))

        super().__init__(**kwargs)

    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """
        LOGGER.info("Audio embedding start...")
        input_file_path = self.get_input_file_path_from_config(**kwargs)

        filename = os.path.basename(input_file_path)
        filename_without_ext = filename.split('.')[0]

        npz_file_name = f'{filename_without_ext}.npz'

        self.ensure_path(self.local_txt_path)
        self.ensure_path(self.local_audio_path)
        self.download_files(input_file_path, self.local_txt_path, self.local_audio_path)

        self.ensure_path(self.embed_file_path)

        self.create_embeddings("*.wav", npz_file_name, self.local_audio_path, self.embed_file_path)
        
        self.upload_to_azure(npz_file_name, input_file_path)

    def download_files(self, input_file_path, local_txt_path, local_audio_path):
        LOGGER.info("Total available cpu count:" + str(multiprocessing.cpu_count()))

        LOGGER.info(f"Downloading source from {input_file_path}")

        self.download_blob_to_location(input_file_path, f'{local_txt_path}{os.path.basename(input_file_path)}')

        text_file = open(f'{local_txt_path}{os.path.basename(input_file_path)}', "r")
        paths = text_file.readlines()
        text_file.close()

        worker_pool = ThreadPoolExecutor(max_workers=int(multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE))

        for file_path in paths:
            file_name = os.path.basename(file_path.rstrip("\n"))
            worker_pool.submit(self.download_blob_to_location, file_path.rstrip('\n'), f'{local_audio_path}{file_name}')
        worker_pool.shutdown(wait=True)

    def download_blob_to_location(self, blob_path, download_path):
        container_name, blob_name = self._parse_blob_url(blob_path)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    def create_embeddings(self, dir_pattern, local_npz_file, local_audio_path, embed_file_path):
        encode_each_batch(local_audio_path, dir_pattern, f'{embed_file_path}{local_npz_file}')

    def upload_to_azure(self, local_npz_file, input_file_path):
        npz_bucket_destination_path = f'{os.path.dirname(input_file_path)}/{local_npz_file}'
        self.upload_blob_from_location(f'{self.embed_file_path}{local_npz_file}', npz_bucket_destination_path)

    def upload_blob_from_location(self, upload_path, blob_path):
        container_name, blob_name = self._parse_blob_url(blob_path)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(upload_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        LOGGER.info(f"npz file uploaded to : {blob_path}")

    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_input_file_path_from_config(self, **kwargs):
        input_file_path = kwargs.get("file_path")
        if input_file_path is None:
            raise Exception("filter by source is mandatory")
        return input_file_path

    def _parse_blob_url(self, blob_url):
        parts = blob_url.split("/")
        container_name = parts[-2]
        blob_name = parts[-1]
        return container_name, blob_name

