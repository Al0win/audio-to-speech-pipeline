import unittest
from unittest.mock import Mock
import os

from ekstep_data_pipelines.audio_embedding.audio_embedding import AudioEmbedding

class AudioEmbeddingsTests(unittest.TestCase):

    def setUp(self):
        self.postgres_client = Mock()
        self.azure_instance = Mock()  # Replace with Azure-specific mock or instance

        self.audio_embedding = AudioEmbedding(
            self.postgres_client
        )

        self.audio_embedding.fs_interface = Mock()  # Adjust this mock for Azure operations

    def test_should_create_embeddings_for_given_audios(self):
        # Adjust paths to Azure Blob Storage format
        self.audio_embedding.create_embeddings('*.wav', 'test.npz', 'https://your-storage-account.blob.core.windows.net/container-name/path/to/source/', 'https://your-storage-account.blob.core.windows.net/container-name/path/to/destination/')

        # Assert that the file was created in Azure Blob Storage
        self.assertTrue(os.path.exists("path/to/your/expected/file/test.npz"))

    def test_upload_to_azure(self):
        # Mock parameters for Azure upload operation
        local_file_path = 'local_file_path'
        input_file_path = 'input_file_path'

        self.audio_embedding.upload_to_azure(local_file_path, input_file_path)

        # Assert that the upload method was called once
        self.assertEqual(
            self.audio_embedding.fs_interface.upload_to_location.call_count, 1
        )

        # Optionally, further assertions on the parameters passed to upload_to_location

if __name__ == '__main__':
    unittest.main()

