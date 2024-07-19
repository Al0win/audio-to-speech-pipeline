import unittest
from unittest.mock import Mock

from ekstep_data_pipelines.common.file_system.azure_file_system import AzureFileSystem


class DataMoverTests(unittest.TestCase):
    def setUp(self):
        self.azure_operations = Mock()
        self.azure_file_system = AzureFileSystem(self.azure_operations)

    def test__should_list_files(self):
        dir = (
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued"
            "/hindi/audio/swayamprabha_chapter/1/clean"
        )
        self.azure_operations.list_blobs_in_a_path.return_value = [
            Path("path1"),
            Path("path2"),
        ]
        files = self.azure_file_system.ls(dir)
        call_args = self.azure_operations.list_blobs_in_a_path.call_args
        self.assertEqual(call_args[0][0], dir)
        self.assertEqual(["path1", "path2"], files)

    def test__should_move_dir(self):
        self.azure_operations.list_blobs_in_a_path.return_value = [
            Path(
                "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued/hindi/"
                "audio/swayamprabha_chapter/1/clean/path1"
            ),
            Path(
                "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued/hindi/"
                "audio/swayamprabha_chapter/1/clean/path2"
            ),
        ]
        src_dir = (
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued/hindi/"
            "audio/swayamprabha_chapter/1/clean"
        )
        target_dir = (
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/landing/hindi/"
            "audio/swayamprabha_chapter/1/clean"
        )
        self.azure_file_system.mv(src_dir, target_dir, True)
        call_args = self.azure_operations.move_blob.call_args_list
        self.assertEqual(call_args[0][0][0], f"{src_dir}/path1")
        self.assertEqual(call_args[0][0][1], f"{target_dir}/path1")
        self.assertEqual(call_args[1][0][0], f"{src_dir}/path2")
        self.assertEqual(call_args[1][0][1], f"{target_dir}/path2")

    def test__should_move_file(self):
        target_dir = (
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/landing/"
            "hindi/audio/swayamprabha_chapter/1/clean"
        )
        self.azure_file_system.mv_file(
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued/hindi/"
            "audio/swayamprabha_chapter/1/clean/file1.wav",
            target_dir,
        )
        call_args = self.azure_operations.move_blob.call_args
        self.assertEqual(
            call_args[0][0],
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/catalogued/hindi/"
            "audio/swayamprabha_chapter/1/clean/file1.wav",
        )
        self.assertEqual(
            call_args[0][1],
            "https://ekstepspeechrecognitiondev.blob.core.windows.net/data/audiotospeech/raw/landing/hindi/"
            "audio/swayamprabha_chapter/1/clean/file1.wav",
        )


class Path:
    def __init__(self, path):
        self.name = path
