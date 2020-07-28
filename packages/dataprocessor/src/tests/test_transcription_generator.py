import os
import shutil
import unittest
from unittest import mock

from src.scripts.transcription_generator import save_transcriptions, create_transcriptions


class TestTrancriptionGenerator(unittest.TestCase):
    @mock.patch("src.scripts.google_speech_client.GoogleSpeechClient")
    def test_create_transcription(self, mock_client):
        output_file_dir = "./src/tests/test_resources/output/transcriptions"
        os.mkdir(output_file_dir)
        transcriptions = create_transcriptions(mock_client, "wav_file_path", output_file_dir, "api_response_file")
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEquals(transcriptions, [''])


    def test_save_transcriptions(self):
        transcriptions = ['', 'मेरे प्यारे देशवासियों', 'नमस्कार', '', 'कोरोना के प्रभाव से हमारी मन की बात भी अछूती नहीं रही है', 'जब मैंने पिछली बार आपसे']
        output_file_dir = './src/tests/test_resources/output/transcriptions'
        os.mkdir(output_file_dir)
        save_transcriptions(output_file_dir, transcriptions, 'chunk')
        files = os.listdir(output_file_dir)
        if os.path.exists(output_file_dir):
            shutil.rmtree(output_file_dir)
        self.assertEqual(len(files), 6)

if __name__ == '__main__':
    unittest.main()
