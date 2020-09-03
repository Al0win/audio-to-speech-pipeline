from audio_transcription.constants import CONFIG_NAME, CLEAN_AUDIO_PATH, LANGUAGE
from audio_transcription.transcription_sanitizer import TranscriptionSanitizer
from audio_transcription.audio_transcription_errors import TranscriptionSanitizationError
from common.audio_commons.transcription_clients.transcription_client_errors import \
    AzureTranscriptionClientError, GoogleTranscriptionClientError

import os

class AudioTranscription:
    LOCAL_PATH = None

    @staticmethod
    def get_instance(data_processor, gcs_instance, audio_commons):
        return AudioTranscription(data_processor, gcs_instance, audio_commons)

    def __init__(self, data_processor, gcs_instance, audio_commons):
        self.data_processor = data_processor
        self.gcs_instance = gcs_instance
        self.transcription_clients = audio_commons.get('transcription_clients')
        self.audio_transcription_config = None

    def process(self, **kwargs):

        self.audio_transcription_config = self.data_processor.config_dict.get(
            CONFIG_NAME)

        source = kwargs.get('audio_source')
        audio_ids = kwargs.get('audio_ids', [])
        stt_api = kwargs.get("speech_to_text_client")

        language = self.audio_transcription_config.get(LANGUAGE)
        remote_path_of_dir = self.audio_transcription_config.get(
            CLEAN_AUDIO_PATH)

        for audio_id in audio_ids:

            try:

                remote_dir_path_for_given_audio_id = f'{remote_path_of_dir}/{source}/{audio_id}/clean/'
                remote_stt_output_path = self.audio_transcription_config.get(
                    'remote_stt_audio_file_path')
                remote_stt_output_path = f'{remote_stt_output_path}/{source}/{audio_id}'

                transcription_client = self.transcription_clients[stt_api]

                all_path = self.gcs_instance.list_blobs_in_a_path(remote_dir_path_for_given_audio_id)

                local_dir_path = self.generate_transcription_for_all_utterenaces(all_path, language, transcription_client)

                self.move_to_gcs(local_dir_path, remote_stt_output_path)

                self.delete_audio_id(f'{remote_path_of_dir}/{source}/')
            except Exception as e:
                # TODO: This should be a specific exception, will need
                #       to throw and handle this accordingly.
                continue

        return

    def delete_audio_id(self, remote_dir_path_for_given_audio_id):
        self.gcs_instance.delete_object(remote_dir_path_for_given_audio_id)

    def move_to_gcs(self, local_path, remote_stt_output_path):
        self.gcs_instance.upload_to_gcs(local_path, remote_stt_output_path)

    def save_transcription(self, transcription, output_file_path):
        with open(output_file_path, "w") as f:
            f.write(transcription)

    def generate_transcription_for_all_utterenaces(self, all_path, language, transcription_client):
        for file_path in all_path:
            local_clean_path = f"/tmp/{file_path.name}/clean"
            local_rejected_path = f"/tmp/{file_path.name}/rejected"

            self.generate_transcription_and_sanitize(local_clean_path, local_rejected_path,  file_path, language, transcription_client)

        return self.get_local_dir_path(local_clean_path)

    def generate_transcription_and_sanitize(self, local_clean_path, local_rejected_path, file_path, language, transcription_client):
        if ".wav" in file_path.name:

            transcription_file_name = local_clean_path.replace('.wav', '.txt')
            self.gcs_instance.download_to_local(
                file_path.name, local_clean_path, False)

            try:
                transcript = transcription_client.generate_transcription(
                    language, local_clean_path)
                original_transcript = transcript
                transcript = TranscriptionSanitizer().sanitize(transcript)

                if original_transcript != transcript:
                    self.save_transcription(original_transcript, 'original_' + transcription_file_name)
                self.save_transcription(transcript, transcription_file_name)
            except TranscriptionSanitizationError as tse:
                print('Transcription not valid: ' + str(tse))
                self.handle_error(local_clean_path, local_rejected_path)
            except (AzureTranscriptionClientError, GoogleTranscriptionClientError) as e:
                print('STT API call failed: ' + str(e))
                self.handle_error(local_clean_path, local_rejected_path)
            except RuntimeError as rte:
                print('Error: ' + str(rte))
                self.handle_error(local_clean_path, local_rejected_path)

    def handle_error(self, local_path, local_rejected_path):
        if not os.path.exists(local_rejected_path):
            os.makedirs(local_rejected_path)
        command = f'mv {local_path} {local_rejected_path}'
        print(f'moving bad wav file: {local_path} to rejected folder: {local_rejected_path}')
        os.system(command)

    def get_local_dir_path(self, local_file_path):
        path_array = local_file_path.split('/')
        path_array.pop()
        path_array.pop()
        return '/'.join(path_array)
