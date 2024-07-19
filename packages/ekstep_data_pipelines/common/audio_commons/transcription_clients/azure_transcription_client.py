from azure.cognitiveservices.speech import SpeechConfig, SpeechRecognizer, AudioConfig, ResultReason
from ekstep_data_pipelines.common.utils import get_logger
from ekstep_data_pipelines.common.audio_commons.transcription_clients.transcription_client_errors import (
    AzureTranscriptionClientError,
)

LOGGER = get_logger("AzureTranscriptionClient")

class AzureTranscriptionClient(object):
    @staticmethod
    def get_instance(config_dict):
        azure_config_dict = config_dict.get("common", {}).get(
            "azure_transcription_client", {}
        )
        return AzureTranscriptionClient(**azure_config_dict)

    def __init__(self, **kwargs):
        self.speech_key = kwargs.get("speech_key")
        self.service_region = kwargs.get("service_region")
        self.language = kwargs.get("language", "hi-IN")
        self.speech_config = SpeechConfig(subscription=self.speech_key, region=self.service_region)
        self.speech_config.speech_recognition_language = self.language

    def generate_transcription(self, language, source_file_path):
        try:
            result = self.speech_to_text(source_file_path)
            return result.text if result else ""
        except RuntimeError as error:
            raise AzureTranscriptionClientError(error)

    def speech_to_text(self, audio_file_path):
        audio_input = AudioConfig(filename=audio_file_path)

        LOGGER.info("Calling Azure STT API for file: %s", audio_file_path)
        speech_recognizer = SpeechRecognizer(speech_config=self.speech_config, audio_config=audio_input)
        LOGGER.info("Recognizing first result...")

        result = speech_recognizer.recognize_once()

        if result.reason == ResultReason.RecognizedSpeech:
            LOGGER.info("Recognized: %s", result.text)
            return result
        elif result.reason == ResultReason.NoMatch:
            msg = "No speech could be recognized: {}".format(result.no_match_details)
            raise RuntimeError(msg)
        elif result.reason == ResultReason.Canceled:
            cancellation_details = result.cancellation_details
            msg = "Speech Recognition canceled: {}".format(cancellation_details.reason)
            raise RuntimeError(msg)
        else:
            msg = "Unknown recognition error occurred"
            raise RuntimeError(msg)
        LOGGER.info("done..")
