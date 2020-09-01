import sys

from ekstep_data_pipelines.audio_processing.audio_duration import calculate_duration

sys.path.insert(0, '..')
sys.path.insert(0, '../..')

import os
import json
import shutil
import subprocess
import pandas as pd
import librosa
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger('Snr')


class SNR:

    """
    Util object for performing SNR analysis over different
    """

    MAX_DURATION = 15

    @staticmethod
    def get_instance():
        curr_instance = SNR()
        return curr_instance

    def __init__(self):
        self.current_working_dir = os.getcwd()

    def get_command(self, current_working_dir, file_path):
        return f'"{current_working_dir}/binaries/WadaSNR/Exe/WADASNR" -i "{file_path}" -t "{current_working_dir}/binaries/WadaSNR/Exe/Alpha0.400000.txt" -ifmt mswav'

    def get_output_directories(self, output_dir, ensure_path=True):
        clean_path, rejected_path = f'{output_dir}/clean', f'{output_dir}/rejected'

        if ensure_path:
            LOGGER.info(f'ensure_path flag is {ensure_path}, ensuring that the directories exist')
            if not os.path.exists(clean_path):
                LOGGER.info(f'{clean_path} does not exist, creating it')
                os.makedirs(clean_path)

            if not os.path.exists(rejected_path):
                LOGGER.info(f'{rejected_path} does not exist, creating it')
                os.makedirs(rejected_path)

        return clean_path, rejected_path

    def move_file_locally(self, source, destination):
        shutil.move(source, destination)

    def compute_file_snr(self, file_path):
        """
        Convert given file to required format with FFMPEG and process with WADA.
        """
        LOGGER.info(f'Measuring SNR for file at {file_path}')
        command = self.get_command(self.current_working_dir, file_path)

        LOGGER.info(f'Command to be run {command}')

        try:
            process_output = subprocess.check_output(command, shell=True)
        except subprocess.CalledProcessError as e:
            LOGGER.error('Called process error:' + str(e))
            return float(-1)

        return float(process_output.split()[-3].decode("utf-8"))

    def process_files_list(self, input_file_list):

        LOGGER.info(f'Processing all the file in the directory {input_file_list}')

        file_snrs = {}

        for file_path in input_file_list:


            snr_value = self.compute_file_snr(file_path)
            file_snrs[file_path] = snr_value

            LOGGER.info(f'{file_path} has an snr value of {snr_value}')

        return file_snrs

    def fit_and_move(self, input_file_list, metadata_file_name, threshold, output_dir_path, audio_id):

        LOGGER.info(f'Processing SNR for for the files {input_file_list}')
        processed_file_snr_dict = self.process_files_list(input_file_list)

        LOGGER.info(f'Getting the clean and reject folders')
        clean_dir_path, rejected_dir_path = self.get_output_directories(output_dir_path)
        LOGGER.info(f'Got the clean and reject folders, clean/{clean_dir_path} and rejected/{rejected_dir_path}')

        metadata = pd.read_csv(metadata_file_name)

        clean_audio_duration=[]
        list_file_utterances_with_duration=[]

        for file_path, snr_value in processed_file_snr_dict.items():

            audio_file_name = file_path.split('/')[-1]
            LOGGER.info(audio_file_name)

            metadata["cleaned_duration"] = round((sum(clean_audio_duration) / 60), 2)
            metadata["audio_id"] = audio_id
            metadata['utterances_files_list'] = str(list_file_utterances_with_duration)

            if snr_value < threshold:
                self.move_file_locally(file_path,  f'{rejected_dir_path}/{audio_file_name}')
                metadata.to_csv(metadata_file_name, index=False)
                list_file_utterances_with_duration.append(
                    {'name': audio_file_name, 'duration': str(clip_duration), 'snr_value': snr_value, 'status': 'Rejected', 'reason': 'High-SNR', 'snr_threshold': threshold})
                continue

            clip_duration = calculate_duration(file_path)

            if(clip_duration > SNR.MAX_DURATION):
                self.move_file_locally(file_path,  f'{rejected_dir_path}/{audio_file_name}')
                metadata.to_csv(metadata_file_name, index=False)
                list_file_utterances_with_duration.append(
                    {'name': audio_file_name, 'duration': str(clip_duration), 'snr_value': snr_value, 'status': 'Rejected', 'reason': 'High Audio Duration', 'max_duration': SNR.MAX_DURATION})
                continue

            clean_audio_duration.append(clip_duration)
            self.move_file_locally(file_path, f'{clean_dir_path}/{audio_file_name}')
            list_file_utterances_with_duration.append({'name': audio_file_name, 'duration': str(clip_duration), 'snr_value': snr_value, 'status': 'Clean'})


            metadata["cleaned_duration"] = round((sum(clean_audio_duration) / 60),2)
            metadata['utterances_files_list'] = json.dumps(list_file_utterances_with_duration)
            metadata.to_csv(metadata_file_name, index=False)
