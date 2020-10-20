import json

from common.utils import get_logger

from sqlalchemy import text

from common.dao.constants import GET_UNIQUE_ID,IS_EXIST

LOGGER = get_logger('CatalogueDao')

class CatalogueDao:

    def __init__(self, postgres_client):
        self.postgres_client = postgres_client

    def get_utterances(self, audio_id):
        parm_dict = {'audio_id': audio_id}
        utterances = self.postgres_client \
            .execute_query('select utterances_files_list from media_metadata_staging where audio_id = :audio_id'
                           , **parm_dict)
        return json.loads(utterances[0][0]) if len(utterances) > 0 else []

    def get_utterances_by_source(self, source, status):
        parm_dict = {'source': source, 'status': status}
        data = self.postgres_client \
            .execute_query('select speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr '
                           'from media_speaker_mapping '
                           'where audio_id '
                           'in (select audio_id from media_metadata_staging where "source" = :source) '
                           'and status = :status '
                           'and staged_for_transcription = false '
                           'and clipped_utterance_duration >= 0.5 and clipped_utterance_duration <= 15'
                           , **parm_dict)
        return data

    def update_utterances(self, audio_id, utterances):
        update_query = 'update media_metadata_staging ' \
                       'set utterances_files_list = :utterances where audio_id = :audio_id'
        utterances_json_str = json.dumps(utterances)
        LOGGER.info('utterances_json_str:' + utterances_json_str)
        LOGGER.info('utterances:' + str(utterances))
        parm_dict = {'utterances': utterances_json_str, 'audio_id': audio_id}
        self.postgres_client.execute_update(update_query, **parm_dict)
        return True

    def find_utterance_by_name(self, utterances, name):
        filtered_utterances = list(filter(lambda d: d['name'] == name, utterances))
        if len(filtered_utterances) > 0:
            return filtered_utterances[0]
        else:
            return None

    def update_utterance_status(self, audio_id, utterance):
        update_query = 'update media_speaker_mapping set status = :status, ' \
                       'fail_reason = :reason where audio_id = :audio_id ' \
                       'and clipped_utterance_file_name = :name'
        name = utterance['name']
        reason = utterance['reason']
        status = utterance['status']
        param_dict = {'status': status, 'reason': reason, 'audio_id': audio_id, 'name': name}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def update_utterances_staged_for_transcription(self, utterances, source):
        if len(utterances) <= 0:
            return True

        update_query = 'update media_speaker_mapping set staged_for_transcription = true ' \
                       'where audio_id in (select audio_id from media_metadata_staging ' \
                       'where "source" = :source) and clipped_utterance_file_name in '
        utterance_names = list(map(lambda u: f'\'{u[1]}\'', utterances))
        update_query = update_query + '(' + ','.join(utterance_names) + ')'
        param_dict = {'source': source}
        self.postgres_client.execute_update(update_query, **param_dict)
        return True

    def get_unique_id(self):
        return self.postgres_client.execute_query(GET_UNIQUE_ID)[0][0]

    def check_file_exist_in_db(self,file_name,hash_code):

        param_dict = {'file_name': file_name,'hash_code':hash_code}
        return self.postgres_client.execute_query(IS_EXIST,**param_dict)[0][0]

    def upload_file(self, meta_data_path):
        """
        Uploading the meta data file from local to
        """
        db = self.postgres_client.db

        with open(meta_data_path, 'r') as f:
            conn = db.raw_connection()
            cursor = conn.cursor()
            cmd = 'COPY media_metadata_staging(raw_file_name,duration,title,speaker_name,audio_id,cleaned_duration,num_of_speakers,language,has_other_audio_signature,type,source,experiment_use,utterances_files_list,source_url,speaker_gender,source_website,experiment_name,mother_tongue,age_group,recorded_state,recorded_district,recorded_place,recorded_date,purpose,media_hash_code) FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()

    def upload_file_to_downloaded_source(self, file_path):

        db_conn = self.postgres_client.db

        LOGGER.info("uploading data to source_metadata")
        with open(file_path, 'r') as f:
            conn = db_conn.raw_connection()
            cursor = conn.cursor()
            cmd = 'COPY source_metadata_downloaded(source,num_speaker,total_duration,num_of_audio) FROM STDIN WITH (FORMAT CSV, HEADER)'
            cursor.copy_expert(cmd, f)
            conn.commit()