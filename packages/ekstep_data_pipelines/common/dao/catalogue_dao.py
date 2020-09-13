import json

import numpy as np
from psycopg2.extensions import register_adapter, AsIs


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)


def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


class CatalogueDao:

    def __init__(self, postgres_client):
        self.postgres_client = postgres_client

    def get_utterances(self, audio_id):
        parm_dict = {'audio_id': audio_id}
        utterances = self.postgres_client \
            .execute_query('select utterances_files_list from media_metadata_staging where audio_id = :audio_id'
                           , **parm_dict)
        print('utterances:' + str(utterances[0][0]))
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
        print('utterances_json_str:' + utterances_json_str)
        print('utterances:' + str(utterances))
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

    def update_utterances_staged_for_transcription(self, utterances):
        update_query = 'update media_speaker_mapping set staged_for_transcription = true ' \
                       'where audio_id = :audio_id ' \
                       'and clipped_utterance_file_name = :name'
        data_list = []
        for utterance in utterances:
            audio_id = utterance[3]
            name = utterance[1]
            param_dict = {'audio_id': audio_id, 'name': name}
            data_list.append(param_dict)
        rows_updated = self.postgres_client.execute_batch(update_query, data_list)
        return rows_updated
