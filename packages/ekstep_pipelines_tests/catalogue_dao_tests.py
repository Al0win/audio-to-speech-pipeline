import json
import unittest
from unittest import mock

from ekstep_data_pipelines.common.dao.catalogue_dao import CatalogueDao

class CatalogueTests(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_get_utterances(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_query.return_value = [
            (
                '[{"name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav", '
                '"duration": "13.38", "snr_value": 38.432806, "status": "Clean"}]',
            )
        ]
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        audio_id = "2020"
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = [
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            }
        ]
        self.assertEqual(expected_utterances, actual_utterances)

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_get_utterances_as_empty_if_no_records(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_query.return_value = []
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        audio_id = "2020"
        actual_utterances = catalogueDao.get_utterances(audio_id)
        expected_utterances = []
        self.assertEqual(expected_utterances, actual_utterances)

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_update_utterances(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_update.return_value = None
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        audio_id = "2020"
        utterances = [
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            }
        ]

        rows_updated = catalogueDao.update_utterances(audio_id, utterances)
        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_params = {
            "utterances": json.dumps(utterances),
            "audio_id": audio_id,
        }
        called_with_sql = (
            "UPDATE media_metadata_staging SET utterances_files_list = @utterances WHERE audio_id = @audio_id"
        )
        self.assertEqual(rows_updated, True)
        self.assertEqual(called_with_sql, args[0][0][0])
        self.assertEqual(called_with_params, args[0][1])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_utterance_by_name(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        name = "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav"
        utterances = [
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            },
            {
                "name": "91_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "3.27",
                "snr_value": 37.0,
                "status": "Clean",
            },
        ]
        utterance = catalogueDao.find_utterance_by_name(utterances, name)
        self.assertEqual(
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            },
            utterance,
        )

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_utterance_by_name_return_None_if_not_found(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        name = "not_exists.wav"
        utterances = [
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            },
            {
                "name": "91_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "3.27",
                "snr_value": 37.0,
                "status": "Clean",
            },
        ]

        utterance = catalogueDao.find_utterance_by_name(utterances, name)
        self.assertEqual(None, utterance)

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_update_utterance_status(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        audio_id = "2020"
        utterance = {
            "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
            "duration": "13.38",
            "is_transcribed": True,
            "stt_api": 'google',
            "snr_value": 38.432806,
            "status": "Clean",
            "reason": "stt error",
        }

        rows_updated = catalogueDao.update_utterance_status(audio_id, utterance)

        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_query = (
            "UPDATE media_speaker_mapping SET status = @status, fail_reason = @reason, "
            "is_transcribed = CASE WHEN is_transcribed = 1 THEN 1 ELSE @is_transcribed END, "
            "stt_api_used = (SELECT DISTINCT e FROM STRING_SPLIT(stt_api_used + ',' + @stt_api_used, ',') e) "
            "WHERE audio_id = @audio_id AND clipped_utterance_file_name = @name"
        )

        called_with_args = {'status': 'Clean', 'is_transcribed': 1, 'stt_api_used': 'google', 'reason': 'stt error',
                            'audio_id': '2020', 'name': '190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav'}
        self.assertEqual(True, rows_updated)
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_get_utterances_by_source(self, mock_azure_sql_client):
        source = "test_source"
        status = "Clean"
        language = "hindi"
        data_set = "test"
        # speaker_id, clipped_utterance_file_name, clipped_utterance_duration, audio_id, snr
        expected_utterances = [
            (1, "file_1.wav", "10", "2010123", 16),
            (2, "file_2.wav", "11", "2010124", 17),
            (3, "file_3.wav", "12", "2010125", 18),
            (4, "file_4.wav", "13", "2010126", 19),
        ]
        called_with_sql = (
            "SELECT speaker_id, clipped_utterance_file_name, clipped_utterance_duration, "
            "audio_id, snr "
            "FROM media_speaker_mapping "
            "WHERE audio_id IN (SELECT audio_id FROM media_metadata_staging "
            "WHERE [source] = @source AND [language] = @language AND (data_set_used_for IS NULL OR data_set_used_for = @data_set)) "
            "AND status = @status "
            "AND staged_for_transcription = 0 "
            "AND clipped_utterance_duration BETWEEN 0.5 AND 15"
        )
        mock_azure_sql_client.execute_query.return_value = expected_utterances
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        args = mock_azure_sql_client.execute_query.call_args_list
        utterances = catalogueDao.get_utterances_by_source(source, status, language, data_set)
        self.assertEqual(utterances, expected_utterances)
        self.assertEqual(called_with_sql, args[0][0][0])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_update_utterances_staged_for_transcription(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_update.return_value = None
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        utterances = [
            {
                "name": "190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav",
                "duration": "13.38",
                "snr_value": 38.432806,
                "status": "Clean",
            }
        ]

        catalogueDao.update_utterances_staged_for_transcription(utterances)

        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_query = (
            "UPDATE media_speaker_mapping SET staged_for_transcription = 1, status = @status "
            "WHERE clipped_utterance_file_name = @name"
        )

        called_with_args = {'status': 'Clean', 'name': '190_Bani_Rahengi_Kitaabe_dr__sunita_rani_ghosh.wav'}
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_update_utterances_staged_for_transcription_empty_list(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        catalogueDao.update_utterances_staged_for_transcription([])
        self.assertFalse(mock_azure_sql_client.execute_update.called)

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_return_a_unique_id(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_query.return_value = [("unique_id",)]
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        unique_id = catalogueDao.get_unique_id()
        self.assertEqual(unique_id, "unique_id")

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_check_in_db_file_present_or_not(self, mock_azure_sql_client):
        mock_azure_sql_client.execute_query.return_value = [(1,)]
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        file_exists = catalogueDao.check_file_exist_in_db("filename.wav")
        self.assertTrue(file_exists)

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_update_utterance_speaker(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        utterance_id = 1
        speaker_id = 2
        catalogueDao.update_utterance_speaker(utterance_id, speaker_id)
        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_query = "UPDATE media_speaker_mapping SET speaker_id = @speaker_id WHERE utterance_id = @utterance_id"
        called_with_args = {'speaker_id': 2, 'utterance_id': 1}
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_insert_speakers(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        speakers = [
            {"name": "speaker1", "language": "hindi"},
            {"name": "speaker2", "language": "english"}
        ]
        catalogueDao.insert_speaker(speakers)
        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_query = (
            "INSERT INTO speaker (name, language) VALUES "
            "(@name1, @language1), "
            "(@name2, @language2)"
        )
        called_with_args = {'name1': 'speaker1', 'language1': 'hindi', 'name2': 'speaker2', 'language2': 'english'}
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_get_utterance_details_by_source(self, mock_azure_sql_client):
        source = "test_source"
        is_transcribed = True
        expected_utterances = [
            (
                1, "file_1.wav", "10", "2010123", "speaker_1", "Clean"
            )
        ]
        mock_azure_sql_client.execute_query.return_value = expected_utterances
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        args = mock_azure_sql_client.execute_query.call_args_list
        called_with_query = (
            "SELECT speaker_id, clipped_utterance_file_name, clipped_utterance_duration, "
            "audio_id, snr, speaker_id, status "
            "FROM media_speaker_mapping "
            "WHERE audio_id IN (SELECT audio_id FROM media_metadata_staging "
            "WHERE [source] = @source) "
            "AND status = 'Clean' "
            "AND is_transcribed = @is_transcribed "
            "AND clipped_utterance_duration BETWEEN 0.5 AND 15"
        )
        utterances = catalogueDao.get_utterance_details_by_source(source, is_transcribed)
        self.assertEqual(utterances, expected_utterances)
        self.assertEqual(called_with_query, args[0][0][0])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test_get_utterance_details_by_source_with_is_transcribed_false(self, mock_azure_sql_client):
        source = "test_source"
        is_transcribed = False
        expected_utterances = [
            (
                1, "file_1.wav", "10", "2010123", "speaker_1", "Clean"
            )
        ]
        mock_azure_sql_client.execute_query.return_value = expected_utterances
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        args = mock_azure_sql_client.execute_query.call_args_list
        called_with_query = (
            "SELECT speaker_id, clipped_utterance_file_name, clipped_utterance_duration, "
            "audio_id, snr, speaker_id, status "
            "FROM media_speaker_mapping "
            "WHERE audio_id IN (SELECT audio_id FROM media_metadata_staging "
            "WHERE [source] = @source) "
            "AND status = 'Clean' "
            "AND is_transcribed = 0 "
            "AND clipped_utterance_duration BETWEEN 0.5 AND 15"
        )
        utterances = catalogueDao.get_utterance_details_by_source(source, is_transcribed)
        self.assertEqual(utterances, expected_utterances)
        self.assertEqual(called_with_query, args[0][0][0])

    @mock.patch("ekstep_data_pipelines.common.azure_sql_db_client.AzureSQLClient")
    def test__should_update_utterance_artifact_name(self, mock_azure_sql_client):
        catalogueDao = CatalogueDao(mock_azure_sql_client)
        utterance_name = "file_1.wav"
        artifact_name = "artifact_1"
        catalogueDao.update_utterance_artifact(utterance_name, artifact_name)
        args = mock_azure_sql_client.execute_update.call_args_list
        called_with_query = (
            "UPDATE media_speaker_mapping SET artifact_name = @artifact_name "
            "WHERE clipped_utterance_file_name = @utterance_name"
        )
        called_with_args = {'artifact_name': 'artifact_1', 'utterance_name': 'file_1.wav'}
        self.assertEqual(called_with_query, args[0][0][0])
        self.assertEqual(called_with_args, args[0][1])

if __name__ == "__main__":
    unittest.main()
