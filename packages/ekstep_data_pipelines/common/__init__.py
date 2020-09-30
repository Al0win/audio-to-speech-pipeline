import yaml
from common.postgres_db_client import PostgresClient
from common.gcs_operations import CloudStorageOperations
from .audio_commons import get_audio_commons
from .infra_commons import get_infra_utils
from .dao.catalogue_dao import CatalogueDao

class BaseProcessor:

    def __init__(self, *agrs, **kwargs):
        self.commons_dict = kwargs.get('commons_dict', {})

        file_system = kwargs.get('file_interface')
        self.fs_interface = self.commons_dict.get('infra_commons', {}).get('storage_clients', {}).get(file_system)


def get_periperhals(intialization_dict_path):
    data_processor = PostgresClient.get_instance(intialization_dict_path)
    gcs_instance = CloudStorageOperations.get_instance(intialization_dict_path)
    catalogue_dao = CatalogueDao(data_processor)
    peripheral_dict ={
        "data_processor": data_processor,
        "gsc_instance": gcs_instance,
        "catalogue_dao": catalogue_dao,
    }

    config_dict = load_config(intialization_dict_path.get('config_file_path'))

    peripheral_dict['audio_commons'] = get_audio_commons(config_dict)
    peripheral_dict['infra_commons'] = get_infra_utils(config_dict)

    return peripheral_dict


def load_config(config_file_path):
    with open(config_file_path, 'r') as file:
        parent_config_dict = yaml.load(file)
        return parent_config_dict.get('config')



