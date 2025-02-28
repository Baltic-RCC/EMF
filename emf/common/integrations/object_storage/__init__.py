import config
from emf.common.integrations.elastic import Elastic
from emf.common.integrations.minio_api import ObjectStorage
from emf.common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(), path=config.paths.integrations.object_storage)
elastic_service = Elastic()
minio_service = ObjectStorage()
