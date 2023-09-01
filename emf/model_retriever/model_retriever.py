import logging
import config
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic
from emf.common.converters import opdm_metadata_to_json

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# EDX > opdm objects
# OPDM.download_object(opdm object) > opdm objects (enhanced with data
# a) NEW MINIO.upload_opdm(opdm object) > opdm objects
# b) MINIO.upload_opdm(bytes) > url
# b) update opdm object with url
# loadflow_tool.validate(opdm_object + bds opdmobject) -> validated model
# update opdm object with validation status
# opdm object remove binary data
# elk handler.batch_upload(opdm_object)


def transfer_model_meta_from_opde_to_elk():
    message_types = EDX_MESSAGE_TYPE.split(",")
    elk_handler = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])
    service = edx.EDX(converter=opdm_metadata_to_json, handler=elk_handler, message_types=message_types)
    service.run()

def store_models_from_opde_to_minio(opdm_objects: list):
    pass

    # OPDM_API to request model
    # downloadm model in bytes
    # stores it in minio

    return opdm_object


def validate_igms():
    pass


if __name__ == "__main__":
    # Testing
    import sys
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Get models metadata from OPDE
    transfer_model_meta_from_opde_to_elk()
