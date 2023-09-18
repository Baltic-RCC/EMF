import logging
import sys
import time

import config
import json
from emf.model_retriever import model_retriever
from emf.common.integrations import elastic, opdm, minio, edx
from emf.common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

edx_service = edx.EDX()
opdm_service = opdm.OPDM()
minio_service = minio.ObjectStorage()
elk_service = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])

while True:
    # Get model from EDX
    body, properties = model_retriever.get_opdm_object_from_edx(message_type=EDX_MESSAGE_TYPE, edx_service=edx_service)

    if not body:
        time.sleep(10)
        continue

    # Download model from OPDE and store to MINIO
    opdm_objects = json.loads(body)
    opdm_objects = model_retriever.opde_models_to_minio(opdm_objects=opdm_objects, opdm_service=opdm_service, minio_service=minio_service)

    # TODO Validate model


    for opdm_object in opdm_objects:
        # Removing content from opdm_object to only keep the model metadata
        for component in opdm_object['opde:Component']:
            component['opdm:Profile'].pop('DATA')

    # Send model metadata to ELK
    elk_service.send(byte_string=json.dumps(opdm_objects).encode('utf-8'), properties=properties)
    logger.info(f"Model metadata sent to ELK")
