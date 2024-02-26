import logging
import time
import config
import json
import uuid
from emf.model_retriever import model_retriever
from emf.common.integrations import elastic, opdm, minio, edx, rabbit
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from emf.loadflow_tool.validator import validate_model
from emf.common.converters import opdm_metadata_to_json

import sys

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s",
                    level=logging.INFO)

# Initialize custom logger
# custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# edx_service = edx.EDX()
rabbit_service = rabbit.BlockingClient(message_converter=opdm_metadata_to_json)
opdm_service = opdm.OPDM()
minio_service = minio.ObjectStorage()

elk_service = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])

while True:
    # Get model from EDX
    # body, properties = model_retriever.get_opdm_object_from_edx(message_type=EDX_MESSAGE_TYPE, edx_service=edx_service)

    # Get network model metadata object from RabbitMQ queue
    method_frame, properties, body = rabbit_service.get_single_message(queue=RMQ_QUEUE)

    if not body:
        time.sleep(10)
        continue

    # Download model from OPDE and store to MINIO
    opdm_objects = json.loads(body)
    opdm_objects = model_retriever.opde_models_to_minio(opdm_objects=opdm_objects, opdm_service=opdm_service, minio_service=minio_service)

    # Get latest boundary set for validation
    latest_boundary = opdm_service.get_latest_boundary()

    # Run network model validation
    for opdm_object in opdm_objects:
        response = validate_model(opdm_objects=[opdm_object, latest_boundary])

        # Filter out non metadata
        response.pop('NETWORK')
        opdm_object["VALIDATION_STATUS"] = response
        for component in opdm_object['opde:Component']:
            component['opdm:Profile'].pop('DATA')

    # Send model metadata to ELK
    elk_service.send(byte_string=json.dumps(opdm_objects, default=str).encode('utf-8'), properties=properties)
    logger.info(f"Network model metadata sent to object-storage.elk")
