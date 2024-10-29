import logging
from uuid import uuid4
from emf.common.logging import custom_logger

# Initialize custom logger
logger = logging.getLogger(__name__)
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid4())})

import config
from emf.model_retriever.model_retriever import HandlerModelsToMinio, HandlerModelsValidator, HandlerMetadataToElastic
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.converters import opdm_metadata_to_json

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(
    que=RMQ_QUEUE,
    message_converter=opdm_metadata_to_json,
    message_handlers=[HandlerModelsToMinio(), HandlerModelsValidator(), HandlerMetadataToElastic()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()

# # EDX SOAP API implementation
# edx_service = edx.EDX()
# opdm_service = opdm.OPDM()
# minio_service = minio.ObjectStorage()
# elk_service = elastic.Handler(index=ELK_INDEX_METADATA, id_from_metadata=True, id_metadata_list=['opde:Id'])
#
# while True:
#     # Get model from EDX
#     body, properties = model_retriever.get_opdm_object_from_edx(message_type=EDX_MESSAGE_TYPE, edx_service=edx_service)
#
#     if not body:
#         time.sleep(10)
#         continue
#
#     # Download model from OPDE and store to MINIO
#     opdm_objects = json.loads(body)
#     opdm_objects = model_retriever.opde_models_to_minio(opdm_objects=opdm_objects, opdm_service=opdm_service, minio_service=minio_service)
#
#     # Get latest boundary set for validation
#     latest_boundary = opdm_service.get_latest_boundary()
#
#     # Run network model validation
#     for opdm_object in opdm_objects:
#         response = validate_model(opdm_objects=[opdm_object, latest_boundary])
#
#         # Filter out non metadata
#         response.pop('NETWORK')
#         opdm_object["VALIDATION_STATUS"] = response
#         for component in opdm_object['opde:Component']:
#             component['opdm:Profile'].pop('DATA')
#
#     # Send model metadata to ELK
#     elk_service.send(byte_string=json.dumps(opdm_objects, default=str).encode('utf-8'), properties=properties)
#     logger.info(f"Network model metadata sent to object-storage.elk")


# TODO BACKLOG
# Get network model metadata object from RabbitMQ queue
# rabbit_service = rabbit.BlockingClient(message_converter=opdm_metadata_to_json)
# method_frame, properties, body = rabbit_service.get_single_message(queue=RMQ_QUEUE)