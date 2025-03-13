import logging
from uuid import uuid4
from emf.common.logging import custom_logger

# Initialize custom logger
logger = logging.getLogger(__name__)
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid4())})

import config
from emf.model_retriever.model_retriever import HandlerModelsToMinio, HandlerModelsToValidator
from emf.common.integrations.elastic import HandlerSendToElastic
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.converters import opdm_metadata_to_json

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(
    queue=INPUT_RMQ_QUEUE,
    message_converter=opdm_metadata_to_json,
    message_handlers=[HandlerSendToElastic(index=METADATA_ELK_INDEX,
                                           id_from_metadata=True,
                                           id_metadata_list=ELK_ID_FROM_METADATA_FIELDS.split(',')),
                      HandlerModelsToMinio(),
                      HandlerModelsToValidator()],
)
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()


# TODO BACKLOG
# Get network model metadata object from RabbitMQ queue
# rabbit_service = rabbit.BlockingClient(message_converter=opdm_metadata_to_json)
# method_frame, properties, body = rabbit_service.get_single_message(queue=RMQ_QUEUE)