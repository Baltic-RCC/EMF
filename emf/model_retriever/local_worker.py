import logging
from uuid import uuid4
import json
from emf.common.logging import custom_logger

# Initialize custom logger
logger = logging.getLogger(__name__)
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'local-model-retriever', 'worker_uuid': str(uuid4())})

import config
from emf.model_retriever.model_retriever import HandlerModelsToMinio, HandlerModelsToValidator, HandlerModelsFromBytesIO
from emf.common.integrations.elastic import HandlerSendToElastic
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(
    queue=INPUT_RMQ_QUEUE,
    message_converter=None,
    message_handlers=[
        HandlerModelsFromBytesIO(),
        HandlerModelsToMinio(),
        HandlerSendToElastic(index=METADATA_ELK_INDEX,
                             id_from_metadata=True,
                             id_metadata_list=ELK_ID_FROM_METADATA_FIELDS.split(','),
                             hashing=json.loads(ELK_ID_HASHING.lower()),
                             ),
        HandlerModelsToValidator()
    ])
try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
