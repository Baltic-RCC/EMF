import logging
from uuid import uuid4
from emf.common.logging import custom_logger

# Initialize custom logger
logger = logging.getLogger(__name__)
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-merger', 'worker_uuid': str(uuid4())})

import config
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.loadflow_tool.model_merger.model_merger import HandlerMergeModels

# Disabling triplets library logging at INFO level
logging.getLogger('triplets').setLevel(logging.WARNING)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.merger)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(que=INPUT_RABBIT_QUE, message_handlers=[HandlerMergeModels()])

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()