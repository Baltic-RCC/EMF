import logging
from uuid import uuid4
from emf.common.logging import custom_logger
from emf.loadflow_tool.model_validator.model_validator import HandlerModelsValidator

# Initialize custom logger
logger = logging.getLogger(__name__)
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-validator', 'worker_uuid': str(uuid4())})

import config
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties

# Disabling triplets library logging at INFO level
# logging.getLogger('triplets').setLevel(logging.WARNING)

parse_app_properties(caller_globals=globals(), path=config.paths.model_validator.model_validator)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(queue=INPUT_RMQ_QUEUE,
                              message_handlers=[HandlerModelsValidator()],
                              )

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
