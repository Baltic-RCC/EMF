import logging
import sys
from uuid import uuid4
from emf.common.logging import custom_logger

# Initialize custom logger before importing config/business-logic modules, so their
# own startup logs are also tagged with worker/worker_uuid
logger = logging.getLogger(__name__)
worker_uuid = str(uuid4())
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-quality', 'worker_uuid': worker_uuid})

import config
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.model_quality.model_quality import HandlerModelQuality

parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)

logger.info(f"Starting 'model-quality' worker with assigned trace uuid: {worker_uuid}")

# RabbitMQ consumer implementation
if CONSUMER_TYPE == "SINGLE_MESSAGE":
    # RabbitMQ single message consumer implementation aligned with KEDA usage
    consumer = rabbit.SingleMessageConsumer(
        queue=INPUT_RMQ_QUEUE,
        message_handlers=[HandlerModelQuality()],
    )
    sys.exit(consumer.run())
elif CONSUMER_TYPE == "LONG_LIVING":
    # RabbitMQ long-living consumer implementation
    consumer = rabbit.RMQConsumer(queue=INPUT_RMQ_QUEUE,
                                  message_handlers=[HandlerModelQuality()],
                                  )
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()
else:
    raise Exception("Unknown CONSUMER_TYPE, please check the config/model_quality/model_quality.properties file")
