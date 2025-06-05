import config
import logging
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.model_quality.model_quality import HandlerModelQuality

# Initialize custom logger
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(queue=INPUT_RMQ_QUEUE,
                              message_handlers=[HandlerModelQuality()],
                              )

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
