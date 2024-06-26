from emf.loadflow_tool.model_merger.handlers.rmm_handler import HandlerRmmToPdnAndMinio
from emf.common.integrations import rabbit
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from uuid import uuid4
import config
import logging

# Initialize custom logger
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'rmm-merger', 'worker_uuid': str(uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.merger)

# RabbitMQ consumer implementation
consumer = rabbit.RMQConsumer(que=INPUT_RABBIT_QUE, message_handlers=[HandlerRmmToPdnAndMinio()])

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()