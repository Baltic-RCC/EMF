import logging
import config
import uuid
from emf.common.integrations import rabbit
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from emf.common.converters import opdm_metadata_to_json
from emf.loadflow_tool.model_merge_handlers import HandlerGetModels, HandlerMergeModels, HandlerPostMergedModel

# Initialize custom logger
elk_handler = custom_logger.initialize_custom_logger(extra={'worker': 'model-merger', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.model_merge)

testing = True

# RabbitMQ's consumer for CGM

consumer = rabbit.RMQConsumer(
    que=RMQ_CGM_QUEUE,
    message_converter=opdm_metadata_to_json,
    message_handlers=[HandlerGetModels(logger_handler=elk_handler), HandlerMergeModels(), HandlerPostMergedModel()])

try:
    consumer.run()
except KeyboardInterrupt:
    consumer.stop()
