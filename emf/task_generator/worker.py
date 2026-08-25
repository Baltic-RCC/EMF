import json
import config
import logging
from emf.task_generator.task_generator import generate_tasks
from emf.common.helpers.utils import filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger

logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.task_generator)

timeframe_conf = config.paths.task_generator.timeframe_conf
process_conf = config.paths.task_generator.process_conf

# Load to json
process_config_json = json.load(process_conf)
timeframe_config_json = json.load(timeframe_conf)

tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_config_json, timeframe_config_json))

if tasks:
    logger.info(f"Creating connection to RMQ")
    rabbit_service = rabbit.BlockingClient()
    logger.info(f"Sending tasks to Rabbit exchange: {RMQ_EXCHANGE}")
    for task in tasks:
        elk_handler.start_trace(task)
        rabbit_service.publish(payload=json.dumps(task),
                               exchange_name=RMQ_EXCHANGE,
                               headers=filter_and_flatten_dict(task, TASK_HEADER_KEYS.split(",")))
        elk_handler.stop_trace()
else:
    logger.info("No tasks generated at current timeframe, exiting worker.")

