import json
import sys
import logging

import config
from emf.task_generator.task_generator import generate_tasks
from emf.task_generator.manual_config import build_manual_run_config
from emf.common.helpers.utils import filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger

logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.task_generator)

timeframe_conf = config.paths.task_generator.timeframe_conf
process_conf = config.paths.task_generator.process_conf

process_config_json = json.load(process_conf)
timeframe_config_json = json.load(timeframe_conf)

mode = TASK_GENERATION_MODE.strip().lower()

if mode == "manual":

    if "RMM" in RUN_TYPE and not INCLUDED_TSO:
        logger.error(f"RMM included TSOs can not be empty for the run type: {RUN_TYPE}")
        sys.exit("Issue with input, check the EMFOS logs for possible error")

    try:
        process_config_json, timeframe_config_json, TIMESTAMP = build_manual_run_config(
            process_config_json, timeframe_config_json, globals()
        )
    except ValueError as error:
        logger.error(str(error))
        sys.exit("Issue with input, check the EMFOS logs for possible error")

    tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_config_json, timeframe_config_json,
                                TIMESTAMP, PROCESS_TIME_SHIFT, task_type="manual", task_initiator=TASK_INITIATOR))

elif mode == "auto":
    tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_config_json, timeframe_config_json,
                                task_type="automatic", task_initiator=TASK_INITIATOR))

else:
    raise ValueError(f"Unknown TASK_GENERATION_MODE '{TASK_GENERATION_MODE}', expected 'auto' or 'manual'")

# Publish tasks
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
