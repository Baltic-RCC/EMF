import json
import config
import logging
from emf.task_generator.task_generator import generate_tasks, filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger

logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.task_generator)

timeframe_conf = config.paths.task_generator.timeframe_conf
process_conf = config.paths.task_generator.process_conf

process_config_json = json.load(process_conf)

for runs in process_config_json[0]['runs']:
    runs['properties']['included'] = CGM_INCLUDED_TSO.split(',') if CGM_INCLUDED_TSO else []
    runs['properties']['excluded'] = CGM_EXCLUDED_TSO.split(',') if CGM_EXCLUDED_TSO else []
    runs['properties']['replacement'] = RUN_REPLACEMENT
for runs in process_config_json[1]['runs']:
    runs['properties']['included'] = RMM_INCLUDED_TSO.split(',') if RMM_INCLUDED_TSO else []
    runs['properties']['excluded'] = RMM_EXCLUDED_TSO.split(',') if RMM_EXCLUDED_TSO else []
    runs['properties']['local_import'] = RMM_LOCAL_IMPORT.split(',') if RMM_LOCAL_IMPORT else []
    runs['properties']['replacement'] = RUN_REPLACEMENT

with open(process_conf, 'w') as file:
    json.dump(process_config_json, file, indent=1)

tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_conf, timeframe_conf, TIMETRAVEL))

if tasks:
    logger.info(f"Creating connection to RMQ")
    rabbit_service = rabbit.BlockingClient()
    logger.info(f"Sending tasks to Rabbit exchange '{RMQ_EXCHANGE}'")
    for task in tasks:
        elk_handler.start_trace(task)
        rabbit_service.publish(payload=json.dumps(task), exchange_name=RMQ_EXCHANGE, headers=filter_and_flatten_dict(task, TASK_HEADER_KEYS.split(",")))
        elk_handler.stop_trace()
else:
    logger.info("No tasks generated at current time.")



