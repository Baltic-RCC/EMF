import json
import config
import logging
import pandas as pd
from emf.task_generator.task_generator import generate_tasks, filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger

logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.task_generator)

timeframe_conf = config.paths.task_generator.manual_timeframe_conf
process_conf = config.paths.task_generator.manual_process_conf

process_config_json = json.load(process_conf)
timeframe_config_json = json.load(timeframe_conf)

if MERGE_TYPE == 'BA':
    process_config_json[0]['runs'][0]['process_id'] = "https://example.com/processes/RMM_CREATION"
elif MERGE_TYPE == 'EU':
    process_config_json[0]['runs'][0]['process_id'] = "https://example.com/processes/CGM_CREATION"

if TIME_HORIZON == '1D':
    process_config_json[0]['runs'][0]['@id'] = "https://example.com/runs/DayAheadRMM"
    process_config_json[0]['runs'][0]['time_frame'] = 'D-1'
    timeframe_config_json[0]['@id'] = "https://example.com/timeHorizons/D-1"
elif TIME_HORIZON == '2D':
    process_config_json[0]['runs'][0]['@id'] = 'https://example.com/runs/TwoDaysAheadRMM'
    process_config_json[0]['runs'][0]['time_frame'] = 'D-2'
    timeframe_config_json[0]['@id'] = "https://example.com/timeHorizons/D-2"
elif TIME_HORIZON == 'ID':
    process_config_json[0]['runs'][0]['@id'] = "https://example.com/runs/IntraDayRMM/1"
    process_config_json[0]['runs'][0]['time_frame'] = 'D-1'
    timeframe_config_json[0]['@id'] = "https://example.com/timeHorizons/D-1"

process_config_json[0]['runs'][0]['properties']['merge_type'] = MERGE_TYPE
process_config_json[0]['runs'][0]['properties']['included'] = INCLUDED_TSO.split(',')
process_config_json[0]['runs'][0]['properties']['excluded'] = EXCLUDED_TSO.split(',')
process_config_json[0]['runs'][0]['properties']['local_import'] = LOCAL_IMPORT.split(',')
process_config_json[0]['runs'][0]['properties']['time_horizon'] = TIME_HORIZON
timeframe_config_json[0]['period_start'] = f'{PROCESS_TIME_SHIFT}'
timeframe_config_json[0]['period_duration'] = TASK_PERIOD_DURATION
timeframe_config_json[0]['reference_time'] = TASK_REFERENCE_TIME

with open(process_conf, 'w') as file:
    json.dump(process_config_json, file, indent=1)

with open(timeframe_conf, 'w') as file:
    json.dump(timeframe_config_json, file, indent=4)

tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_conf, timeframe_conf, TIMETRAVEL))
task_df = pd.DataFrame(tasks)
if tasks:
    a = pd.json_normalize(task_df['task_properties'])
else:
    a = None

if tasks:
    logger.info(f"Creating connection to RMQ")
    rabbit_service = rabbit.BlockingClient()
    logger.info(f"Sending tasks to Rabbit exchange '{RMQ_EXCHANGE}'")
    for task in tasks:
        rabbit_service.publish(payload=json.dumps(task), exchange_name=RMQ_EXCHANGE, headers=filter_and_flatten_dict(task, TASK_HEADER_KEYS.split(",")))
else:
    logger.info("No tasks generated at current time.")
