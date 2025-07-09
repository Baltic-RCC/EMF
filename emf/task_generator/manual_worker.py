import json
import os
import sys
import config
import logging
from emf.task_generator.task_generator import generate_tasks
from emf.common.helpers.utils import filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger


logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.manual_task_generator)

# Get default timeframe and process conf
timeframe_conf = config.paths.task_generator.timeframe_conf
process_conf = config.paths.task_generator.process_conf

# Load to json
process_config_json = json.load(process_conf)
timeframe_config_json = json.load(timeframe_conf)


# Based on RUN_TYPE get process timeframe config
break_top = False
for merge_index, merge_type in enumerate(process_config_json):
    for run in merge_type.get("runs", []):
        if RUN_TYPE in run["@id"]:
            process_config_json.pop(merge_index)
            process_config_json[0]['runs'] = [run]
            break_top = True
            break
    if break_top:
        break

for time_conf in timeframe_config_json:
    if process_config_json[0]['runs'][0]["time_frame"] in time_conf["@id"]:
        timeframe_config_json = [time_conf]
        break

if "RMM" in RUN_TYPE and not(INCLUDED_TSO):
    logger.error(f"RMM included TSOs can not be empty for the run type: {RUN_TYPE}")
    sys.exit("Issue with input, check the EMFOS logs for possible error")


# Update process configuration from ENV variables if defined
process_config_json[0]['runs'][0]['run_at'] = '* * * * *'
process_config_json[0]['runs'][0]['properties']['included'] = [tso.strip() for tso in INCLUDED_TSO.split(',')] if INCLUDED_TSO else []
process_config_json[0]['runs'][0]['properties']['excluded'] = [tso.strip() for tso in EXCLUDED_TSO.split(',')] if EXCLUDED_TSO else []
process_config_json[0]['runs'][0]['properties']['local_import'] = [tso.strip() for tso in LOCAL_IMPORT.split(',')] if LOCAL_IMPORT else []
process_config_json[0]['runs'][0]['properties']['fix_net_interchange2'] = FIX_NET_INTERCHANGE2
process_config_json[0]['runs'][0]['properties']['version'] = TASK_VERSION
process_config_json[0]['runs'][0]['properties']['replacement'] = RUN_REPLACEMENT
process_config_json[0]['runs'][0]['properties']['scaling'] = RUN_SCALING
process_config_json[0]['runs'][0]['properties']['upload_to_opdm'] = UPLOAD_TO_OPDM
process_config_json[0]['runs'][0]['properties']['upload_to_minio'] = UPLOAD_TO_MINIO
process_config_json[0]['runs'][0]['properties']['send_merge_report'] = SEND_MERGE_REPORT
process_config_json[0]['runs'][0]['properties']['post_temp_fixes'] = POST_TEMP_FIXES
process_config_json[0]['runs'][0]['properties']['force_outage_fix'] = FORCE_OUTAGE_FIX

# If single timestamp run is defined
if TIMESTAMP:
    timeframe_config_json[0]['reference_time_start'] = TASK_REFERENCE_TIME
    timeframe_config_json[0]['reference_time_end'] = TASK_REFERENCE_TIME
    timeframe_config_json[0]['period_start'] = 'PT0M'
    timeframe_config_json[0]['period_end'] = 'PT1H'

# Generate tasks
tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_config_json, timeframe_config_json,
                            TIMESTAMP, PROCESS_TIME_SHIFT))

# Publish tasks
if tasks:
    logger.info(f"Creating connection to RMQ")
    rabbit_service = rabbit.BlockingClient()
    logger.info(f"Sending tasks to Rabbit exchange: {RMQ_EXCHANGE}")
    for task in tasks:
        rabbit_service.publish(payload=json.dumps(task),
                               exchange_name=RMQ_EXCHANGE,
                               headers=filter_and_flatten_dict(task, TASK_HEADER_KEYS.split(",")))
else:
    logger.info("No tasks generated at current time.")
