import json
import config
import logging
from emf.task_generator.task_generator import generate_tasks, filter_and_flatten_dict
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties
from emf.common.logging.custom_logger import initialize_custom_logger

logger = logging.getLogger("task_generator.worker")
elk_handler = initialize_custom_logger()

parse_app_properties(globals(), config.paths.task_generator.manual_task_generator)

timeframe_conf = config.paths.task_generator.timeframe_conf
process_conf = config.paths.task_generator.process_conf

process_config_json = json.load(process_conf)
timeframe_config_json = json.load(timeframe_conf)

#Testing locally
#RUN_TYPE = "WeekAheadRMM"
print(RUN_TYPE)

#Based on run time get pro
for merge_type in process_config_json:
    for run in merge_type.get("runs",[]):
        if RUN_TYPE in run["@id"]:
            process_config_json[0]['runs'][0]= run
            for time_conf in timeframe_config_json:
                if run["time_frame"] in time_conf["@id"]:
                    timeframe_config_json[0] = time_conf


process_config_json[0]['runs'][0]['run_at'] = '* * * * *'

# process_config_json[0]['runs'][0]['properties']['merge_type'] = MERGE_TYPE
# process_config_json[0]['runs'][0]['properties']['time_horizon'] = TIME_HORIZON
# process_config_json[0]['runs'][0]['properties']['merging_entity'] = TASK_MERGING_ENTITY
# process_config_json[0]['runs'][0]['properties']['mas'] = TASK_MAS
process_config_json[0]['runs'][0]['properties']['included'] = [tso.strip() for tso in INCLUDED_TSO.split(',')] if INCLUDED_TSO else []
process_config_json[0]['runs'][0]['properties']['excluded'] = [tso.strip() for tso in EXCLUDED_TSO.split(',')] if EXCLUDED_TSO else []
process_config_json[0]['runs'][0]['properties']['local_import'] = [tso.strip() for tso in LOCAL_IMPORT.split(',')] if LOCAL_IMPORT else []
process_config_json[0]['runs'][0]['properties']['version'] = TASK_VERSION
process_config_json[0]['runs'][0]['properties']['replacement'] = RUN_REPLACEMENT
process_config_json[0]['runs'][0]['properties']['replacement_local'] = RUN_REPLACEMENT_LOCAL
process_config_json[0]['runs'][0]['properties']['scaling'] = RUN_SCALING
process_config_json[0]['runs'][0]['properties']['upload_to_opdm'] = UPLOAD_TO_OPDM
process_config_json[0]['runs'][0]['properties']['upload_to_minio'] = UPLOAD_TO_MINIO
process_config_json[0]['runs'][0]['properties']['send_merge_report'] = SEND_MERGE_REPORT
process_config_json[0]['runs'][0]['properties']['pre_temp_fixes'] = PRE_TEMP_FIXES
process_config_json[0]['runs'][0]['properties']['post_temp_fixes'] = POST_TEMP_FIXES
process_config_json[0]['runs'][0]['properties']['force_outage_fix'] = FORCE_OUTAGE_FIX


if PROCESS_TIME_SHIFT:
    timeframe_config_json[0]['period_start'] = f'{PROCESS_TIME_SHIFT}'
if TIMETRAVEL:
    timeframe_config_json[0]['period_duration'] = TASK_PERIOD_DURATION
    timeframe_config_json[0]['reference_time'] = TASK_REFERENCE_TIME


with open(process_conf, 'w') as file:
    json.dump(process_config_json, file, indent=1)

with open(timeframe_conf, 'w') as file:
    json.dump(timeframe_config_json, file, indent=4)

tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, process_conf, timeframe_conf, TIMETRAVEL))

if tasks:
    logger.info(f"Creating connection to RMQ")
    rabbit_service = rabbit.BlockingClient(host=RMQ_SERVER)
    logger.info(f"Sending tasks to Rabbit exchange '{RMQ_EXCHANGE}'")
    for task in tasks:
        rabbit_service.publish(payload=json.dumps(task), exchange_name=RMQ_EXCHANGE, headers=filter_and_flatten_dict(task, TASK_HEADER_KEYS.split(",")))
else:
    logger.info("No tasks generated at current time.")
