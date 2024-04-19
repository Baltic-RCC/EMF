import datetime
import logging
import time
import os

import config
import json
import sys
from json import JSONDecodeError

from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic
from aniso8601 import parse_datetime
from emf.common.logging.custom_logger import ElkLoggingHandler
from emf.loadflow_tool.model_merger import (CgmModelComposer, get_models, get_local_models, PROCESS_ID_KEYWORD,
                                            RUN_ID_KEYWORD, JOB_ID_KEYWORD, save_merged_model_to_local_storage,
                                            publish_merged_model_to_opdm, save_merged_model_to_minio,
                                            publish_metadata_to_elastic, DEFAULT_MERGE_TYPES, AREA_KEYWORD,
                                            DEFAULT_AREA, INCLUDED_TSO_KEYWORD, DownloadModels, EXCLUDED_TSO_KEYWORD,
                                            get_version_number)
from emf.task_generator.time_helper import parse_duration

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.model_merge)

# TODO handle these constants
NUMBER_OF_CGM_TRIES = 3
NUMBER_OF_CGM_TRIES_KEYWORD = 'task_retry_count'
TASK_TIMEOUT = 'PT5M'
TASK_TIMEOUT_KEYWORD = 'task_timeout'
SLEEP_BETWEEN_TRIES = 'PT1M'

TASK_PROPERTIES_KEYWORD = 'task_properties'
TIMESTAMP_KEYWORD = 'timestamp_utc'
MERGE_TYPE_KEYWORD = 'merge_type'
TIME_HORIZON_KEYWORD = 'time_horizon'

SAVE_MERGED_MODEL_TO_LOCAL_STORAGE = True
PUBLISH_MERGED_MODEL_TO_MINIO = True
PUBLISH_MERGED_MODEL_TO_OPDM = False
PUBLISH_METADATA_TO_ELASTIC = False

failed_cases_collector = []
succeeded_cases_collector = []


def running_in_local_machine():
    """
    For debugging purposes only
    """
    return "PYCHARM_HOSTED" in os.environ


def flatten_tuple(data):
    """
    Flattens the nested tuple to eventually a single level tuple.
    Use this when passing args as is from one handler to another
    :param data: tuple of arguments
    :return levelled tuple
    """
    if isinstance(data, tuple):
        if len(data) == 0:
            return ()
        else:
            return flatten_tuple(data[0]) + flatten_tuple(data[1:])
    else:
        return (data,)


def find_key(input_dictionary: dict, key):
    """
    Searches in depth for a key in dictionary
    :param input_dictionary:  from where to search
    :param key: key to be searched
    :return value of the key if found, None otherwise
    """
    if key in input_dictionary:
        return input_dictionary[key]
    for value in input_dictionary.values():
        if isinstance(value, dict):
            result = find_key(input_dictionary=value, key=key)
            if result is not None:
                return result
    return None


def get_payload(args, keyword: str = MERGE_TYPE_KEYWORD):
    """
    Searches keyword from args. Tries to parse the arg to dict and checks if keyword is present. if it
    is returns the arg
    :param args: tuple of args
    :param keyword: keyword to be searched
    :return argument which is dictionary and has the keyword or None
    """
    args = flatten_tuple(args)
    if args and len(args) > 0:
        for argument in args:
            try:
                if isinstance(argument, dict):
                    dict_value = argument
                else:
                    dict_value = json.loads(argument.decode('utf-8'))
                if not find_key(dict_value, keyword):
                    raise UnknownArgumentException
                return dict_value
            except JSONDecodeError:
                continue
    return None


def run_sleep_timer(time_value: any = None):
    """
    Waits for some given time
    :param time_value: seconds to wait
    """
    if time_value is not None:
        if isinstance(time_value, float) and time_value > 0:
            time.sleep(time_value)


class UnknownArgumentException(JSONDecodeError):
    pass


def handle_not_received_case(message):
    """
    Do something if models were not found
    TODO: report rabbit context if available
    :param message: log error message
    """
    logger.error(message)
    # currently in the debugging mode do not consume more messages
    if running_in_local_machine():
        failed_cases_collector.append(message)
        # raise SystemExit


class HandlerGetModels:
    """
    This one gathers the necessary data
    """

    def __init__(self,
                 logger_handler: ElkLoggingHandler = None,
                 number_of_igm_tries: int = NUMBER_OF_CGM_TRIES,
                 default_area: str = DEFAULT_AREA,
                 cgm_minio_bucket: str = EMF_OS_MINIO_BUCKET,
                 cgm_minio_prefix: str = EMF_OS_MINIO_FOLDER,
                 merge_types: str |dict = MERGE_TYPES,
                 merging_entity: str = MERGING_ENTITY,
                 sleep_between_tries: str = SLEEP_BETWEEN_TRIES,
                 elk_index_version_number: str = ELK_VERSION_INDEX):
        """
        :param logger_handler: attach rabbit context to it
        :param number_of_igm_tries: max allowed tries before quitting
        :param default_area: default merging area
        :param cgm_minio_bucket: bucket where combined models are stored
        :param cgm_minio_prefix: prefix of models
        :param merge_types: the default dict consisting areas, included tsos and excluded tsos
        :param merging_entity: the name of the merging entity
        :param sleep_between_tries: sleep between igm requests if failed
        :param elk_index_version_number: elastic index from where look version number
        """
        self.number_of_igm_tries = number_of_igm_tries
        self.logger_handler = logger_handler
        self.opdm_service = None
        merge_types = merge_types or DEFAULT_MERGE_TYPES
        if isinstance(merge_types, str):
            merge_types = merge_types.replace("'", "\"")
            merge_types = json.loads(merge_types)
        self.merge_types = merge_types
        self.merging_entity = merging_entity
        self.cgm_minio_bucket = cgm_minio_bucket
        self.cgm_minio_prefix = cgm_minio_prefix
        self.sleep_between_tries = parse_duration(sleep_between_tries)
        self.elk_index_for_version_number = elk_index_version_number
        self.default_area = default_area

    def handle(self, *args, **kwargs):
        """
        Checks and parses the json, gathers necessary data and stores it to CGM_Composer and passes it on
        """
        # Check the args: if there is a dict, json that can be converted to dict and consists a keyword
        unnamed_args = args
        input_data = get_payload(unnamed_args, keyword=MERGE_TYPE_KEYWORD)
        # For debugging
        manual_testing = False
        if input_data is not None:
            if self.logger_handler is not None:
                # Pack rabbit context to elastic log handler
                self.logger_handler.extra.update({PROCESS_ID_KEYWORD: input_data.get(PROCESS_ID_KEYWORD),
                                                  RUN_ID_KEYWORD: input_data.get(RUN_ID_KEYWORD),
                                                  JOB_ID_KEYWORD: input_data.get(JOB_ID_KEYWORD)})
                logger.info(f"Logger was updated with process_id, run_id and job_id (under extra fields)")
            number_of_tries = self.number_of_igm_tries

            if TASK_PROPERTIES_KEYWORD in input_data and isinstance(input_data[TASK_PROPERTIES_KEYWORD], dict):
                # Unpack the properties section
                scenario_date = input_data[TASK_PROPERTIES_KEYWORD].get(TIMESTAMP_KEYWORD)
                time_horizon = input_data[TASK_PROPERTIES_KEYWORD].get(TIME_HORIZON_KEYWORD)
                merge_type = input_data[TASK_PROPERTIES_KEYWORD].get(MERGE_TYPE_KEYWORD)
                # Get some models, allow only max tries
                get_igms_try = 1
                available_models, latest_boundary = None, None
                # Extract tso and area data
                area = self.merge_types.get(merge_type, {}).get(AREA_KEYWORD, self.default_area)
                included_tsos = self.merge_types.get(merge_type, {}).get(INCLUDED_TSO_KEYWORD, [])
                excluded_tsos = self.merge_types.get(merge_type, {}).get(EXCLUDED_TSO_KEYWORD, [])
                while get_igms_try <= number_of_tries:
                    if running_in_local_machine() and manual_testing:
                        available_models, latest_boundary = get_local_models(time_horizon=time_horizon,
                                                                             scenario_date=scenario_date,
                                                                             download_policy=DownloadModels.MINIO,
                                                                             use_local_files=True)
                    else:
                        available_models, latest_boundary = get_models(time_horizon=time_horizon,
                                                                       scenario_date=scenario_date,
                                                                       download_policy=DownloadModels.MINIO,
                                                                       included_tsos=included_tsos,
                                                                       excluded_tsos=excluded_tsos)
                    available_models = [model for model in available_models
                                        if model.get('pmd:TSO') not in ['APG', 'SEPS', '50Hertz']]
                    if available_models and latest_boundary:
                        break
                    message = []
                    if not available_models:
                        message.append('models')
                    if not latest_boundary:
                        message.append('latest_boundary')
                    sleepy_message = f"Going to sleep {self.sleep_between_tries}"
                    logger.warning(f"Failed get {' and '.join(message)}. {sleepy_message}")
                    time.sleep(self.sleep_between_tries)
                    get_igms_try += 1
                # If no luck report to elastic and call it a day
                if not available_models and not latest_boundary:
                    handle_not_received_case(f"Get Models: nothing got")
                # Get the version number
                version_number = get_version_number(scenario_date=scenario_date,
                                                    time_horizon=time_horizon,
                                                    modeling_entity=f"{self.merging_entity}-{area}")
                # Pack everything and pass it on
                cgm_input = CgmModelComposer(igm_models=available_models,
                                             boundary_data=latest_boundary,
                                             time_horizon=time_horizon,
                                             scenario_date=scenario_date,
                                             area=area,
                                             merging_entity=self.merging_entity,
                                             rabbit_data=input_data,
                                             version=version_number)
                return cgm_input, args, kwargs
        return args, kwargs


class HandlerMergeModels:
    def __init__(self,
                 publish_to_opdm: bool = PUBLISH_MERGED_MODEL_TO_OPDM,
                 publish_to_minio: bool = PUBLISH_MERGED_MODEL_TO_MINIO,
                 minio_bucket: str = EMF_OS_MINIO_BUCKET,
                 folder_in_bucket: str = EMF_OS_MINIO_FOLDER,
                 save_to_local_storage: bool = SAVE_MERGED_MODEL_TO_LOCAL_STORAGE,
                 publish_to_elastic: bool = PUBLISH_METADATA_TO_ELASTIC,
                 elk_server: str = elastic.ELK_SERVER,
                 cgm_index: str = ELK_VERSION_INDEX):
        """
        Initializes the handler which starts to send out merged models
        :param publish_to_opdm: publish cgm to opdm
        :param publish_to_minio: save cgm to minio
        :param minio_bucket: bucket where to store models
        :param folder_in_bucket: prefix for models
        :param save_to_local_storage: whether to save to local storage
        :param publish_to_elastic: save metadata to elastic
        :param elk_server: name of the elastic server
        :param cgm_index: index in the elastic where to send the metadata
        """
        self.minio_bucket = minio_bucket
        self.folder_in_bucket = folder_in_bucket
        self.use_folders = True
        self.elastic_server = elk_server
        self.cgm_index = cgm_index
        self.save_to_local_storage = save_to_local_storage
        self.send_to_minio = publish_to_minio
        self.send_to_opdm = publish_to_opdm
        self.send_to_elastic = publish_to_elastic

    def handle(self, *args, **kwargs):
        """
        Calls the merge and posts the results
        """
        # check if CgmModelComposerCgmModelComposer is in args
        args = flatten_tuple(args)

        cgm_compose = None
        # Check if CgmModelComposer is in args
        for item in args:
            if isinstance(item, CgmModelComposer):
                cgm_compose = item
                break
        # check if CgmModelComposer is in kwargs
        if cgm_compose is None:
            for key in kwargs:
                if isinstance(kwargs[key], CgmModelComposer):
                    cgm_compose = kwargs[key]
                    break
        # If there was nothing, report and return
        if cgm_compose is None:
            handle_not_received_case("Merger: no inputs received")
            logger.error(f"Pipeline failed, no dataclass present with igms")
            return args, kwargs
        # else merge the model and start sending it out
        try:
            cgm_compose.compose_cgm()
            # Get the files
            cgm_files = cgm_compose.cgm
            folder_name = cgm_compose.get_folder_name()
            # And send them out
            if self.send_to_opdm or not running_in_local_machine():
                publish_merged_model_to_opdm(cgm_files=cgm_files)
            if self.send_to_minio or not running_in_local_machine():
                save_merged_model_to_minio(cgm_files=cgm_files,
                                           minio_bucket=self.minio_bucket,
                                           folder_in_bucket=self.folder_in_bucket)
            # For the future reference, store merge data to elastic
            if self.send_to_elastic and False:
                consolidated_metadata = cgm_compose.get_consolidated_metadata()
                publish_metadata_to_elastic(metadata=consolidated_metadata,
                                            cgm_index=self.cgm_index,
                                            elastic_server=self.elastic_server)
            if self.save_to_local_storage or running_in_local_machine():
                save_merged_model_to_local_storage(cgm_files=cgm_files,
                                                   cgm_folder_name=folder_name)
            if running_in_local_machine():
                succeeded_cases_collector.append(cgm_compose.get_log_message())
        except Exception as ex_msg:
            handle_not_received_case(f"Merger: {cgm_compose.get_log_message()} exception: {ex_msg}")
        return args, kwargs


if __name__ == "__main__":

    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    testing_time_horizon = '1D'
    testing_merging_type = 'CGM'
    start_date = parse_datetime("2024-04-11T00:30:00+00:00")
    end_date = parse_datetime("2024-04-12T00:00:00+00:00")

    delta = end_date - start_date
    delta_sec = delta.days * 24 * 3600 + delta.seconds
    # Generate time array with 1h interval
    time_step = 3600
    testing_scenario_dates = [(start_date + datetime.timedelta(0, t)).isoformat()
                              for t in range(0, delta_sec, time_step)]

    for testing_scenario_date in testing_scenario_dates:
        example_input_from_rabbit = {
            "@context": "https://example.com/task_context.jsonld",
            "@type": "Task",
            "@id": "urn:uuid:f9d476ec-2507-4ad2-8a37-72afdcd68bbf",
            "process_id": "https://example.com/processes/CGM_CREATION",
            "run_id": "https://example.com/runs/IntraDayCGM/1",
            "job_id": "urn:uuid:00815bce-5cb5-4f45-8541-c5642680d474",
            "task_type": "automatic",
            "task_initiator": "some.body",
            "task_priority": "normal",
            "task_creation_time": "2024-04-04T06:57:51.018050",
            "task_status": "created",
            "task_status_trace":
                [
                    {
                        "status": "created",
                        "timestamp": "2024-04-04T06:57:51.018050"
                    }
                ],
            "task_dependencies": [],
            "task_tags": [],
            "task_retry_count": 0,
            "task_timeout": "PT1H",
            "task_gate_open": "2024-04-04T04:00:00+00:00",
            "task_gate_close": "2024-04-04T04:15:00+00:00",
            "job_period_start": "2024-04-04T05:00:00+00:00",
            "job_period_end": "2024-04-04T13:00:00+00:00",
            "task_properties":
                {
                    "timestamp_utc": testing_scenario_date,
                    "merge_type": testing_merging_type,
                    "time_horizon": testing_time_horizon
                }
        }

        message_handlers = [HandlerGetModels(), HandlerMergeModels()]
        body = (example_input_from_rabbit,)
        properties = {}
        for message_handler in message_handlers:
            try:
                logger.info(f"---Handling message with {message_handler.__class__.__name__}")
                body = message_handler.handle(body, properties=properties)
            except Exception as ex:
                logger.error(f"Message handling failed: {ex}")
    if running_in_local_machine():
        print("FAILED:")
        print('\r\n'.join(failed_cases_collector))
        print("SUCCESS:")
        print('\r\n'.join(succeeded_cases_collector))
