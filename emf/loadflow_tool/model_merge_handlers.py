import logging
import time
import os

import requests.exceptions
import zeep.exceptions
import config
import json
import sys
from datetime import timedelta
from json import JSONDecodeError

from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, opdm, minio
from emf.common.logging.custom_logger import check_the_folder_path, SEPARATOR_SYMBOL, ElkLoggingHandler
from emf.loadflow_tool.model_merger import CgmModelComposer, get_models, \
    get_version_number_from_elastic, get_local_models, PROCESS_ID_KEYWORD, RUN_ID_KEYWORD, JOB_ID_KEYWORD, \
    get_version_number_from_minio
from emf.task_generator.time_helper import parse_duration

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.model_merge)

# Keywords used when parsing rabbit data
TASK_PROPERTIES_KEYWORD = 'task_properties'
TIMESTAMP_KEYWORD = 'timestamp_utc'
MERGE_TYPE_KEYWORD = 'merge_type'
TIME_HORIZON_KEYWORD = 'time_horizon'

LOCAL_STORAGE_LOCATION = './merged_examples/'

# Specify the different merge types, note that this is backup, priority is in config/cgm_worker/model_merge.properties
DEFAULT_MERGE_TYPES = {'CGM': {'AREA': 'EU', 'TSO': []},
                       'RMM': {'AREA': 'BA', 'TSO': ['ELERING', 'AST', 'LITGRID', 'PSE']}}
#WANTED_TSOS = ['50Hertz', 'D4', 'D7', 'ELES', 'ELIA', 'LITGRID', 'SEPS', 'TERNA', 'TTG']
AREA_KEYWORD = 'AREA'
DEFAULT_AREA = 'EU'
TSO_KEYWORD = 'TSO'
DEFAULT_TSO = []

# TODO handle these constants
NUMBER_OF_CGM_TRIES = 3
NUMBER_OF_CGM_TRIES_KEYWORD = 'task_retry_count'
TASK_TIMEOUT = 'PT5M'
TASK_TIMEOUT_KEYWORD = 'task_timeout'
SLEEP_BETWEEN_TRIES = 'PT1M'


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


def save_merged_model_to_local_storage(cgm_files,
                                       cgm_folder_name: str = None,
                                       local_storage_location: str = None):
    """
    Saves merged cgm to local storage. This is meant for testing purposes only
    :param cgm_files: list of cgm_files
    :param cgm_folder_name: sub folder name where to gather files
    :param local_storage_location: path to store
    :return: None
    """
    if not local_storage_location:
        return
    if cgm_folder_name is not None:
        local_storage_location = local_storage_location + '/' + cgm_folder_name
        local_storage_location = check_the_folder_path(local_storage_location)
    if not os.path.exists(local_storage_location):
        os.makedirs(local_storage_location)
    for cgm_file in cgm_files:
        full_file_name = local_storage_location + cgm_file.name
        with open(full_file_name, 'wb') as f:
            f.write(cgm_file.getbuffer())


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
    :param time_value: time to wait
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
    """
    logger.error(message)
    # currently in the debugging mode do not consume more messages
    if running_in_local_machine():
        raise SystemExit
    else:
        pass
    # raise SystemExit


class HandlerGetModels:
    """
    Handler for getting the models
    """

    def __init__(self,
                 logger_handler: ElkLoggingHandler = None,
                 number_of_igm_tries: int = NUMBER_OF_CGM_TRIES,
                 default_timeout: str = TASK_TIMEOUT,
                 default_area: str = DEFAULT_AREA,
                 default_tso=None,
                 cgm_minio_bucket: str = EMF_OS_MINIO_BUCKET,
                 cgm_minio_prefix: str = EMF_OS_MINIO_FOLDER,
                 merge_types: str = MERGE_TYPES,
                 merging_entity: str = MERGING_ENTITY,
                 sleep_between_tries: str = SLEEP_BETWEEN_TRIES,
                 elk_index_version_number: str = ELK_VERSION_INDEX):
        """
        So, where models may be: opdm, minio, somewhere else?
        """
        if default_tso is None:
            default_tso = DEFAULT_TSO
        self.number_of_igm_tries = number_of_igm_tries
        self.logger_handler = logger_handler
        self.opdm_service = None
        self.merge_types = DEFAULT_MERGE_TYPES
        self.merging_entity = merging_entity
        self.default_timeout = default_timeout
        self.cgm_minio_bucket = cgm_minio_bucket
        self.cgm_minio_prefix = cgm_minio_prefix
        self.sleep_between_tries = parse_duration(sleep_between_tries)
        self.elk_index_for_version_number = elk_index_version_number
        self.default_area = default_area
        self.default_tso = default_tso
        try:
            self.opdm_service = opdm.OPDM()
        except (ConnectionError, requests.exceptions.HTTPError) as con_err:
            logger.error(f"Unable to init HandlerGetModels, OPDM unreachable: {con_err}")
        try:
            self.merge_types = eval(merge_types)
        except Exception as ex:
            logger.warning(f"Unknown input: {ex}, taking default values")

    def handle(self, *args, **kwargs):
        """
        Checks and parses the json, gathers necessary data and stores it to CGM_Composer
        """
        # Check the args: if there is a dict, json that can be converted to dict and consists a keyword
        unnamed_args = args
        input_data = get_payload(unnamed_args, keyword=MERGE_TYPE_KEYWORD)
        # For debugging
        manual_testing = False
        if input_data is not None and self.logger_handler is not None:
            # Pack those things to logs
            self.logger_handler.extra.update({PROCESS_ID_KEYWORD: input_data.get(PROCESS_ID_KEYWORD),
                                              RUN_ID_KEYWORD: input_data.get(RUN_ID_KEYWORD),
                                              JOB_ID_KEYWORD: input_data.get(JOB_ID_KEYWORD)})
            logger.info(f"Logger was updated with process_id, run_id and job_id (under extra fields)")
            # number_of_tries = input_data.get(NUMBER_OF_CGM_TRIES_KEYWORD)
            number_of_tries = self.number_of_igm_tries
            task_timeout_value = parse_duration(input_data.get(TASK_TIMEOUT_KEYWORD, self.default_timeout))

            if TASK_PROPERTIES_KEYWORD in input_data and isinstance(input_data[TASK_PROPERTIES_KEYWORD], dict):
                scenario_date = input_data[TASK_PROPERTIES_KEYWORD].get(TIMESTAMP_KEYWORD)
                time_horizon = input_data[TASK_PROPERTIES_KEYWORD].get(TIME_HORIZON_KEYWORD)
                merge_type = input_data[TASK_PROPERTIES_KEYWORD].get(MERGE_TYPE_KEYWORD)
                # Get some models, allow only max tries
                get_igms_try = 1
                available_models, latest_boundary = None, None
                # Extract tso and area data
                area = self.merge_types.get(merge_type, {}).get(AREA_KEYWORD, self.default_area)
                tso_names = self.merge_types.get(merge_type, {}).get(TSO_KEYWORD, self.default_tso)
                while get_igms_try <= number_of_tries:
                    if running_in_local_machine() and manual_testing:
                        available_models, latest_boundary = get_local_models(time_horizon=time_horizon,
                                                                             scenario_date=scenario_date,
                                                                             use_local_files=True,
                                                                             opdm_client=self.opdm_service)
                    else:
                        available_models, latest_boundary = get_models(time_horizon=time_horizon,
                                                                       scenario_date=scenario_date,
                                                                       opdm_client=self.opdm_service,
                                                                       tso_names=tso_names)
                    if available_models and latest_boundary:
                        break
                    message = []
                    if not available_models:
                        message.append('models')
                    if not latest_boundary:
                        message.append('latest_boundary')
                    sleepy_message = f"Going to sleep {self.sleep_between_tries}" if task_timeout_value else ''
                    logger.warning(f"Failed get {' and '.join(message)}. {sleepy_message}")
                    time.sleep(self.sleep_between_tries)
                    get_igms_try += 1
                # If no luck report to elastic and call it a day
                if not available_models and not latest_boundary:
                    handle_not_received_case(f"Get Models: nothing got")
                # Get the version number
                version_elk = get_version_number_from_elastic(index_name=self.elk_index_for_version_number,
                                                              modeling_entity=f"{self.merging_entity}-{area}",
                                                              time_horizon=time_horizon,
                                                              scenario_date=scenario_date)
                version_minio = get_version_number_from_minio(minio_bucket=self.cgm_minio_bucket,
                                                              sub_folder=self.cgm_minio_prefix,
                                                              modeling_entity=f"{self.merging_entity}-{area}",
                                                              time_horizon=time_horizon,
                                                              scenario_date=scenario_date)
                version_number = version_minio if int(version_minio) > int(version_elk) else version_elk
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
                 minio_bucket: str = EMF_OS_MINIO_BUCKET,
                 elk_server: str = elastic.ELK_SERVER,
                 cgm_index: str = ELK_VERSION_INDEX,
                 folder_in_bucket: str = EMF_OS_MINIO_FOLDER):
        """
        Initializes the handler which starts to send out merged models
        :param minio_bucket:
        :param elk_server:
        :param cgm_index:
        :param folder_in_bucket:
        """
        self.opdm_service = None
        try:
            self.opdm_service = opdm.OPDM()
        except (ConnectionError, requests.exceptions.HTTPError) as con_err:
            logger.error(f"Unable to init HandlerMergeModels, OPDM unreachable: {con_err}")
        self.minio_bucket = minio_bucket
        self.folder_in_bucket = folder_in_bucket
        self.minio_service = minio.ObjectStorage()
        self.use_folders = True
        self.elastic_server = elk_server
        self.cgm_index = cgm_index

    def handle(self, *args, **kwargs):
        # check if CgmModelComposerCgmModelComposer is in args
        args = flatten_tuple(args)

        cgm_compose = None
        send_to_opdm = False
        send_to_minio = True
        send_to_elastic = False
        save_to_local_storage = True
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
            consolidated_metadata = cgm_compose.get_consolidated_metadata()
            folder_name = cgm_compose.get_folder_name()
            # And send them out
            if send_to_opdm or not running_in_local_machine():
                self.publish_merged_model_to_opdm(cgm_files=cgm_files, metadata=consolidated_metadata)
            if send_to_minio or not running_in_local_machine():
                self.save_merged_model_to_minio(cgm_files=cgm_files,
                                                cgm_folder_name=folder_name,
                                                metadata=consolidated_metadata)
            # For the future reference, store merge data to elastic
            if send_to_elastic or not running_in_local_machine() and False:
                self.publish_metadata_to_elastic(metadata=consolidated_metadata)
            if save_to_local_storage or running_in_local_machine():
                save_merged_model_to_local_storage(cgm_files=cgm_files,
                                                   local_storage_location=LOCAL_STORAGE_LOCATION,
                                                   cgm_folder_name=folder_name)
        except Exception as ex_msg:
            handle_not_received_case(f"Merger: {ex_msg}")
        return args, kwargs

    def publish_metadata_to_elastic(self, metadata: dict):
        """
        Publishes metadata to elastic
        :param metadata: metadata information
        :return response
        """

        if metadata:
            response = elastic.Elastic.send_to_elastic(index=self.cgm_index,
                                                       json_message=metadata,
                                                       server=self.elastic_server)
            return response

    def publish_merged_model_to_opdm(self, metadata: dict = None, cgm_files: list = None):
        """
        Sends files to opdm
        :param metadata: dict of metadata, no action intended
        :param cgm_files: list of files to be sent
        :return tuple of results
        """
        # Post files if given
        result = ()
        # Send metadata out if given
        # TODO handle metadata
        if metadata:
            pass
        # Send files out if given
        if cgm_files and len(cgm_files) > 0:
            opdm_publication_responses = []
            for instance_file in cgm_files:
                try:
                    if self.opdm_service is not None:
                        logger.info(f"Publishing {instance_file.name} to OPDM")
                        file_response = self.opdm_service.publication_request(instance_file, "CGMES")
                        opdm_publication_responses.append({"name": instance_file.name, "response": file_response})
                except zeep.exceptions.Fault as fault:
                    logger.error(f"Unable to send OPDM: {fault}")
            result = result + (opdm_publication_responses,)
        return result

    def save_merged_model_to_minio(self,
                                   cgm_files: [] = None,
                                   cgm_folder_name: str = None,
                                   metadata: dict = None,
                                   use_folders: bool = None):
        """
        Posts cgm files to minio
        :param cgm_files: list of individual cgm files
        :param metadata: used for creating the folder name
        :param cgm_folder_name: name of the sub folder to where to store the files
        :param use_folders: if true create folder where to store the data, otherwise just post to bucket
        :return: file name and link to file, the link to the file
        """
        if use_folders is None:
            use_folders = self.use_folders
        links_to_file = {}
        if self.minio_service is not None and cgm_files is not None:
            # check if the given bucket exists
            if not self.minio_service.client.bucket_exists(bucket_name=self.minio_bucket):
                logger.warning(f"{self.minio_bucket} does not exist")
                return links_to_file
            for cgm_file in cgm_files:
                file_name = cgm_file.name
                if use_folders:
                    if cgm_folder_name:
                        cgm_file.name = (self.folder_in_bucket + SEPARATOR_SYMBOL +
                                         cgm_folder_name + SEPARATOR_SYMBOL +
                                         cgm_file.name)
                    else:
                        cgm_file.name = (self.folder_in_bucket + SEPARATOR_SYMBOL +
                                         cgm_file.name)
                self.minio_service.upload_object(file_path_or_file_object=cgm_file,
                                                 bucket_name=self.minio_bucket,
                                                 # metadata=metadata
                                                 )
                time_to_expire = timedelta(days=7)
                link_to_file = self.minio_service.client.get_presigned_url(method="GET",
                                                                           bucket_name=self.minio_bucket,
                                                                           object_name=cgm_file.name,
                                                                           expires=time_to_expire)
                cgm_file.name = file_name
                links_to_file[file_name] = link_to_file
            return links_to_file


if __name__ == "__main__":
    # Testing
    # import sys
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
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
                "timestamp_utc": "2024-04-04T05:30:00+00:00",
                "merge_type": "CGM",
                "time_horizon": "ID"
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
