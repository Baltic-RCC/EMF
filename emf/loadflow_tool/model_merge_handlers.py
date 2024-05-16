import base64
import datetime
import logging
import time
import os
import uuid
from enum import Enum

import config
import json
from json import JSONDecodeError

from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic
from aniso8601 import parse_datetime
from emf.common.logging.custom_logger import ElkLoggingHandler, initialize_custom_logger
from emf.loadflow_tool.loadflow_settings import CGM_RELAXED_2
from emf.loadflow_tool.model_merger import (CgmModelComposer, get_models, PROCESS_ID_KEYWORD,
                                            RUN_ID_KEYWORD, JOB_ID_KEYWORD, save_merged_model_to_local_storage,
                                            publish_merged_model_to_opdm, save_merged_model_to_minio,
                                            publish_metadata_to_elastic,
                                            DownloadModels, get_version_number, CgmExportType, TASK_PROPERTIES_KEYWORD,
                                            MERGE_TYPE_KEYWORD, INCLUDED_TSO_KEYWORD, EXCLUDED_TSO_KEYWORD,
                                            IMPORT_TSO_LOCALLY_KEYWORD, get_local_models, MODELS_KEYWORD, TASK_KEYWORD)
from emf.loadflow_tool.validator import send_cgm_qas_report, validate_cgm_model, OPDM_PROFILE_KEYWORD, DATA_KEYWORD, \
    OPDE_COMPONENT_KEYWORD
from emf.task_generator.time_helper import parse_duration

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.model_merge)

# How many times to try to get models
NUMBER_OF_CGM_TRIES = 3
SLEEP_BETWEEN_TRIES = 'PT5S'

# Rabbit context keywords
WORKER_KEYWORD = 'worker'
WORKER_UUID_KEYWORD = 'worker_uuid'

# Testing purposes only
failed_cases_collector = []
succeeded_cases_collector = []


class ContentExportType(Enum):
    DEFAULT = None
    JSON = 'json'


merging_type = ContentExportType[EXCHANGE_FORMAT]


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


def get_payload(argument):
    """
    Searches keyword from args. Tries to parse the arg to dict and checks if keyword is present. if it
    is returns the arg
    :param argument: tuple of args
    :return argument which is dictionary and has the keyword or None
    """
    try:
        if isinstance(argument, dict):
            content = argument
        else:
            content = convert_from_format(argument, content_format=ContentExportType.JSON)
        task_data = find_key(content, TASK_KEYWORD) or content if find_key(content, MERGE_TYPE_KEYWORD) else None
        model_data = find_key(content, MODELS_KEYWORD)
        return task_data, model_data
    except JSONDecodeError:
        return None, None
    except Exception as ex_value:
        logger.warning(f"Unable to parse {argument}: {ex_value}")


class UnknownInputException(JSONDecodeError):
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


def add_field_to_dict_if_exists(input_dict: dict, output_dict: dict, input_dict_keyword, output_dict_keyword=None):
    """
    Adds a field to output_dict from input_dict if field exists in input_dict (for escaping None fields in the
    output logs)
    :param input_dict: dictionary from where to get the field
    :param output_dict: dictionary to where to put the field
    :param input_dict_keyword: key in input_dict
    :param output_dict_keyword: key in output_dict, if not given input_dict_keyword is used instead
    """
    output_dict_keyword = output_dict_keyword or input_dict_keyword
    if field_value := input_dict.get(input_dict_keyword):
        output_dict[output_dict_keyword] = field_value
    return output_dict


def get_first_elk_logging_handler(log_handler: ElkLoggingHandler = None):
    """
    Gets first ElkLoggingHandler instance from root logger handlers
    :param log_handler: input log_handler (if it defined)
    """
    if not isinstance(log_handler, ElkLoggingHandler):
        handlers = logging.getLogger().handlers
        for handler in handlers:
            if isinstance(handler, ElkLoggingHandler):
                log_handler = handler
                break
    return log_handler


def set_logger_to_report_rabbit_params(rabbit_data: dict,
                                       log_handler: ElkLoggingHandler = None,
                                       worker_name: str = 'model-merger',
                                       overwrite_existing_extra: bool = False):
    """
    Adds extra fields to logger based on the rabbit data
    :param log_handler: instance of logger, must contain the field "extra"
    :param rabbit_data: rabbit message data
    :param worker_name: name of the worker
    :param overwrite_existing_extra: reset the fields if necessary
    """
    # if handler is not given iterate over the existing ones and find the first that has Elk connection capability
    # attach the parameters to it and carry on
    log_handler = get_first_elk_logging_handler(log_handler=log_handler)
    if isinstance(log_handler, ElkLoggingHandler):
        extra_data = log_handler.extra
        if not extra_data or overwrite_existing_extra:
            extra_data = {}
        extra_data[WORKER_KEYWORD] = extra_data.get(WORKER_KEYWORD, worker_name)
        extra_data[WORKER_UUID_KEYWORD] = extra_data.get(WORKER_UUID_KEYWORD, str(uuid.uuid4()))
        extra_data = add_field_to_dict_if_exists(rabbit_data, extra_data, PROCESS_ID_KEYWORD)
        extra_data = add_field_to_dict_if_exists(rabbit_data, extra_data, RUN_ID_KEYWORD)
        extra_data = add_field_to_dict_if_exists(rabbit_data, extra_data, JOB_ID_KEYWORD)
        log_handler.extra = extra_data
    return log_handler


def unset_logger_to_report_rabbit_params(log_handler: ElkLoggingHandler):
    """
    Removes extra fields from the logger
    For example if running multiple jobs in same pipeline
    :param log_handler: instance of Elk log handler which contains extra field
    """
    log_handler = get_first_elk_logging_handler(log_handler=log_handler)
    if isinstance(log_handler, ElkLoggingHandler):
        extra_data = log_handler.extra
        # extra_data.pop(WORKER_KEYWORD, None)
        # extra_data.pop(WORKER_UUID_KEYWORD, None)
        extra_data.pop(PROCESS_ID_KEYWORD, None)
        extra_data.pop(RUN_ID_KEYWORD, None)
        extra_data.pop(JOB_ID_KEYWORD, None)
        log_handler.extra = extra_data


def set_cgm_composer_from_input(*args, **kwargs):
    """
    Searches the dedicated fields from the input and tries to parse them to CgmComposer object
    """
    args = flatten_tuple(args)
    cgm_composer = CgmModelComposer()
    # Check if CgmModelComposer is in args
    task_data = None
    models_data = None
    # Check args
    for item in args:
        task_data, models_data = get_payload(argument=item)
        if task_data or models_data:
            break
    # if nothing was found
    if not task_data and not models_data:
        # Check kwargs
        for key in kwargs:
            task_data, models_data = get_payload(argument=kwargs[key])
            if task_data or models_data:
                break
    if not task_data and not models_data:
        handle_not_received_case("No inputs received")
    cgm_composer.set_task_data(task_data=task_data)
    cgm_composer.set_models(models_data=models_data)
    return cgm_composer


def file_bytes_to_json_string(data_file: bytes):
    """
    Converts input file to string
    :param data_file: byte array representing a file
    :return: char array as a string
    """
    encoded_data = base64.b64encode(data_file)
    data_json = encoded_data.decode('utf-8')
    return data_json


def file_string_to_bytes(data_json: str):
    """
    Tries to convert a string to bytes
    :param data_json: file represented as a string
    :returns: converted file
    :throws JSONDecodeError if not able to convert
    """
    try:
        encoded_data = data_json.encode('utf-8')
        data_file = base64.b64decode(encoded_data)
        return data_file
    except Exception:
        raise UnknownInputException


def convert_to_format(content: dict, content_format: ContentExportType = merging_type):
    """
    Gets data in specified format, currently, dict or json
    :param content: input content as string
    :param content_format: specifies content export format, currently default: dict, and json: dict-> json
    :return content of the CgmComposer
    """
    # Expand this to add additional formats if needed
    if content_format == content_format.JSON:
        parsed_model_data = []
        model_data = content.get(MODELS_KEYWORD)
        for model in model_data:
            component = model.get(OPDE_COMPONENT_KEYWORD, [])
            for profile in component:
                data_json = file_bytes_to_json_string(profile.get(OPDM_PROFILE_KEYWORD, {}).get(DATA_KEYWORD))
                profile[OPDM_PROFILE_KEYWORD][DATA_KEYWORD] = data_json
            model[OPDE_COMPONENT_KEYWORD] = component
            parsed_model_data.append(model)
        model_data = parsed_model_data
        content[MODELS_KEYWORD] = model_data
        content = json.dumps(content)
    return content


def convert_from_format(input_data, content_format: ContentExportType = merging_type):
    """
    Gets data in specified format, currently, dict or json
    :param input_data: input content as string
    :param content_format: specifies content export format, currently default: dict, and json: dict-> json
    :return content of the CgmComposer
    """
    content = None
    if content_format == content_format.JSON:
        content = json.loads(input_data)
        fixed_model_data = []
        if model_data := content.get(MODELS_KEYWORD):
            for model in model_data:
                component = model.get(OPDE_COMPONENT_KEYWORD, [])
                for profile in component:
                    data_bytes = file_string_to_bytes(profile.get(OPDM_PROFILE_KEYWORD, {}).get(DATA_KEYWORD))
                    profile[OPDM_PROFILE_KEYWORD][DATA_KEYWORD] = data_bytes
                model[OPDE_COMPONENT_KEYWORD] = component
                fixed_model_data.append(model)
            model_data = fixed_model_data
            content[MODELS_KEYWORD] = model_data
    return content


class HandlerGetModels:
    """
    This one gathers the necessary data
    """

    def __init__(self,
                 logger_handler: ElkLoggingHandler = None,
                 number_of_igm_tries: int = NUMBER_OF_CGM_TRIES,
                 cgm_minio_bucket: str = EMF_OS_MINIO_OPDE_MODELS_BUCKET,
                 cgm_minio_prefix: str = EMF_OS_MINIO_OPDE_MODELS_FOLDER,
                 merge_types: str | dict = MERGE_TYPES,
                 get_model_policy: str = EMF_OS_CGM_GET_MODEL_FROM,
                 use_fallback_task_properties: bool = False,
                 sleep_between_tries: str = SLEEP_BETWEEN_TRIES,
                 elk_index_version_number: str = ELASTIC_LOGS_INDEX,
                 debugging: bool = False):
        """
        :param logger_handler: attach rabbit context to it
        :param number_of_igm_tries: max allowed tries before quitting
        :param cgm_minio_bucket: bucket where combined models are stored
        :param cgm_minio_prefix: prefix of models
        :param merge_types: the default dict consisting areas, included tsos and excluded tsos
        :param use_fallback_task_properties: use merge types specified in properties section
        :param sleep_between_tries: sleep between igm requests if failed
        :param elk_index_version_number: elastic index from where look version number
        :param debugging: whether the debugging is allowed
        """
        self.number_of_igm_tries = number_of_igm_tries
        self.logger_handler = logger_handler or logger
        self.opdm_service = None
        if isinstance(merge_types, str):
            merge_types = merge_types.replace("'", "\"")
            merge_types = json.loads(merge_types)
        self.merge_types = merge_types
        self.use_fallback_task_properties = use_fallback_task_properties
        self.cgm_minio_bucket = cgm_minio_bucket
        self.cgm_minio_prefix = cgm_minio_prefix
        self.download_policy = DownloadModels[get_model_policy]
        self.sleep_between_tries = 0
        self.set_sleep_between_tries(input_value=sleep_between_tries)
        self.elk_index_for_version_number = elk_index_version_number
        self.debugging = debugging

    def set_sleep_between_tries(self, input_value: str | int):
        """
        Sets time to sleep between tries
        :param input_value: time interval value
        """
        if isinstance(input_value, str):
            input_value = parse_duration(input_value).total_seconds()
        self.sleep_between_tries = input_value

    def handle(self, *args, **kwargs):
        """
        Checks and parses the json, gathers necessary data and stores it to CGM_Composer and passes it on
        """
        get_igms_try = 1
        cgm_input = set_cgm_composer_from_input(args, kwargs)
        if input_data := cgm_input.task_data:
            self.logger_handler = set_logger_to_report_rabbit_params(log_handler=self.logger_handler,
                                                                     rabbit_data=input_data)
            task_properties_data = input_data.get(TASK_PROPERTIES_KEYWORD, {})
            included_tsos = task_properties_data.get(INCLUDED_TSO_KEYWORD, [])
            excluded_tsos = task_properties_data.get(EXCLUDED_TSO_KEYWORD, [])
            local_import_tsos = task_properties_data.get(IMPORT_TSO_LOCALLY_KEYWORD, [])
            # fallback to values in properties section if needed
            if self.use_fallback_task_properties:
                included_tsos = included_tsos or self.merge_types.get(cgm_input.area, {}).get(INCLUDED_TSO_KEYWORD, [])
                excluded_tsos = excluded_tsos or self.merge_types.get(cgm_input.area, {}).get(EXCLUDED_TSO_KEYWORD, [])
                local_import_tsos = (local_import_tsos or
                                     self.merge_types.get(cgm_input.area, {}).get(IMPORT_TSO_LOCALLY_KEYWORD, []))
            # One possible field to get from rabbit, currently keep it as is
            number_of_tries = self.number_of_igm_tries
            available_models, latest_boundary = None, None
            while get_igms_try <= number_of_tries:
                if running_in_local_machine() and self.debugging:
                    available_models, latest_boundary = get_local_models(time_horizon=cgm_input.time_horizon,
                                                                         scenario_date=cgm_input.scenario_date,
                                                                         download_policy=self.download_policy,
                                                                         use_local_files=True)
                else:
                    available_models, latest_boundary = get_models(time_horizon=cgm_input.time_horizon,
                                                                   scenario_date=cgm_input.scenario_date,
                                                                   download_policy=self.download_policy,
                                                                   included_tsos=included_tsos,
                                                                   excluded_tsos=excluded_tsos,
                                                                   locally_imported_tsos=local_import_tsos)
                if available_models and latest_boundary:
                    break
                logger.warning(f"Failed get models. Going to sleep {self.sleep_between_tries}")
                time.sleep(self.sleep_between_tries)
                get_igms_try += 1
            # If no luck report to elastic and call it a day
            if not available_models and not latest_boundary:
                handle_not_received_case(f"Get Models: nothing found")
            cgm_input.igm_models = available_models
            cgm_input.boundary_data = latest_boundary
            # Get the version number
            cgm_input.version = get_version_number(scenario_date=cgm_input.scenario_date,
                                                   time_horizon=cgm_input.time_horizon,
                                                   modeling_entity=f"{cgm_input.merging_entity}-{cgm_input.area}")
            content = convert_to_format(cgm_input.get_content())
            unset_logger_to_report_rabbit_params(self.logger_handler)
            return content, args, kwargs
        return args, kwargs


class HandlerMergeModels:

    def __init__(self,
                 check_cgm_validity: bool = False):
        """
        Initializes the handler which merges and validates the model
        :param check_cgm_validity: publish cgm to opdm
        """
        self.validate_cgm_model = check_cgm_validity

    def handle(self, *args, **kwargs):
        """
        Calls the merge and posts the results
        """
        # check if CgmModelComposerCgmModelComposer is in args
        cgm_input = set_cgm_composer_from_input(args, kwargs)

        # If there was nothing, report and return
        if (cgm_input.igm_models is None) or (cgm_input.boundary_data is None):
            handle_not_received_case("Merger: missing models")
            return args, kwargs
        # This is an example, set fields to logger in each step separately
        logger_handler = set_logger_to_report_rabbit_params(rabbit_data=cgm_input.task_data)
        try:
            cgm_input.compose_cgm()
            qas_meta_data = cgm_input.get_data_for_qas()
            # Turn this part on if there is need to go through validate_model
            if self.validate_cgm_model:
                # Either validate it if needed
                opdm_objects = cgm_input.get_cgm(export_type=CgmExportType.FULL)
                validation_result = validate_cgm_model(opdm_objects,
                                                       loadflow_parameters=CGM_RELAXED_2,
                                                       run_element_validations=False,
                                                       report_data=qas_meta_data,
                                                       send_qas_report=True,
                                                       report_type="CGM",
                                                       debugging=True)
                logger.info(f"CGM validation: {validation_result.get('VALIDATION_STATUS', {}).get('valid')}")
            else:
                # Or send it as is
                send_cgm_qas_report(qas_meta_data=qas_meta_data)
        except Exception as ex_msg:
            handle_not_received_case(f"Merger: {cgm_input.get_log_message()} exception: {ex_msg}")
        # This is an example, after finishing the step release the fields
        unset_logger_to_report_rabbit_params(log_handler=logger_handler)
        content = convert_to_format(cgm_input.get_content())
        return content, args, kwargs


class HandlerPostMergedModel:

    def __init__(self,
                 publish_to_opdm: bool = EMF_OS_CGM_SAVE_OPDM,
                 publish_to_minio: bool = EMF_OS_CGM_SAVE_MINIO,
                 minio_bucket: str = EMF_OS_MINIO_OPDE_MODELS_BUCKET,
                 folder_in_bucket: str = EMF_OS_MINIO_OPDE_MODELS_FOLDER,
                 save_to_local_storage: bool = EMF_OS_CGM_SAVE_OPDM,
                 publish_to_elastic: bool = False,
                 elk_server: str = elastic.ELK_SERVER,
                 cgm_index: str = ELASTIC_LOGS_INDEX,
                 full_export_needed: bool = MINIO_EXPORT_FULL_MODEL):
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
        :param full_export_needed: specify if full model (igms+cgm+boundary is needed)
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
        self.export_type = CgmExportType.FULL_FILES_ONLY if full_export_needed else CgmExportType.BARE

    def handle(self, *args, **kwargs):
        """
        Calls the merge and posts the results
        """
        # check if CgmModelComposerCgmModelComposer is in args
        cgm_input = set_cgm_composer_from_input(args, kwargs)
        # If there was nothing, report and return
        if not cgm_input.get_cgm():
            handle_not_received_case("Post merged model: missing CGM")
            return args, kwargs
        # This is an example, set fields to logger in each step separately
        logger_handler = set_logger_to_report_rabbit_params(rabbit_data=cgm_input.task_data)
        try:
            cgm_files_bare = cgm_input.get_cgm(export_type=CgmExportType.BARE)
            cgm_files = cgm_input.get_cgm(export_type=self.export_type)
            folder_name = cgm_input.get_folder_name()
            # And send them out
            if self.send_to_opdm:
                publish_merged_model_to_opdm(cgm_files=cgm_files_bare)
            if self.send_to_minio:
                save_merged_model_to_minio(cgm_files=cgm_files,
                                           minio_bucket=self.minio_bucket,
                                           folder_in_bucket=self.folder_in_bucket,
                                           # comment next lines if following the path by file name is needed
                                           # if uncommented, gathers files to <t.horizon>/<m.entity>/<area>/<date>/<ver>
                                           merging_entity=cgm_input.merging_entity,
                                           time_horizon=cgm_input.time_horizon,
                                           scenario_datetime=cgm_input.scenario_date,
                                           area=cgm_input.area,
                                           version=cgm_input.version,
                                           # file_type='CGM'
                                           )
            # For the future reference, store merge data to elastic
            if self.send_to_elastic:
                consolidated_metadata = cgm_input.get_consolidated_metadata()
                publish_metadata_to_elastic(metadata=consolidated_metadata,
                                            cgm_index=self.cgm_index,
                                            elastic_server=self.elastic_server)
            if self.save_to_local_storage:
                save_merged_model_to_local_storage(cgm_files=cgm_files,
                                                   cgm_folder_name=folder_name)
            if running_in_local_machine():
                succeeded_cases_collector.append(cgm_input.get_log_message())
        except Exception as ex_msg:
            handle_not_received_case(f"Merger: {cgm_input.get_log_message()} exception: {ex_msg}")
        # This is an example, after finishing the step release the fields
        content = convert_to_format(cgm_input.get_content())
        unset_logger_to_report_rabbit_params(log_handler=logger_handler)
        return content, args, kwargs


if __name__ == "__main__":
    elk_handler = initialize_custom_logger()
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        # handlers=[logging.StreamHandler(sys.stdout)]
    )

    # testing_time_horizon = '1D'
    # testing_merging_type = 'BA'
    # testing_included_tsos = ['ELERING', 'AST', 'LITGRID', 'PSE']
    # testing_excluded_tsos = ['APG', '50Hertz']
    # testing_local_import = ['LITGRID']
    testing_time_horizon = '1D'
    testing_merging_type = 'EU'
    testing_included_tsos = []
    testing_excluded_tsos = []
    testing_local_import = ['LITGRID']
    start_date = parse_datetime("2024-05-09T00:30:00+00:00")
    end_date = parse_datetime("2024-05-10T00:00:00+00:00")

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
                    "included": testing_included_tsos,
                    "excluded": testing_excluded_tsos,
                    "local_import": testing_local_import,
                    "time_horizon": testing_time_horizon,
                }
        }

        message_handlers = [HandlerGetModels(debugging=False),
                            HandlerMergeModels(check_cgm_validity=False),
                            HandlerPostMergedModel(publish_to_opdm=False,
                                                   publish_to_minio=False,
                                                   save_to_local_storage=True)]
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
