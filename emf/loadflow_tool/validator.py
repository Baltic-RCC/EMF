import os.path
import shutil
import zipfile
from datetime import datetime
from enum import Enum
from io import BytesIO
from os import listdir
from os.path import join
from zipfile import ZipFile
import ntpath

import logging
import time
import math

import requests
from aniso8601 import parse_datetime

import config
from dict2xml import dict2xml
from emf.common.logging.custom_logger import PyPowsyblLogGatherer, PyPowsyblLogReportingPolicy, check_the_folder_path
from emf.common.xslt_engine.saxonpy_api import xslt30_convert
from emf.loadflow_tool.loadflow_settings import *
from emf.loadflow_tool.helper import attr_to_dict, load_model, metadata_from_filename
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, rabbit
from pathlib import Path


# Initialize custom logger
# custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.validator)
parse_app_properties(caller_globals=globals(), path=config.paths.xslt_service.xslt)

# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue

ENTSOE_FOLDER = './path_to_ENTSOE_zip/TestConfigurations_packageCASv2.0'
CGM_XSL_PATH = "config/xslt_service/CGM_entsoeQAReport_Level_8.xsl"

OPDE_COMPONENT_KEYWORD = 'opde:Component'
OPDE_DEPENDENCIES_KEYWORD = 'opde:Dependencies'
OPDE_DEPENDS_ON_KEYWORD = 'opde:DependsOn'
OPDM_PROFILE_KEYWORD = 'opdm:Profile'
OPDM_OPDM_OBJECT_KEYWORD = 'opdm:OPDMObject'

PMD_TSO_KEYWORD = 'pmd:TSO'
PMD_FILENAME_KEYWORD = 'pmd:fileName'
PMD_CGMES_PROFILE_KEYWORD = 'pmd:cgmesProfile'
PMD_MODEL_PART_REFERENCE_KEYWORD = 'pmd:modelPartReference'
PMD_MERGING_ENTITY_KEYWORD = 'pmd:mergingEntity'
PMD_MERGING_AREA_KEYWORD = 'pmd:mergingArea'
PMD_SCENARIO_DATE_KEYWORD = 'pmd:scenarioDate'
PMD_VERSION_NUMBER_KEYWORD = "pmd:versionNumber"
PMD_TIME_HORIZON_KEYWORD = 'pmd:timeHorizon'
PMD_VALID_FROM_KEYWORD = 'pmd:validFrom'
PMD_CREATION_DATE_KEYWORD = 'pmd:creationDate'
PMD_MODEL_ID_KEYWORD = 'pmd:modelid'
PMD_MODELING_AUTHORITY_SET_KEYWORD = 'pmd:modelingAuthoritySet'

DATA_KEYWORD = 'DATA'

MODEL_MESSAGE_TYPE_KEYWORD = 'Model.messageType'
MODEL_MODELING_ENTITY_KEYWORD = 'Model.modelingEntity'
MODEL_MERGING_ENTITY_KEYWORD = 'Model.mergingEntity'
MODEL_DOMAIN_KEYWORD = 'Model.domain'
MODEL_FOR_ENTITY_KEYWORD = 'Model.forEntity'
MODEL_SCENARIO_TIME_KEYWORD = 'Model.scenarioTime'
MODEL_PROCESS_TYPE_KEYWORD = 'Model.processType'
MODEL_VERSION_KEYWORD = 'Model.version'

XML_KEYWORD = '.xml'
ZIP_KEYWORD = '.zip'

MISSING_TSO_NAME = 'UnknownTSO'
LONG_FILENAME_SUFFIX = u"\\\\?\\"

VALIDATION_STATUS_KEYWORD = 'VALIDATION_STATUS'
VALID_KEYWORD = 'valid'
VALIDATION_DURATION_KEYWORD = 'validation_duration_s'
LOADFLOW_RESULTS_KEYWORD = 'loadflow_results'

PREFERRED_FILE_TYPES = [XML_KEYWORD, ZIP_KEYWORD]
IGM_FILE_TYPES = ['_EQ_', '_TP_', '_SV_', '_SSH_']
BOUNDARY_FILE_TYPES = ['_EQBD_', '_TPBD_', '_EQ_BD_', '_TP_BD_']
BOUNDARY_FILE_TYPE_FIX = {'_EQ_BD_': '_EQBD_', '_TP_BD_': '_TPBD_'}
SPECIAL_TSO_NAME = ['ENTSO-E']

"""Mapper for elements of the file name to igm profile"""
IGM_FILENAME_MAPPING_TO_OPDM = {PMD_FILENAME_KEYWORD: PMD_FILENAME_KEYWORD,
                                MODEL_SCENARIO_TIME_KEYWORD: PMD_SCENARIO_DATE_KEYWORD,
                                MODEL_PROCESS_TYPE_KEYWORD: PMD_TIME_HORIZON_KEYWORD,
                                MODEL_MODELING_ENTITY_KEYWORD: PMD_MODEL_PART_REFERENCE_KEYWORD,
                                MODEL_MESSAGE_TYPE_KEYWORD: PMD_CGMES_PROFILE_KEYWORD,
                                MODEL_VERSION_KEYWORD: PMD_VERSION_NUMBER_KEYWORD}

"""Mapper for the elements of the file name to boundary profile"""
BOUNDARY_FILENAME_MAPPING_TO_OPDM = {PMD_FILENAME_KEYWORD: PMD_FILENAME_KEYWORD,
                                     MODEL_SCENARIO_TIME_KEYWORD: PMD_SCENARIO_DATE_KEYWORD,
                                     MODEL_MODELING_ENTITY_KEYWORD: PMD_MODEL_PART_REFERENCE_KEYWORD,
                                     MODEL_MESSAGE_TYPE_KEYWORD: PMD_CGMES_PROFILE_KEYWORD,
                                     MODEL_VERSION_KEYWORD: PMD_VERSION_NUMBER_KEYWORD}
SYSTEM_SPECIFIC_FOLDERS = ['__MACOSX']
UNWANTED_FILE_TYPES = ['.xlsx', '.docx', '.pptx']
RECURSION_LIMIT = 2
USE_ROOT = False        # extracts to root, not to folder specified to zip. Note that some zip examples may not work!


class LocalInputType(Enum):
    """
    Enum for different data loads (igm and boundary)
    """
    BOUNDARY = 'boundary',
    IGM = 'igm'
    UNDEFINED = 'undefined'


class LocalFileLoaderError(FileNotFoundError):
    """
    For throwing when errors occur during the process of loading local files
    """
    pass


def validate_model(opdm_objects,
                   loadflow_parameters=CGM_RELAXED_2,
                   run_element_validations=True,
                   send_qas_report=True,
                   report_type="IGM",
                   debugging: bool = False,
                   report_data: dict = None):
    # Load data
    start_time = time.time()
    model_data = load_model(opdm_objects=opdm_objects)
    network = model_data["network"]

    # Run all validations except SHUNTS, that does not work on pypowsybl 0.24.0
    if run_element_validations:
        validations = list(
            set(attr_to_dict(pypowsybl._pypowsybl.ValidationType).keys()) - set(["ALL", "name", "value", "SHUNTS"]))

        model_data["validations"] = {}

        for validation in validations:
            validation_type = getattr(pypowsybl._pypowsybl.ValidationType, validation)
            logger.info(f"Running validation: {validation_type}")
            try:
                # TODO figure out how to store full validation results if needed. Currently only status is taken
                model_data["validations"][validation] = pypowsybl.loadflow.run_validation(network=network,
                                                                                          validation_types=[
                                                                                              validation_type])._valid.__bool__()
            except Exception as error:
                logger.error(f"Failed {validation_type} validation with error: {error}")
                continue

    # Validate if loadflow can be run
    logger.info(f"Solving load flow")
    loadflow_report = pypowsybl.report.Reporter()
    loadflow_result = pypowsybl.loadflow.run_ac(network=network,
                                                parameters=loadflow_parameters,
                                                reporter=loadflow_report)

    # Parsing loadflow results
    # TODO move sanitization to Elastic integration
    loadflow_result_dict = {}
    for island in loadflow_result:
        island_results = attr_to_dict(island)
        island_results['status'] = island_results.get('status').name
        island_results['distributed_active_power'] = 0.0 if math.isnan(island_results['distributed_active_power']) else \
            island_results['distributed_active_power']
        loadflow_result_dict[f"component_{island.connected_component_num}"] = island_results
    model_data["loadflow_results"] = loadflow_result_dict
    # model_data["loadflow_report"] = json.loads(loadflow_report.to_json())
    # model_data["loadflow_report_str"] = str(loadflow_report)

    # Validation status and duration
    # TODO check only main island component 0?
    model_valid = any([True if val["status"] == "CONVERGED" else False for key, val in loadflow_result_dict.items()])
    model_data["valid"] = model_valid
    model_data["validation_duration_s"] = round(time.time() - start_time, 3)
    logger.info(f"Load flow validation status: {model_valid} [duration {model_data['validation_duration_s']}s]")

    # Pop out pypowsybl network object
    model_data.pop('network')

    # Send validation data to Elastic
    try:
        response = elastic.Elastic.send_to_elastic(index=ELK_INDEX, json_message=model_data)
    except Exception as exception_error:
        logger.error(f"Validation report sending to Elastic failed: {exception_error}")

    #for QAS report preparation take model_data and modify to xml for transformation
    if send_qas_report:
        #get correct XLT
        if report_type == "IGM":
            xsl_path = "config/xslt_service/IGM_entsoeQAReport_Level_8.xsl"
        elif report_type == "CGM":
            xsl_path = "config/xslt_service/CGM_entsoeQAReport_Level_8.xsl"
        else:
            logger.error(f"Unknown report type, not able to generate report")

        with open(Path(__file__).parent.parent.parent.joinpath(xsl_path), 'rb') as file:
            xsl_bytes = file.read()

        if not report_data:
            report_data = {'validation': {}, 'metadata': {'profile': {}}}
            report_data['validation'] = model_data
            report_data['metadata']['profile'] = [profile['opdm:Profile'] for profile in opdm_objects[0]['opde:Component']]
            data = [var.pop('DATA') for var in report_data['metadata']['profile']]
        message_data = {"XML": dict2xml(report_data, wrap='report'), "XSL": xsl_bytes}

        try:#publish message to Rabbit to wait for conversion
            # debugging
            if "PYCHARM_HOSTED" in os.environ and debugging:
                logger.info(str(message_data))
            else:
                rabbit_service = rabbit.BlockingClient()
                rabbit_service.publish(str(message_data), RMQ_EXCHANGE)
                logger.info(f"Validation report sending to Rabbit for ..")
        except Exception as error:
            logger.error(f"Validation report sending to Rabbit for {error}")

    return model_data


def send_cgm_qas_report(qas_meta_data: dict, xsl_path: str = CGM_XSL_PATH, exchange_name: str = RMQ_EXCHANGE):
    """
    Reduced version from previous, load flow is run by CgmCompose after merge and necessary fields are also
    gathered by it (CgmCompose.get_data_for_qas()). Therefore compose the report
    :param qas_meta_data: dictionary matching the fields
    :param xsl_path: path to template
    :param exchange_name: name of the exchange where to send the report
    """
    with open(Path(__file__).parent.parent.parent.joinpath(xsl_path), 'rb') as file:
        xsl_bytes = file.read()
    message_data = {"XML": dict2xml(qas_meta_data, wrap='Result'), "XSL": xsl_bytes}
    # TODO send where it is needed
    debugging = False
    try:
        # debugging
        if "PYCHARM_HOSTED" in os.environ and debugging:
            body = xslt30_convert(message_data.get('XML'), message_data.get('XSL'))
            fields = qas_meta_data.get('MergeInformation', {}).get('MetaData', {})
            time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
            scenario_date = parse_datetime(fields.get('scenarioDate')).strftime('%Y%m%dT%H%M%S')
            file_name = (f"./example_reports/qas_{scenario_date}"
                         f"_{fields.get('timeHorizon', '')}"
                         f"_{fields.get('mergingArea', '')}"
                         f"_from_{time_moment_now}.xml")
            check_and_create_the_folder_path(os.path.dirname(file_name))
            with open(file_name, 'wb') as output_file:
                output_file.write(body)
        rabbit_service = rabbit.BlockingClient()
        rabbit_service.publish(str(message_data), exchange_name)
        logger.info(f"CGM QAS report sending to Rabbit for ..")
    except Exception as error:
        logger.error(f"CGM QAS report sending to Rabbit for {error}")


def validate_models(igm_models: list = None, boundary_data: list = None):
    """
    Validates the raw output from the opdm
    :param igm_models: list of igm models
    :param boundary_data: dictionary containing the boundary data
    :return list of validated models
    """
    valid_models = []
    invalid_models = []
    # Validate models
    if not igm_models or not boundary_data:
        logger.error(f"Missing input data")
        return valid_models
    for igm_model in igm_models:

        try:
            validation_response = validate_model([igm_model, boundary_data])
            model[VALIDATION_STATUS_KEYWORD] = validation_response
            if validation_response[VALID_KEYWORD]:
                valid_models.append(igm_model)
            else:
                invalid_models.append(igm_model)
        except:
            invalid_models.append(igm_model)
            logger.error("Validation failed")
    return valid_models


"""-----------------CONTENT RELATED TO LOADING DATA FROM LOCAL STORAGE-----------------------------------------------"""


def read_in_zip_file(zip_file_path: str | BytesIO, file_types: [] = None) -> {}:
    """
    Reads in files from the given zip file
    :param zip_file_path: path to the zip file (relative or absolute)
    :param file_types: list of file types
    :return: dictionary with file names as keys and file contents as values
    """
    content = {}
    with ZipFile(zip_file_path, 'r') as zip_file:
        for file_name in zip_file.namelist():
            if file_types is None or any([file_keyword in file_name for file_keyword in file_types]):
                logger.info(f"Reading {file_name} from {zip_file_path}")
                content[file_name] = zip_file.read(file_name)
    return content


def read_in_xml_file(xml_file_path: str, file_types: [] = None) -> {}:
    """
    Reads in data from the given xml file
    :param xml_file_path: path to the xml file (relative or absolute)
    :param file_types: list of file types
    :return: dictionary with file names as keys and file contents as values
    """
    content = {}
    file_name = os.path.basename(xml_file_path)
    if file_types is None or any([file_keyword in file_name for file_keyword in file_types]):
        logger.info(f"Reading {file_name}")
        with open(xml_file_path, 'r', encoding='utf8') as file_content:
            content[file_name] = file_content.read()
    return content


def save_content_to_zip_file(content: {}):
    """
    Saves content to zip file (in memory)
    :param content: the content of zip file (key: file name, value: file content)
    :return: byte array
    """
    output_object = BytesIO()
    with ZipFile(output_object, "w") as output_zip:
        if content:
            for file_name in content:
                logger.info(f"Converting {file_name} to zip container")
                output_zip.writestr(file_name, content[file_name])
        output_object.seek(0)
    return output_object.getvalue()


def parse_boundary_message_type_profile(message_type_value: str) -> str:
    """
    Slices the 4-letter string to add _ in the middle: 'EQBD' to 'EQ_BD'
    :param message_type_value: input string
    :return: updated string if it was 4 chars long
    """
    if len(message_type_value) == 4:
        return message_type_value[:2] + '_' + message_type_value[2:]
    return message_type_value


def map_meta_dict_to_dict(input_dict: {}, meta_dict: {}, key_dict: {}) -> {}:
    """
    Maps values from meta_dict to input dict based on key value pairs from key_dict
    input_dict[key_dict[key]] = meta_dict[key]
    :param input_dict: input and output dictionary (OPDM profile)
    :param meta_dict: metadata (parameters from file name)
    :param key_dict: mapper, values are keys for input dict, keys are keys for meta dict
    :return: updated input_dict
    """
    if meta_dict != {} and key_dict != {}:
        for key in meta_dict.keys():
            if key in key_dict:
                input_dict[key_dict[key]] = meta_dict[key]
    return input_dict


def get_meta_from_filename(file_name: str):
    """
    Extends the 'get_metadata_from_filename(file_name)' from helper by adding file name to metadata dictionary
    :param file_name: file name to be parsed
    :return: dictionary with metadata
    """
    try:
        fixed_file_name = file_name
        for key in BOUNDARY_FILE_TYPE_FIX:
            if key in fixed_file_name:
                fixed_file_name = fixed_file_name.replace(key, BOUNDARY_FILE_TYPE_FIX[key])
        # meta_data = get_metadata_from_filename(fixed_file_name)
        meta_data = metadata_from_filename(fixed_file_name)
        # Revert back cases where there is a '-' in TSO's name like ENTSO-E
        for case in SPECIAL_TSO_NAME:
            if case in fixed_file_name:
                meta_data[PMD_MODEL_PART_REFERENCE_KEYWORD] = case
                if "-".join([meta_data.get(PMD_MERGING_ENTITY_KEYWORD, ''),
                             meta_data.get(PMD_MERGING_AREA_KEYWORD, '')]) == case:
                    meta_data[PMD_MERGING_ENTITY_KEYWORD] = None
                    meta_data[PMD_MERGING_AREA_KEYWORD] = None
                break
    except ValueError as err:
        logger.warning(f"Unable to parse file name: {err}, trying to salvage")
        meta_data = salvage_data_from_file_name(file_name=file_name)
    meta_data[PMD_FILENAME_KEYWORD] = file_name
    return meta_data


def salvage_data_from_file_name(file_name: str):
    """
    Function to try to extract something from the file name
    param file_name: name of the file as string
    return dictionary with metadata
    """
    meta_data = {}
    for element in IGM_FILE_TYPES:
        if element in file_name:
            meta_data[MODEL_MESSAGE_TYPE_KEYWORD] = element.replace("_", "")
    return meta_data


def load_data(file_name: str | BytesIO, file_types: list = None):
    """
    Loads data from given file.
    :param file_name: file from where to load (with relative or absolute path)
    :param file_types: list of file types
    :return: dictionary with filenames as keys, contents as values, if something was found, none otherwise
    """
    data = None
    if zipfile.is_zipfile(file_name):
        data = read_in_zip_file(file_name, file_types)
    elif file_name.endswith(XML_KEYWORD):
        data = read_in_xml_file(file_name, file_types)
    return data


def get_one_set_of_igms_from_local_storage(file_data: [], tso_name: str = None, file_types: [] = None):
    """
    Loads igm data from local storage.
    :param file_data: list of file names
    :param tso_name: the name of the tso if given
    :param file_types: list of file types
    :return: dictionary that wants to be similar to OPDM profile
    """
    igm_value = {OPDE_COMPONENT_KEYWORD: []}
    if tso_name is not None:
        igm_value[PMD_TSO_KEYWORD] = tso_name
    for file_datum in file_data:
        if (data := load_data(file_datum, file_types)) is None:
            continue
        meta_for_data = {key: get_meta_from_filename(key) for key in data.keys()}
        for datum in data:
            if PMD_TSO_KEYWORD not in igm_value:
                if MODEL_MODELING_ENTITY_KEYWORD in meta_for_data[datum]:
                    igm_value[PMD_TSO_KEYWORD] = meta_for_data[datum][MODEL_MODELING_ENTITY_KEYWORD]
                elif PMD_MODEL_PART_REFERENCE_KEYWORD in meta_for_data[datum]:
                    igm_value[PMD_TSO_KEYWORD] = meta_for_data[datum][PMD_MODEL_PART_REFERENCE_KEYWORD]

            opdm_profile_content = meta_for_data[datum]
            # opdm_profile_content = map_meta_dict_to_dict(input_dict={},
            #                                              meta_dict=meta_for_data[datum],
            #                                              key_dict=IGM_FILENAME_MAPPING_TO_OPDM)
            opdm_profile_content[DATA_KEYWORD] = save_content_to_zip_file({datum: data[datum]})
            igm_value[OPDE_COMPONENT_KEYWORD].append({OPDM_PROFILE_KEYWORD: opdm_profile_content})
    return set_igm_values_from_profiles(igm_value)


def set_igm_values_from_profiles(igm_value: dict):
    """
    Purely for cosmetic purposes only: parse values from file name to opde:Component fields
    :param igm_value: opde component dict created from reading local files
    :return updated igm_value
    """
    scenario_date = None
    time_horizon = None
    model_part_reference = None
    version_number = None
    for component in igm_value.get(OPDE_COMPONENT_KEYWORD):
        try:
            profile = component.get(OPDM_PROFILE_KEYWORD, {})
            new_scenario_date = parse_datetime(profile.get(PMD_VALID_FROM_KEYWORD))
            new_scenario_date_str = new_scenario_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            scenario_date = new_scenario_date_str \
                if not scenario_date or new_scenario_date > parse_datetime(scenario_date) else scenario_date
            time_horizon = time_horizon or profile.get(PMD_TIME_HORIZON_KEYWORD)
            model_part_reference = model_part_reference or profile.get(PMD_MODEL_PART_REFERENCE_KEYWORD)
            version_number = profile.get(PMD_VERSION_NUMBER_KEYWORD) \
                if not version_number or int(profile.get(PMD_VERSION_NUMBER_KEYWORD)) > int(version_number) \
                else version_number
        except Exception:
            continue
    igm_value[PMD_SCENARIO_DATE_KEYWORD] = scenario_date or ''
    igm_value[PMD_TIME_HORIZON_KEYWORD] = time_horizon or ''
    igm_value[PMD_MODEL_PART_REFERENCE_KEYWORD] = model_part_reference or ''
    igm_value[PMD_VERSION_NUMBER_KEYWORD] = version_number or ''
    return igm_value


def get_one_set_of_boundaries_from_local_storage(file_names: [], file_types: [] = None):
    """
    Loads boundary data from local storage.
    :param file_names: list of file names
    :param file_types: list of file types
    :return: dictionary that wants to be similar to OPDM profile
    """
    boundary_value = {OPDE_COMPONENT_KEYWORD: []}
    for file_name in file_names:
        if (data := load_data(file_name, file_types)) is None:
            continue
        meta_for_data = {key: get_meta_from_filename(key) for key in data.keys()}
        for datum in data:
            if MODEL_MESSAGE_TYPE_KEYWORD in meta_for_data:
                meta_for_data[MODEL_MESSAGE_TYPE_KEYWORD] = parse_boundary_message_type_profile(
                    meta_for_data[MODEL_MESSAGE_TYPE_KEYWORD])
            elif PMD_CGMES_PROFILE_KEYWORD in meta_for_data:
                meta_for_data[PMD_CGMES_PROFILE_KEYWORD] = (
                    parse_boundary_message_type_profile(meta_for_data[PMD_CGMES_PROFILE_KEYWORD]))
            opdm_profile_content = meta_for_data[datum]
            # opdm_profile_content = map_meta_dict_to_dict(input_dict={},
            #                                              meta_dict=meta_for_data[datum],
            #                                              key_dict=BOUNDARY_FILENAME_MAPPING_TO_OPDM)
            opdm_profile_content[DATA_KEYWORD] = save_content_to_zip_file({datum: data[datum]})
            boundary_value[OPDE_COMPONENT_KEYWORD].append({OPDM_PROFILE_KEYWORD: opdm_profile_content})
    return boundary_value


def get_zip_file_list_from_dir(path_to_dir: str):
    """
    Lists names of zip files from the given directory
    :param path_to_dir: search directory
    :return: list of file names
    """
    file_list = [join(path_to_dir, file_name)
                 for file_name in listdir(path_to_dir)
                 if zipfile.is_zipfile(join(path_to_dir, file_name))]
    return file_list


def get_xml_file_list_from_dir(path_to_dir: str):
    """
    Lists names of .xml files from the given directory
    :param path_to_dir: search directory
    :return: list of file names
    """
    file_list = [join(path_to_dir, file_name)
                 for file_name in listdir(path_to_dir)
                 if file_name.endswith(XML_KEYWORD)]
    return file_list


def get_list_of_content_files(paths: str | list) -> []:
    """
    Gets list of file names of interest (.zip, .xml) from the given path or paths (list or string)
    :param paths: either directory (get multiple files) or single file name
    :return: list of file names
    """
    path_list = paths
    if isinstance(paths, str):
        path_list = [paths]
    list_of_files = []
    for element in path_list:
        if os.path.isdir(element):
            zip_files = get_zip_file_list_from_dir(element)
            xml_files = get_xml_file_list_from_dir(element)
            list_of_files.extend(zip_files)
            list_of_files.extend(xml_files)
        elif os.path.isfile(element) and (zipfile.is_zipfile(element) or element.endswith(XML_KEYWORD)):
            list_of_files.append(element)
        else:
            logger.error(f"{element} is not a path nor a .xml or a .zip file")
            raise LocalFileLoaderError
    return list_of_files


def get_data_from_files(file_locations: list | str | dict,
                        get_type: LocalInputType = LocalInputType.IGM,
                        file_keywords: list = None):
    """
    Extracts and parses data to necessary profile
    :param file_locations: list of files or their locations, one element per tso
    :param get_type: type of data to be extracted
    :param file_keywords: list of identifiers that are in file names that should be loaded
    :return: dictionary wanting to be similar to opdm profile
    """
    all_models = []
    tso_counter = 1
    if isinstance(file_locations, str):
        file_locations = [file_locations]
    if isinstance(file_locations, dict):
        for element in file_locations:
            file_set = get_list_of_content_files(file_locations[element])
            if get_type is LocalInputType.BOUNDARY:
                all_models.append(get_one_set_of_boundaries_from_local_storage(file_names=file_set,
                                                                               file_types=file_keywords))
            else:
                all_models.append(get_one_set_of_igms_from_local_storage(file_data=file_set,
                                                                         tso_name=element,
                                                                         file_types=file_keywords))
    elif isinstance(file_locations, list):
        for element in file_locations:
            file_set = get_list_of_content_files(element)
            if get_type is LocalInputType.BOUNDARY:
                all_models.append(get_one_set_of_boundaries_from_local_storage(file_names=file_set,
                                                                               file_types=file_keywords))
            else:
                igm_value = get_one_set_of_igms_from_local_storage(file_data=file_set,
                                                                   file_types=file_keywords)
                if PMD_TSO_KEYWORD not in igm_value:
                    tso_name = f"{MISSING_TSO_NAME}-{tso_counter}"
                    tso_counter += 1
                    logger.warning(f"TSO name not found assigning default name as {tso_name}")
                    igm_value[PMD_TSO_KEYWORD] = tso_name
                all_models.append(igm_value)
    else:
        logger.error(f"Unsupported input")
        raise LocalFileLoaderError

    return all_models


def filter_file_list_by_file_keywords(file_list: list | str | dict, file_keywords: list = None):
    """
    Ables to filter the file list by file identifying keywords ('TP', 'SSH', 'EQ', 'SV')
    :param file_list: list of file names
    :param file_keywords: list of file identifiers
    :return updated file list if file_keywords was provided, file_list otherwise
    """
    if file_keywords is None:
        return file_list
    new_file_list = []
    for file_name in file_list:
        if any([file_keyword in file_name for file_keyword in file_keywords]):
            new_file_list.append(file_name)
    return new_file_list


def get_local_igm_data(file_locations: list | str | dict, file_keywords: list = None):
    """
    Call this with a list of files/directories to load igm data
    :param file_locations: list of files or their locations, one element per tso
    :param file_keywords: list of identifiers that are in file names that should be loaded
    :return: dictionary wanting to be similar to opdm profile if something useful was found
    """
    output = get_data_from_files(file_locations=file_locations,
                                 get_type=LocalInputType.IGM,
                                 file_keywords=file_keywords)
    if len(output) == 0:
        logger.error(f"Data for igms were not valid, no igms were extracted")
        raise LocalFileLoaderError
    return output


def get_local_boundary_data(file_locations: list | str, file_keywords: list = None):
    """
    Call this with a list of files/directories to load boundary data
    :param file_locations: list of files or their locations, one element per tso
    :param file_keywords: list of identifiers that are in file names that should be loaded
    :return: dictionary wanting to be similar to opdm profile if something useful was found
    """
    boundaries = get_data_from_files(file_locations=file_locations,
                                     get_type=LocalInputType.BOUNDARY,
                                     file_keywords=file_keywords)
    try:
        return boundaries[0]
    except IndexError:
        logger.error(f"Data for boundaries were not valid, no boundaries were extracted")
        raise LocalFileLoaderError


def get_local_files():
    """
    This is just an example
    Input is a list or dictionary (tso name: path(s) to tso igm files) of elements when there are more than one
    TSO, boundary, otherwise it can be a single string entry.
    For each element in the list of inputs, the value can be single path to directory, zip file, xml file
    or list of them
    Note that inputs are not checked during the loading. For example if element of one TSO contains zip file
    and directory to zip file (something like ['c:/Path_to_zip/', 'c:/Path_to_zip/zip_file.zip'])
    then zip file (zip_file.zip) is read in twice and sent to validator (ending probably with pypowsybl error).
    NB! if tso name is not given (input type is not dictionary), then it is extracted from the name of the first
    file which is processed and which follows the standard described in helper.get_metadata_from_filename()
    NB! Directories and file names used here (./path_to_data/, etc.) are for illustration purposes only.
    To use the local files specify the paths to the data accordingly (absolute or relative path)
    """
    # Addresses can be relative or absolute.
    # 1. load in by directory per tso which contains zip files
    # igm_files = ['./path_to_data/case_1_TSO1_zip_files/',
    #              './path_to_data/case_1_TSO2_zip_files/',
    #              './path_to_data/case_1_TSO3_zip_files/']
    # boundary_file = './path_to_data/case_1_BOUNDARY_zip_files/'
    # 2. load in by zip files per tso
    # igm_files = ['./path_to_data/case_2_combined/TSO1_ZIP_OF_XMLS.zip',
    #              './path_to_data/case_2_combined/TSO2_ZIP_OF_XMLS.zip',
    #              './path_to_data/case_2_combined/TSO3_ZIP_OF_XMLS.zip']
    # boundary_file = './path_to_data/case_2_combined/BOUNDARY_ZIP_OF_XMLS.zip'
    # 3. load in by directory per tso which stores xml files
    # igm_files = ['./path_to_data/case_3_TSO1_xml_files/',
    #              './path_to_data/case_3_TSO2_xml_files/',
    #              './path_to_data/case_3_TSO3_xml_files/']
    # boundary_file = './path_to_data/case_3_BOUNDARY_xml_files/'
    # 4. Load data in as dictionary in form of TSO name: paths
    igm_files = {'TSO1': './path_to_data/case_3_TSO1_xml_files/',
                 'TSO2': './path_to_data/case_3_TSO2_xml_files/',
                 'TSO3': './path_to_data/case_3_TSO3_xml_files/'}
    boundary_file = './path_to_data/case_3_BOUNDARY_xml_files/'
    # Get data and carry on
    models = get_local_igm_data(igm_files, IGM_FILE_TYPES)
    try:
        boundary = get_local_boundary_data(boundary_file, BOUNDARY_FILE_TYPES)
    except NameError:
        boundary = None
    return models, boundary


def check_and_create_the_folder_path(folder_path: str):
    """
    Checks if folder path doesn't have any excessive special characters and it exists. Creates it if it does not
    :param folder_path: input given
    :return checked folder path
    """
    folder_path = check_the_folder_path(folder_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    return folder_path


def download_zip_file(url_to_zip: str, path_to_download: str = None):
    """
    Downloads a zip file from url.
    Note that the file may be rather large so do it in stream
    :param url_to_zip: url of the zip file
    :param path_to_download: location to download the file
    : return loaded_file_name: the path to downloaded zip file
    """
    loaded_file_name = url_to_zip.split('/')[-1]
    if path_to_download is not None:
        path_to_download = check_the_folder_path(path_to_download)
        loaded_file_name = path_to_download + loaded_file_name
    with requests.get(url_to_zip, stream=True) as r:
        with open(loaded_file_name, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
    return loaded_file_name


def check_and_extract_zip_files_in_folder(root_folder: str,
                                          files: [],
                                          depth: int = 1,
                                          use_root: bool = USE_ROOT,
                                          max_depth: int = RECURSION_LIMIT):
    """
    Checks if files in folder are zip files, and extracts them recursively
    :param root_folder: the name of the root folder
    :param files: list of files
    :param depth: current depth of recursion
    :param use_root: use root folder for extraction
    :param max_depth: max allowed recursion depth
    """
    root_folder = check_the_folder_path(root_folder)
    for file_name in files:
        full_file_name = root_folder + file_name
        file_extension = os.path.splitext(full_file_name)[-1]
        xml_file = os.path.splitext(full_file_name)[0] + ".xlm"
        if file_extension == ".xlm" or xml_file in files:
            return
        if zipfile.is_zipfile(full_file_name) and file_extension not in UNWANTED_FILE_TYPES:
            extract_zip_file(current_zip_file=full_file_name,
                             root_folder=root_folder,
                             use_root=use_root,
                             depth=depth + 1,
                             max_depth=max_depth)


def extract_zip_file(current_zip_file: str,
                     root_folder: str = None,
                     use_root: bool = USE_ROOT,
                     depth: int = 1,
                     max_depth: int = RECURSION_LIMIT):
    """
    Extracts content of the zip file to the root.
    :param current_zip_file: zip file to be extracted
    :param root_folder: folder where to extract
    :param use_root: use root folder for extraction
    :param depth: current depth of recursion
    :param max_depth: max allowed recursion depth
    """
    # Stop the recursion before going to deep
    if depth > max_depth:
        return
    if root_folder is None or use_root is False:
        root_folder = os.path.splitext(current_zip_file)[0]
    root_folder = check_the_folder_path(root_folder)
    logger.info(f"Extracting {current_zip_file} to {root_folder}")
    with zipfile.ZipFile(current_zip_file, 'r') as level_one_zip_file:
        # level_one_zip_file.extractall(path=root_folder)
        for info in level_one_zip_file.infolist():
            zip_file_name = info.filename
            try:
                level_one_zip_file.extract(zip_file_name, path=root_folder)
            except FileNotFoundError:
                # Workaround for extracting long file names
                output_path = root_folder + zip_file_name
                check_and_create_the_folder_path(os.path.dirname(output_path))
                output_path_unicode = output_path.encode('unicode_escape').decode()
                file_path = os.path.abspath(os.path.normpath(output_path_unicode))
                file_path = LONG_FILENAME_SUFFIX + file_path
                buffer_size = 16 * 1024
                with level_one_zip_file.open(info) as f_in, open(file_path, 'wb') as f_out:
                    while True:
                        buffer = f_in.read(buffer_size)
                        if not buffer:
                            break
                        f_out.write(buffer)
            except Exception as e:
                logger.error(f"Uncaught exception: {e}")
    os.remove(current_zip_file)
    # Getting relevant paths
    all_elements = [x for x in os.walk(root_folder)]
    for root, folders, files in all_elements:
        # Don't go to system specific folders or generate endless recursion
        if any(root in system_folder for system_folder in SYSTEM_SPECIFIC_FOLDERS) or root == root_folder:
            continue
        check_and_extract_zip_files_in_folder(root_folder=root,
                                              use_root=use_root,
                                              files=files,
                                              depth=depth,
                                              max_depth=max_depth)


def search_directory(root_folder: str, search_path: str):
    """
    Searches the search_path starting from the root_folder. Note that the requested path has to end with the search_path
    :param root_folder: root folder from where to start looking
    :param search_path: the part of the path to search from the root_folder
    :return full path from root_folder to search_path if found, raise exception otherwise
    """
    all_folders = [check_the_folder_path(x[0]) for x in os.walk(root_folder)]
    search_path = check_the_folder_path(search_path)
    matches = [path_name for path_name in all_folders if str(path_name).endswith(search_path)]
    matches_count = len(matches)
    if matches_count == 1:
        return matches[0]
    elif matches_count == 0:
        raise LocalFileLoaderError(f"{search_path} not found in {root_folder}")
    else:
        raise LocalFileLoaderError(f"{search_path} is too broad, found {matches_count} possible matches")


def check_and_get_examples(path_to_search: str,
                           use_root: bool = USE_ROOT,
                           local_folder_for_examples: str = ENTSOE_EXAMPLES_LOCAL,
                           url_for_examples: str = ENTSOE_EXAMPLES_EXTERNAL,
                           recursion_depth: int = RECURSION_LIMIT):
    """
    Checks if examples are present if no then downloads and extracts them
    :param local_folder_for_examples: path to the examples
    :param use_root: use root folder for extraction
    :param url_for_examples: path to online storage
    :param recursion_depth: the max allowed iterations for the recursion
    :param path_to_search: folder to search
    """
    file_name = url_for_examples.split('/')[-1]
    local_folder_for_examples = check_the_folder_path(local_folder_for_examples)
    full_file_name = local_folder_for_examples + file_name
    # Check if folder exists, create it otherwise
    if not os.path.exists(local_folder_for_examples):
        os.makedirs(local_folder_for_examples)
    try:
        # Try to get the directory, catch error if not found
        directory_needed = search_directory(local_folder_for_examples, path_to_search)
        return directory_needed
    except LocalFileLoaderError:
        # Check if directory contains necessary file and it is zip file
        if not os.path.isfile(full_file_name) or not zipfile.is_zipfile(full_file_name):
            # Download the file
            logger.info(f"Downloading examples from {url_for_examples} to {local_folder_for_examples}")
            full_file_name = download_zip_file(url_for_examples, local_folder_for_examples)
        # Now, there should be a zip present, extract it
        extract_zip_file(current_zip_file=full_file_name,
                         root_folder=local_folder_for_examples,
                         use_root=use_root,
                         max_depth=recursion_depth)
    # And try to find the necessary path
    return search_directory(local_folder_for_examples, path_to_search)


def check_if_filename_exists_in_list(existing_file_names: list, new_file_name: str):
    """
    Checks if filename is present in dict, if not then adds it
    Also checks version numbers, replaces the filename if the version number is bigger
    :param existing_file_names: list of existing file names
    :param new_file_name: new file name to be added
    """
    reduced_file_name = os.path.basename(new_file_name)
    file_base = os.path.splitext(reduced_file_name)[0]
    file_base_dict = get_meta_from_filename(reduced_file_name)
    if any([file_base in existing_file_name for existing_file_name in existing_file_names]):
        return existing_file_names
    new_version_number = file_base_dict.get(MODEL_VERSION_KEYWORD) or file_base_dict.get(PMD_VERSION_NUMBER_KEYWORD)
    file_base_reduced = file_base.removesuffix(new_version_number)
    similar = {file_name: get_meta_from_filename(os.path.basename(file_name))
               for file_name in existing_file_names if file_base_reduced in file_name}
    if similar:
        exists = {file: similar[file].get(MODEL_VERSION_KEYWORD) or similar[file].get(PMD_VERSION_NUMBER_KEYWORD)
                  for file in similar}
        max_file = max(exists, key=exists.get)
        if int(exists[max_file]) < int(new_version_number):
            existing_file_names.remove(max_file)
        else:
            return existing_file_names
    existing_file_names.append(new_file_name)
    return existing_file_names


def group_files_by_origin(list_of_files: [], root_folder: str = None, allow_merging_entities: bool = True):
    """
    When input is a directory containing the .xml and .zip files for all the TSOs and boundaries as well and
    if files follow the standard name convention, then this one sorts them by TSOs and by boundaries
    The idea is that one tso can have only one type of file only once (e.g. one tso cannot have two 'TP' files)
    and there is only one list of boundaries
    :param list_of_files: list of files to divide
    :param root_folder: root folder for relative or absolute paths
    :param allow_merging_entities: true: allow cases like TECNET-CE-ELIA to list of models
    :return: dictionaries for containing TSO files, boundary files
    """
    tso_files = {}
    # Take assumption that we have only one boundary
    boundaries = {}
    igm_file_types = [file_type.replace('_', '') for file_type in IGM_FILE_TYPES]
    boundary_file_types = [file_type.strip("_") for file_type in BOUNDARY_FILE_TYPES]
    if root_folder is not None:
        root_folder = check_the_folder_path(root_folder)
    for file_name in list_of_files:
        file_extension = os.path.splitext(file_name)[-1]
        file_base = os.path.splitext(file_name)[0]
        # Check if file is supported file
        if file_extension not in PREFERRED_FILE_TYPES:
            continue
        # Check if file supports standard naming convention, refer to helper.get_metadata_from_filename for more details
        file_name_meta = get_meta_from_filename(file_name)
        if root_folder is not None:
            file_name = root_folder + file_name
        tso_name = (file_name_meta.get(MODEL_MODELING_ENTITY_KEYWORD) or
                    file_name_meta.get(PMD_MODEL_PART_REFERENCE_KEYWORD))
        file_type_name = (file_name_meta.get(MODEL_MESSAGE_TYPE_KEYWORD) or
                          file_name_meta.get(PMD_CGMES_PROFILE_KEYWORD))
        merging_entity = (file_name_meta.get(PMD_MERGING_ENTITY_KEYWORD, '') or
                          file_name_meta.get(MODEL_MERGING_ENTITY_KEYWORD, ''))
        merging_entity = None if merging_entity == '' else merging_entity
        modeling_entity = file_name_meta.get(MODEL_FOR_ENTITY_KEYWORD, '')
        modeling_entity = None if modeling_entity == '' else modeling_entity
        # if needed skip the cases when there is merging entity and part_reference present, didn't like to pypowsybl
        if not allow_merging_entities and tso_name and merging_entity and tso_name not in SPECIAL_TSO_NAME:
            continue
        if not tso_name:
            tso_name = modeling_entity or merging_entity
        if tso_name and file_type_name:
            # Handle TSOs
            if file_type_name in igm_file_types:
                if tso_name not in tso_files.keys():
                    tso_files[tso_name] = []
                # Check if file without the extension is already present
                tso_files[tso_name] = check_if_filename_exists_in_list(existing_file_names=tso_files[tso_name],
                                                                       new_file_name=file_name)
            # Handle boundaries
            elif file_type_name in boundary_file_types:
                if tso_name not in boundaries.keys():
                    boundaries[tso_name] = []
                # Check if file without the extension is already present
                boundaries[tso_name] = check_if_filename_exists_in_list(existing_file_names=boundaries[tso_name],
                                                                        new_file_name=file_name)
            else:
                logger.warning(f"Names follows convention but unable to categorize it: {file_name}")
        else:
            logger.warning(f"Unrecognized file: {file_name}")
    return tso_files, boundaries


def check_model_completeness(model_data: list | dict, file_types: list | str):
    """
    Skips models which do not contain necessary files
    :param model_data: models to be checked
    :param file_types: list of file types to search
    :return updated file list
    """
    checked_models = []
    if isinstance(file_types, str):
        file_types = [file_types]
    if isinstance(model_data, dict):
        model_data = [model_data]
    for model_datum in model_data:
        existing_types = [item[OPDM_PROFILE_KEYWORD][PMD_CGMES_PROFILE_KEYWORD]
                          for item in model_datum[OPDE_COMPONENT_KEYWORD]]
        if all(file_type in existing_types for file_type in file_types):
            checked_models.append(model_datum)
    return checked_models


def get_local_entsoe_files(path_to_directory: str | list,
                           allow_merging_entities: bool = True,
                           igm_files_needed: list = None,
                           boundary_files_needed: list = None):
    """
    Gets list of files in directory and divides them to model and boundary data
    :param path_to_directory: path to directory from where to search
    :param allow_merging_entities: true allow cases like TECNET-CE-ELIA to list of models
    :param igm_files_needed: specify explicitly the file types needed (escape pypowsybl "EQ" missing error)
    :param boundary_files_needed: specify explicitly the file types needed for boundary data
    :return dictionary of tso files and list of boundary data
    """
    if isinstance(path_to_directory, str):
        path_to_directory = [path_to_directory]
    models = []
    all_boundaries = []
    boundary = None
    for single_path in path_to_directory:
        try:
            full_path = check_and_get_examples(single_path)
        except Exception as ex:
            logger.error(f"FATAL ERROR WHEN GETTING FILES: {ex}")
            sys.exit()
        full_path = check_the_folder_path(full_path)
        file_names = next(os.walk(full_path), (None, None, []))[2]
        models_data, boundary_data = group_files_by_origin(list_of_files=file_names,
                                                           root_folder=full_path,
                                                           allow_merging_entities=allow_merging_entities)
        if models_data:
            models_transformed = get_local_igm_data(models_data, IGM_FILE_TYPES)
            models.extend(models_transformed)
        if boundary_data:
            try:
                boundary_transformed = get_local_boundary_data(boundary_data, BOUNDARY_FILE_TYPES)
            except NameError:
                boundary_transformed = None
            if boundary_transformed is not None:
                all_boundaries.append(boundary_transformed)
    if len(all_boundaries) == 0:
        logger.warning(f"No boundaries found")
    else:
        if len(all_boundaries) > 1:
            logger.warning(f"Multiple boundaries detected, taking first occurrence")
        boundary = all_boundaries[0]
    if igm_files_needed is not None:
        models = check_model_completeness(models, igm_files_needed)
    if boundary_files_needed is not None:
        boundary = check_model_completeness(boundary, boundary_files_needed)
    return models, boundary


"""-----------------END OF CONTENT RELATED TO LOADING DATA FROM LOCAL STORAGE----------------------------------------"""

# TEST
if __name__ == "__main__":

    import sys
    from emf.common.integrations.opdm import OPDM

    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    # logging.getLogger('powsybl').setLevel(1)
    # Add a pypowsybl log gatherer
    # Set up the log gatherer:
    # topic name: currently used as a start of a file name
    # send_it_to_elastic: send the triggered log entry to elastic (parameters are defined in custom_logger.properties)
    # upload_to_minio: upload log file to minio (parameters are defined in custom_logger.properties)
    # report_on_command: trigger reporting explicitly
    # logging policy: choose according to the need. Currently:
    #   ALL_ENTRIES: gathers all log entries no matter of what
    #   ENTRIES_IF_LEVEL_REACHED: gathers all log entries when at least one entry was at least on the level specified
    #   ENTRY_ON_LEVEL: gathers only entry which was at least on the level specified
    #   ENTRIES_ON_LEVEL: gathers all entries that were at least on the level specified
    #   ENTRIES_COLLECTED_TO_LEVEL: gathers all entries to the first entry that was at least on the level specified
    # print_to_console: propagate log to parent
    # reporting_level: level that triggers policy
    pypowsybl_log_gatherer = PyPowsyblLogGatherer(topic_name='IGM_validation',
                                                  send_to_elastic=False,
                                                  upload_to_minio=False,
                                                  report_on_command=False,
                                                  logging_policy=PyPowsyblLogReportingPolicy.ENTRIES_IF_LEVEL_REACHED,
                                                  print_to_console=False,
                                                  reporting_level=logging.ERROR)

    # Switch this to True if files from local storage are used
    load_data_from_local_storage = True
    try:
        if load_data_from_local_storage:
            # available_models, latest_boundary = get_local_files()
            # Change this according the test case to be used. Note that it must reference to the end folder that will
            # be used. Also it must be unique enough do be distinguished from other folders (for example instead of
            # using 'Combinations' use 'TC1_T11_NonConform_L1/Combinations' etc)
            # Some examples for
            #   https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/QoCDC_v3.2.1_test_models.zip
            folder_to_study = 'TC3_T1_Conform'
            # folder_to_study = 'TC3_T3_Conform'
            # folder_to_study = 'TC4_T1_Conform/Initial'
            # Some examples for
            #   https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/TestConfigurations_packageCASv2.0.zip
            # folder_to_study = ['CGMES_v2.4.15_MicroGridTestConfiguration_T1_BE_Complete_v2',
            #                    'CGMES_v2.4.15_MicroGridTestConfiguration_T1_NL_Complete_v2',
            #                    'Type1_T1/CGMES_v2.4.15_MicroGridTestConfiguration_BD_v2']
            # In general this function checks if the paths (path_to_directory) exist in ENTSOE_EXAMPLES_LOCAL,
            # if not then it tries to download and extract zip from ENTSOE_EXAMPLES_EXTERNAL. If this fails or path
            # is not still found it carries on as usual.
            # Note that zip can be downloaded and extracted but in this case it must be extracted to the path
            # path_to_directory: string or list, end of the path from where to load the files
            # (starting from ENTSOE_EXAMPLES_LOCAL). Note that these must be unique enough (errors are thrown when
            # two or more paths are found)
            # allow_merging_entities: Whether to allow merging entities, pypowsybl validation was not happy about that
            # igm_files_needed: in order for pypowsybl validation to work, atleast these files should be present in
            # igm
            available_models, latest_boundary = get_local_entsoe_files(path_to_directory=folder_to_study,
                                                                       allow_merging_entities=False,
                                                                       igm_files_needed=['EQ'])
        else:
            raise LocalFileLoaderError
    except FileNotFoundError:
        # if needed catch and handle LocalFileLoaderError separately
        logger.info(f"Fetching data from external resources")
        opdm = OPDM()
        latest_boundary = opdm.get_latest_boundary()
        available_models = opdm.get_latest_models_and_download(time_horizon='ID',
                                                               scenario_date='2024-04-05T22:30',
                                                               # tso='ELERING'
                                                               )
        # available_models = opdm.get_latest_models_and_download(time_horizon='1D',
        #                                                        scenario_date='2024-03-14T09:30',
        #                                                        # tso='ELERING'
        #                                                        )

    validated_models = []
    # Validate models
    for model in available_models:
        tso = model['pmd:TSO']
        pypowsybl_log_gatherer.set_tso(tso)
        try:
            if isinstance(latest_boundary, dict):
                response = validate_model([model, latest_boundary], debugging=True)
            else:
                response = validate_model([model])
            model[VALIDATION_STATUS_KEYWORD] = response
            # Example for manual triggering for posting the logs. The value given must be positive:
            log_post_trigger = model.get(VALIDATION_STATUS_KEYWORD, {}).get('valid') is False
            # Note that this switch is governed by report_on_command in PyPowsyblLogGatherer initialization
            pypowsybl_log_gatherer.trigger_to_report_externally(log_post_trigger)
            validated_models.append(model)
        except Exception as error:
            validated_models.append(model)
            logger.error(f"For {model.get('pmd:TSO')} validation failed", error)
    pypowsybl_log_gatherer.stop_working()
    # Print validation statuses
    [print(dict(tso=model['pmd:TSO'], valid=model.get('VALIDATION_STATUS', {}).get('valid'),
                duration=model.get('VALIDATION_STATUS', {}).get('validation_duration_s'))) for model in
     validated_models]

    # With EMF IGM Validation settings
    # {'tso': '50Hertz', 'valid': True, 'duration': 6.954386234283447}
    # {'tso': 'D7', 'valid': None, 'duration': None}
    # {'tso': 'ELERING', 'valid': True, 'duration': 2.1578593254089355}
    # {'tso': 'ELES', 'valid': False, 'duration': 1.6410691738128662}
    # {'tso': 'ELIA', 'valid': True, 'duration': 5.016804456710815}
    # {'tso': 'REE', 'valid': None, 'duration': None}
    # {'tso': 'SEPS', 'valid': None, 'duration': None}
    # {'tso': 'TTG', 'valid': True, 'duration': 5.204774856567383}
    # {'tso': 'PSE', 'valid': True, 'duration': 1.555201530456543}
