import pypowsybl
import os.path
import zipfile
from enum import Enum
from io import BytesIO
from os import listdir
from os.path import join
from zipfile import ZipFile

import logging
import json
import time
import math
import config
from emf.loadflow_tool.loadflow_settings import *
from emf.loadflow_tool.helper import attr_to_dict, load_model, get_metadata_from_filename
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic

# Initialize custom logger
# custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.validator)

# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue

TSO_KEYWORD = 'pmd:TSO'
DATA_KEYWORD = 'DATA'
FILENAME_KEYWORD = 'pmd:fileName'
MODEL_MESSAGE_TYPE = 'Model.messageType'
MAGIC_XML_IDENTIFICATION = '.xml'
MODELING_ENTITY = 'Model.modelingEntity'
OP_COMPONENT_KEYWORD = 'opde:Component'
OP_PROFILE_KEYWORD = 'opdm:Profile'

"""Mapper for elements of the file name to igm profile"""
IGM_FILENAME_MAPPING_TO_OPDM = {FILENAME_KEYWORD: FILENAME_KEYWORD,
                                'Model.scenarioTime': 'pmd:scenarioDate',
                                'Model.processType': 'pmd:timeHorizon',
                                'Model.modelingEntity': 'pmd:modelPartReference',
                                MODEL_MESSAGE_TYPE: 'pmd:cgmesProfile',
                                'Model.version': 'pmd:versionNumber'}

"""Mapper for the elements of the file name to boundary profile"""
BOUNDARY_FILENAME_MAPPING_TO_OPDM = {FILENAME_KEYWORD: FILENAME_KEYWORD,
                                     'Model.scenarioTime': 'pmd:scenarioDate',
                                     'Model.modelingEntity': 'pmd:modelPartReference',
                                     MODEL_MESSAGE_TYPE: 'pmd:cgmesProfile',
                                     'Model.version': 'pmd:versionNumber'}


class LocalInputType(Enum):
    """
    Enum for different data loads (igm and boundary)
    """
    BOUNDARY = 'boundary',
    IGM = 'igm'
    UNDEFINED = 'undefined'
    

def validate_model(opdm_objects, loadflow_parameters=CGM_RELAXED_2, run_element_validations=True):
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
                model_data["validations"][validation] = pypowsybl.loadflow.run_validation(network=network, validation_types=[validation_type])._valid.__bool__()
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
        island_results['distributed_active_power'] = 0.0 if math.isnan(island_results['distributed_active_power']) else island_results['distributed_active_power']
        loadflow_result_dict[f"component_{island.connected_component_num}"] = island_results
    model_data["loadflow_results"] = loadflow_result_dict
    # model_data["loadflow_report"] = json.loads(loadflow_report.to_json())
    # model_data["loadflow_report_str"] = str(loadflow_report)

    # Validation status and duration
    # TODO check only main island component 0?
    model_valid = any([True if val["status"] == "CONVERGED" else False for key, val in loadflow_result_dict.items()])
    model_data["valid"] = model_valid
    model_data["validation_duration_s"] = time.time() - start_time

    # Pop out pypowsybl network object
    model_data.pop('network')

    # Send validation data to Elastic
    try:
        response = elastic.Elastic.send_to_elastic(index=ELK_INDEX, json_message=model_data)
    except Exception as error:
        logger.error(f"Validation report sending to Elastic failed: {error}")

    return model_data


"""-----------------CONTENT RELATED TO LOADING DATA FROM LOCAL STORAGE-----------------------------------------------"""


def read_in_zip_file(zip_file_path: str) -> {}:
    """
    Reads in files from the given zip file
    :param zip_file_path: path to the zip file (relative or absolute)
    :return: dictionary with file names as keys and file contents as values
    """
    content = {}
    with ZipFile(zip_file_path, 'r') as zip_file:
        for name in zip_file.namelist():
            logger.info(f"Reading {name} from {zip_file_path}")
            content[name] = zip_file.read(name)
    return content


def read_in_xml_file(xml_file_path: str) -> {}:
    """
    Reads in data from the given xml file
    :param xml_file_path: path to the xml file (relative or absolute)
    :return: dictionary with file names as keys and file contents as values
    """
    content = {}
    file_name = os.path.basename(xml_file_path)
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
    meta_data = get_metadata_from_filename(file_name)
    meta_data[FILENAME_KEYWORD] = file_name
    return meta_data


def load_data(file_name: str):
    """
    Loads data from given file.
    :param file_name: file from where to load (with relative or absolute path)
    :return: dictionary with filenames as keys, contents as values, if something was found, none otherwise
    """
    data = None
    if zipfile.is_zipfile(file_name):
        data = read_in_zip_file(file_name)
    elif file_name.endswith(MAGIC_XML_IDENTIFICATION):
        data = read_in_xml_file(file_name)
    return data


def get_one_set_of_igms_from_local_storage(file_names: []):
    """
    Loads igm data from local storage.
    :param file_names: list of file names
    :return: dictionary that wants to be similar to OPDM profile
    """
    igm_value = {OP_COMPONENT_KEYWORD: []}
    for file_name in file_names:
        if (data := load_data(file_name)) is None:
            continue
        meta_for_data = {key: get_meta_from_filename(key) for key in data.keys()}
        for datum in data:
            if MODELING_ENTITY in meta_for_data[datum] and TSO_KEYWORD not in igm_value:
                igm_value[TSO_KEYWORD] = meta_for_data[datum][MODELING_ENTITY]
            opdm_profile_content = map_meta_dict_to_dict(input_dict={},
                                                         meta_dict=meta_for_data[datum],
                                                         key_dict=IGM_FILENAME_MAPPING_TO_OPDM)
            opdm_profile_content[DATA_KEYWORD] = save_content_to_zip_file({datum: data[datum]})
            igm_value[OP_COMPONENT_KEYWORD].append({OP_PROFILE_KEYWORD: opdm_profile_content})
    return igm_value


def get_one_set_of_boundaries_from_local_storage(file_names: []):
    """
    Loads boundary data from local storage.
    :param file_names: list of file names
    :return: dictionary that wants to be similar to OPDM profile
    """
    boundary_value = {OP_COMPONENT_KEYWORD: []}
    for file_name in file_names:
        if (data := load_data(file_name)) is None:
            continue
        meta_for_data = {key: get_meta_from_filename(key) for key in data.keys()}
        for datum in data:
            if MODEL_MESSAGE_TYPE in meta_for_data:
                meta_for_data[MODEL_MESSAGE_TYPE] = parse_boundary_message_type_profile(
                    meta_for_data[MODEL_MESSAGE_TYPE])
            opdm_profile_content = map_meta_dict_to_dict(input_dict={},
                                                         meta_dict=meta_for_data[datum],
                                                         key_dict=BOUNDARY_FILENAME_MAPPING_TO_OPDM)
            opdm_profile_content[DATA_KEYWORD] = save_content_to_zip_file({datum: data[datum]})
            boundary_value[OP_COMPONENT_KEYWORD].append({OP_PROFILE_KEYWORD: opdm_profile_content})
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
                 if file_name.endswith(MAGIC_XML_IDENTIFICATION)]
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
            total_files = []
            total_files.extend(zip_files)
            total_files.extend(xml_files)
            list_of_files.append(total_files)
        elif os.path.isfile(element) and (zipfile.is_zipfile(element) or element.endswith(MAGIC_XML_IDENTIFICATION)):
            list_of_files.append([element])
        else:
            logger.error(f"{element} is not a path nor .xml or .zip file")
    return list_of_files


def get_data_from_files(file_locations: list | str, get_type: LocalInputType = LocalInputType.IGM):
    """
    Extracts and parses data to necessary profile
    :param file_locations: list of files or their locations, one element per tso
    :param get_type: type of data to be extracted
    :return: dictionary wanting to be similar to opdm profile
    """
    file_list = get_list_of_content_files(file_locations)
    all_models = []
    for file_set in file_list:
        if get_type is LocalInputType.BOUNDARY:
            all_models.append(get_one_set_of_boundaries_from_local_storage(file_set))
        else:
            all_models.append(get_one_set_of_igms_from_local_storage(file_set))
    return all_models


def get_local_igm_data(file_locations: list | str):
    """
    Call this with a list of files/directories to load igm data
    :param file_locations: list of files or their locations, one element per tso
    :return: dictionary wanting to be similar to opdm profile if something useful was found
    """
    output = get_data_from_files(file_locations=file_locations, get_type=LocalInputType.IGM)
    if len(output) == 0:
        logger.error(f"Data for igms were not valid, no igms were extracted")
    return output


def get_local_boundary_data(file_locations: list | str):
    """
    Call this with a list of files/directories to load boundary data
    :param file_locations: list of files or their locations, one element per tso
    :return: dictionary wanting to be similar to opdm profile if something useful was found
    """
    boundaries = get_data_from_files(file_locations=file_locations, get_type=LocalInputType.BOUNDARY)
    try:
        return boundaries[0]
    except IndexError:
        logger.error(f"Data for boundaries were not valid, no boundaries were extracted")


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
    #logging.getLogger('powsybl').setLevel(1)

    opdm = OPDM()

    load_data_from_local_storage = False

    if load_data_from_local_storage:
        # # 1. load in by directory per tso which contains zip files
        # list_of_igm_locations = ['./test_local_packages/case_1_TSO1_zip_files/',
        #                          './test_local_packages/case_1_TSO2_zip_files/',
        #                          './test_local_packages/case_1_TSO3_zip_files/']
        # boundary_location = './test_local_packages/case_1_BOUNDARY_zip_files/'
        # 2. load in by zip files per tso
        # list_of_igm_locations = ['./test_local_packages/case_2_combined/TSO1_ZIP_OF_XMLS.zip',
        #                          './test_local_packages/case_2_combined/TSO2_ZIP_OF_XMLS.zip',
        #                          './test_local_packages/case_2_combined/TSO3_ZIP_OF_XMLS.zip']
        # boundary_location = './test_local_packages/case_2_combined/BOUNDARY_ZIP_OF_XMLS.zip'
        # # 3. load in by directory per tso which stores xml files
        list_of_igm_locations = ['./test_local_packages/case_3_TSO1_xml_files/',
                                 './test_local_packages/case_3_TSO2_xml_files/',
                                 './test_local_packages/case_3_TSO3_xml_files/']
        boundary_location = './test_local_packages/case_3_BOUNDARY_xml_files/'
        # Get data and carry on
        available_models = get_local_igm_data(list_of_igm_locations)
        latest_boundary = get_local_boundary_data(boundary_location)
    else:
        latest_boundary = opdm.get_latest_boundary()
        available_models = opdm.get_latest_models_and_download(time_horizon='1D',
                                                               scenario_date='2024-02-02T00:30',
                                                               # tso='ELERING'
                                                               )

    validated_models = []


    # Validate models
    for model in available_models:

        try:
            response = validate_model([model, latest_boundary])
            model["VALIDATION_STATUS"] = response
            validated_models.append(model)

        except Exception as error:
            validated_models.append(model)
            #logger.error("Validation failed", error)

    # Print validation statuses
    [print(dict(tso=model['pmd:TSO'], valid=model.get('VALIDATION_STATUS', {}).get('VALID'), duration=model.get('VALIDATION_STATUS', {}).get('VALIDATION_DURATION_S'))) for model in validated_models]

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
