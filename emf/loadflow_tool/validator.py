import os.path
import shutil
import zipfile
from enum import Enum
from io import BytesIO
from os import listdir
from os.path import join
from zipfile import ZipFile

import logging
import time
import math

import requests

import config
from emf.common.logging.custom_logger import PyPowsyblLogGatherer, PyPowsyblLogReportingPolicy, SEPARATOR_SYMBOL
from emf.loadflow_tool.loadflow_settings import *
from emf.loadflow_tool.helper import attr_to_dict, load_model, get_metadata_from_filename
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic

# Initialize custom logger
# custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.validator)

# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue

ENTSOE_FOLDER = './path_to_ENTSOE_zip/TestConfigurations_packageCASv2.0'

TSO_KEYWORD = 'pmd:TSO'
DATA_KEYWORD = 'DATA'
FILENAME_KEYWORD = 'pmd:fileName'
MODEL_MESSAGE_TYPE = 'Model.messageType'
XML_KEYWORD = '.xml'
ZIP_KEYWORD = '.zip'
MODELING_ENTITY = 'Model.modelingEntity'
OP_COMPONENT_KEYWORD = 'opde:Component'
OP_PROFILE_KEYWORD = 'opdm:Profile'
MISSING_TSO_NAME = 'UnknownTSO'

PREFERRED_FILE_TYPES = [XML_KEYWORD, ZIP_KEYWORD]
IGM_FILE_TYPES = ['_EQ_', '_TP_', '_SV_', '_SSH_']
BOUNDARY_FILE_TYPES = ['_EQBD_', '_TPBD_', ]

"""Mapper for elements of the file name to igm profile"""
IGM_FILENAME_MAPPING_TO_OPDM = {FILENAME_KEYWORD: FILENAME_KEYWORD,
                                'Model.scenarioTime': 'pmd:scenarioDate',
                                'Model.processType': 'pmd:timeHorizon',
                                MODELING_ENTITY: 'pmd:modelPartReference',
                                MODEL_MESSAGE_TYPE: 'pmd:cgmesProfile',
                                'Model.version': 'pmd:versionNumber'}

"""Mapper for the elements of the file name to boundary profile"""
BOUNDARY_FILENAME_MAPPING_TO_OPDM = {FILENAME_KEYWORD: FILENAME_KEYWORD,
                                     'Model.scenarioTime': 'pmd:scenarioDate',
                                     MODELING_ENTITY: 'pmd:modelPartReference',
                                     MODEL_MESSAGE_TYPE: 'pmd:cgmesProfile',
                                     'Model.version': 'pmd:versionNumber'}
SYSTEM_SPECIFIC_FOLDERS = ['__MACOSX']
UNWANTED_FILE_TYPES = ['.xlsx', '.docx', '.pptx']
WINDOWS_SEPARATOR = '\\'
RECURSION_LIMIT = 2
UNUSED_FIELDS = ["Model.domain", "Model.forEntity"]


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


def read_in_zip_file(zip_file_path: str, file_types: [] = None) -> {}:
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
        meta_data = get_metadata_from_filename(file_name)
    except ValueError as err:
        logger.warning(f"Unable to parse file name: {err}, trying to salvage")
        meta_data = salvage_data_from_file_name(file_name=file_name)
    meta_data[FILENAME_KEYWORD] = file_name
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
            meta_data["Model.messageType"] = element.replace("_", "")
    return meta_data


def load_data(file_name: str, file_types: list = None):
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


def get_one_set_of_igms_from_local_storage(file_names: [], tso_name: str = None, file_types: [] = None):
    """
    Loads igm data from local storage.
    :param file_names: list of file names
    :param tso_name: the name of the tso if given
    :param file_types: list of file types
    :return: dictionary that wants to be similar to OPDM profile
    """
    igm_value = {OP_COMPONENT_KEYWORD: []}
    if tso_name is not None:
        igm_value[TSO_KEYWORD] = tso_name
    for file_name in file_names:
        if (data := load_data(file_name, file_types)) is None:
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


def get_one_set_of_boundaries_from_local_storage(file_names: [], file_types: [] = None):
    """
    Loads boundary data from local storage.
    :param file_names: list of file names
    :param file_types: list of file types
    :return: dictionary that wants to be similar to OPDM profile
    """
    boundary_value = {OP_COMPONENT_KEYWORD: []}
    for file_name in file_names:
        if (data := load_data(file_name, file_types)) is None:
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
                all_models.append(get_one_set_of_igms_from_local_storage(file_names=file_set,
                                                                         tso_name=element,
                                                                         file_types=file_keywords))
    elif isinstance(file_locations, list):
        for element in file_locations:
            file_set = get_list_of_content_files(element)
            if get_type is LocalInputType.BOUNDARY:
                all_models.append(get_one_set_of_boundaries_from_local_storage(file_names=file_set,
                                                                               file_types=file_keywords))
            else:
                igm_value = get_one_set_of_igms_from_local_storage(file_names=file_set,
                                                                   file_types=file_keywords)
                if TSO_KEYWORD not in igm_value:
                    tso_name = f"{MISSING_TSO_NAME}-{tso_counter}"
                    tso_counter += 1
                    logger.warning(f"TSO name not found assigning default name as {tso_name}")
                    igm_value[TSO_KEYWORD] = tso_name
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


def check_the_folder_path(folder_path: str):
    """
    Checks folder path for special characters
    :param folder_path: input given
    :return checked folder path
    """
    if not folder_path.endswith(SEPARATOR_SYMBOL):
        folder_path = folder_path + SEPARATOR_SYMBOL
    double_separator = SEPARATOR_SYMBOL + SEPARATOR_SYMBOL
    # Escape '//'
    folder_path = folder_path.replace(double_separator, SEPARATOR_SYMBOL)
    # Escape '\'
    folder_path = folder_path.replace(WINDOWS_SEPARATOR, SEPARATOR_SYMBOL)
    return folder_path


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
                                          depth: int = 0,
                                          max_depth: int = RECURSION_LIMIT):
    """
    Checks if files in folder are zip files, and extracts them recursively
    :param root_folder: the name of the root folder
    :param files: list of files
    :param depth: current depth of recursion
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
                             depth=depth + 1,
                             max_depth=max_depth)


def extract_zip_file(current_zip_file: str, root_folder: str, depth: int = 0, max_depth: int = RECURSION_LIMIT):
    """
    Extracts content of the zip file to the root.
    :param current_zip_file: zip file to be extracted
    :param root_folder: folder where to extract
    :param depth: current depth of recursion
    :param max_depth: max allowed recursion depth
    """
    # Stop the recursion before going to deep
    if depth > max_depth:
        return
    root_folder = check_the_folder_path(root_folder)
    logger.info(f"Extracting {current_zip_file} to {root_folder}")
    with zipfile.ZipFile(current_zip_file, 'r') as level_one_zip_file:
        level_one_zip_file.extractall(path=root_folder)
    os.remove(current_zip_file)
    # Getting relevant paths
    all_elements = [x for x in os.walk(root_folder)]
    for root, folders, files in all_elements:
        # Don't go to system specific folders or generate endless recursion
        if any(root in system_folder for system_folder in SYSTEM_SPECIFIC_FOLDERS) or root == root_folder:
            continue
        check_and_extract_zip_files_in_folder(root_folder=root, files=files, depth=depth + 1, max_depth=max_depth)


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
                           local_folder_for_examples: str = ENTSOE_EXAMPLES_LOCAL,
                           url_for_examples: str = ENTSOE_EXAMPLES_EXTERNAL,
                           recursion_depth: int = RECURSION_LIMIT):
    """
    Checks if examples are present if no then downloads and extracts them
    :param local_folder_for_examples: path to the examples
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
                         max_depth=recursion_depth)
    # And try to find the necessary path
    return search_directory(local_folder_for_examples, path_to_search)


def group_files_by_origin(list_of_files: [], root_folder: str = None):
    """
    When input is a directory containing the .xml and .zip files for all the TSOs and boundaries as well and
    if files follow the standard name convention, then this one sorts them by TSOs and by boundaries
    The idea is that one tso can have only one type of file only once (e.g. one tso cannot have two 'TP' files)
    and there is only one list of boundaries
    :param list_of_files: list of files to divide
    :param root_folder: root folder for relative or absolute paths
    :return: dictionaries for containing TSO files, boundary files
    """
    tso_files = {}
    # Take assumption that we have only one boundary
    boundaries = {}
    igm_file_types = [file_type.replace('_', '') for file_type in IGM_FILE_TYPES]
    boundary_file_types = [file_type.replace('_', '') for file_type in BOUNDARY_FILE_TYPES]
    if root_folder is not None:
        root_folder = check_the_folder_path(root_folder)
    for file_name in list_of_files:
        file_extension = os.path.splitext(file_name)[-1]
        file_base = os.path.splitext(file_name)[0]
        # Check if file is supported file
        if file_extension not in ['.xml', '.zip']:
            continue
        # Check if file supports standard naming convention, refer to helper.get_metadata_from_filename for more details
        file_name_meta = get_meta_from_filename(file_name)
        if root_folder is not None:
            file_name = root_folder + file_name
        if MODELING_ENTITY in file_name_meta.keys() and MODEL_MESSAGE_TYPE in file_name_meta.keys():
            tso_name = file_name_meta[MODELING_ENTITY]
            file_type_name = file_name_meta[MODEL_MESSAGE_TYPE]
            # Handle TSOs
            if file_type_name in igm_file_types:
                if tso_name not in tso_files.keys():
                    tso_files[tso_name] = []
                # Check if file without the extension is already present
                if not any(file_base in file_listed for file_listed in tso_files[tso_name]):
                    tso_files[tso_name].append(file_name)
            # Handle boundaries
            elif file_type_name in boundary_file_types:
                if tso_name not in boundaries.keys():
                    boundaries[tso_name] = []
                # Check if file without the extension is already present
                if not any(file_base in file_listed for file_listed in boundaries[tso_name]):
                    boundaries[tso_name].append(file_name)
            else:
                logger.warning(f"Names follows convention but unable to categorize it: {file_name}")
        else:
            logger.warning(f"Unrecognized file: {file_name}")
    return tso_files, boundaries


def get_local_entsoe_files(path_to_directory: str):
    """
    Gets list of files in directory and divides them to model and boundary data
    :param path_to_directory: path to directory from where to search
    :return dictionary of tso files and list of boundary data
    """
    try:
        full_path = check_and_get_examples(path_to_directory)
    except Exception as ex:
        logger.error(f"FATAL ERROR WHEN GETTING FILES: {ex}")
        sys.exit()
    full_path = check_the_folder_path(full_path)
    file_names = next(os.walk(full_path), (None, None, []))[2]
    models_data, boundary_data = group_files_by_origin(file_names, full_path)
    models = get_local_igm_data(models_data, IGM_FILE_TYPES)
    try:
        boundary = get_local_boundary_data(boundary_data, BOUNDARY_FILE_TYPES)
    except NameError:
        boundary = None
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
    # send_it_to_elastic: send the log to elastic, not operational yet
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
                                                  logging_policy=PyPowsyblLogReportingPolicy.ALL_ENTRIES,
                                                  print_to_console=False,
                                                  reporting_level=logging.ERROR)

    # Switch this to True if files from local storage are used
    load_data_from_local_storage = False
    try:
        if load_data_from_local_storage:
            # available_models, latest_boundary = get_local_files()
            # Change this according the test case to be used. Note that it must reference to the end folder that will
            # be used. Also it must be unique enough do be distinguished from other folders (for example instead of
            # using 'Combinations' use 'TC1_T11_NonConform_L1/Combinations' etc)
            folder_to_study = 'TC3_T3_Conform'
            available_models, latest_boundary = get_local_entsoe_files(folder_to_study)
        else:
            raise LocalFileLoaderError
    except FileNotFoundError:
        # if needed catch and handle LocalFileLoaderError separately
        logger.info(f"Fetching data from external resources")
        opdm = OPDM()
        latest_boundary = opdm.get_latest_boundary()
        available_models = opdm.get_latest_models_and_download(time_horizon='1D',
                                                               scenario_date='2024-03-14T09:30',
                                                               # tso='ELERING'
                                                               )

    validated_models = []

    # Validate models
    for model in available_models:
        tso = model['pmd:TSO']
        pypowsybl_log_gatherer.set_tso(tso)
        try:
            if isinstance(latest_boundary, dict):
                response = validate_model([model, latest_boundary])
            else:
                response = validate_model([model])
            model["VALIDATION_STATUS"] = response
            # Example for manual triggering for posting the logs. The value given must be positive:
            log_post_trigger = model.get('VALIDATION_STATUS', {}).get('valid') is False
            # Note that this switch is governed by report_on_command in PyPowsyblLogGatherer initialization
            pypowsybl_log_gatherer.trigger_to_report_externally(log_post_trigger)
            validated_models.append(model)

        except Exception as error:
            validated_models.append(model)
            logger.error("Validation failed", error)
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
