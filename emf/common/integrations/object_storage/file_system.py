import logging
import os
import uuid
import zipfile
from enum import Enum
from io import BytesIO
from os import listdir
from os.path import join
from pathlib import Path
from zipfile import ZipFile

import requests
from aniso8601 import parse_datetime

import config
from emf.common.config_parser import parse_app_properties
from emf.common.logging.pypowsybl_logger import get_pypowsybl_log_handler
from emf.common.integrations.object_storage.file_system_general import OPDE_COMPONENT_KEYWORD, OPDM_PROFILE_KEYWORD, \
    DATA_KEYWORD, PMD_FILENAME_KEYWORD, PMD_CGMES_PROFILE_KEYWORD, PMD_MODEL_PART_REFERENCE_KEYWORD, \
    PMD_MERGING_ENTITY_KEYWORD, PMD_SCENARIO_DATE_KEYWORD, OPDE_OBJECT_TYPE_KEYWORD, PMD_TSO_KEYWORD, \
    PMD_VERSION_NUMBER_KEYWORD, PMD_TIME_HORIZON_KEYWORD, BOUNDARY_OBJECT_TYPE, IGM_OBJECT_TYPE, \
    MODEL_MESSAGE_TYPE_KEYWORD, MODEL_MODELING_ENTITY_KEYWORD, MODEL_MERGING_ENTITY_KEYWORD, \
    MODEL_VERSION_KEYWORD, IGM_FILE_TYPES, SPECIAL_TSO_NAME, PMD_CONTENT_REFERENCE_KEYWORD, \
    VALIDATION_STATUS_KEYWORD, check_and_create_the_folder_path, get_meta_from_filename, check_the_folder_path
from emf.loadflow_tool.helper import generate_OPDM_ContentReference_from_filename
from emf.loadflow_tool.model_validator.validator import validate_model

# from emf.loadflow_tool.model_validator.validator import validate_model

LOCAL_STORAGE_LOCATION = './cgm_merge'
PMD_VALID_FROM_KEYWORD = 'pmd:validFrom'
MODEL_FOR_ENTITY_KEYWORD = 'Model.forEntity'
XML_KEYWORD = '.xml'
ZIP_KEYWORD = '.zip'
MISSING_TSO_NAME = 'UnknownTSO'
LONG_FILENAME_SUFFIX = u"\\\\?\\"
VALIDATION_DURATION_KEYWORD = 'validation_duration_s'
LOADFLOW_RESULTS_KEYWORD = 'loadflow_results'
PREFERRED_FILE_TYPES = [XML_KEYWORD, ZIP_KEYWORD]
BOUNDARY_FILE_TYPES = ['_EQBD_', '_TPBD_', '_EQ_BD_', '_TP_BD_']
SYSTEM_SPECIFIC_FOLDERS = ['__MACOSX']
UNWANTED_FILE_TYPES = ['.xlsx', '.docx', '.pptx']
RECURSION_LIMIT = 2
USE_ROOT = False  # extracts to root, not to folder specified to zip. Note that some zip examples may not work!

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.local_file_import)

PY_ENTSOE_EXAMPLES_LOCAL = ENTSOE_EXAMPLES_LOCAL
PY_ENTSOE_EXAMPLES_EXTERNAL = ENTSOE_EXAMPLES_EXTERNAL
PY_MODEL_FOLDER_PATH = MODEL_FOLDER_PATH


class LocalFileLoaderError(FileNotFoundError):
    """
    For throwing when errors occur during the process of loading local files
    """
    pass


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


def get_one_set_of_igms_from_local_storage(file_data: [],
                                           tso_name: str = None,
                                           file_types: [] = None,
                                           igm_reference: str = None,
                                           priority_profile: str = 'SV'):
    """
    Loads igm data from local storage.
    :param file_data: list of file names
    :param tso_name: the name of the subtopic_name if given
    :param file_types: list of file types
    :param igm_reference: content reference if given
    :param priority_profile:  set the corresponding to profile reference
    :return: dictionary that wants to be similar to OPDM profile
    """
    igm_value = {OPDE_OBJECT_TYPE_KEYWORD: IGM_OBJECT_TYPE,
                 PMD_CONTENT_REFERENCE_KEYWORD: igm_reference,
                 OPDE_COMPONENT_KEYWORD: []}
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
            # Update the file name
            if original_file_name := opdm_profile_content.get(PMD_FILENAME_KEYWORD):
                new_name = Path(original_file_name).stem + '.zip'
                opdm_profile_content[PMD_FILENAME_KEYWORD] = new_name
                reference_value = generate_OPDM_ContentReference_from_filename(new_name)
                opdm_profile_content[PMD_CONTENT_REFERENCE_KEYWORD] = reference_value
            if (not igm_value[PMD_CONTENT_REFERENCE_KEYWORD] or
                    (opdm_profile_content.get(PMD_CGMES_PROFILE_KEYWORD) == priority_profile and not igm_reference)):
                if opdm_profile_content.get(PMD_CONTENT_REFERENCE_KEYWORD):
                    igm_value[PMD_CONTENT_REFERENCE_KEYWORD] = opdm_profile_content[PMD_CONTENT_REFERENCE_KEYWORD]
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
        except Exception as ex:
            logger.warning(f"Unable to set IGM from profile: {ex}")
            continue
    igm_value[PMD_SCENARIO_DATE_KEYWORD] = scenario_date or ''
    igm_value[PMD_TIME_HORIZON_KEYWORD] = time_horizon or ''
    igm_value[PMD_MODEL_PART_REFERENCE_KEYWORD] = model_part_reference or ''
    igm_value[PMD_VERSION_NUMBER_KEYWORD] = version_number or ''
    return igm_value


def get_one_set_of_boundaries_from_local_storage(file_names: [],
                                                 file_types: [] = None,
                                                 boundary_reference: str = None,
                                                 priority_profile: str = 'TPBD'):
    """
    Loads boundary data from local storage.
    :param file_names: list of file names
    :param file_types: list of file types
    :param boundary_reference: content reference if given
    :param priority_profile:  set the corresponding to profile reference
    :return: dictionary that wants to be similar to OPDM profile
    """
    boundary_value = {OPDE_OBJECT_TYPE_KEYWORD: BOUNDARY_OBJECT_TYPE,
                      PMD_CONTENT_REFERENCE_KEYWORD: boundary_reference,
                      OPDE_COMPONENT_KEYWORD: []}
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
            # Update the file name
            if cgmes_profile := opdm_profile_content.get(PMD_CGMES_PROFILE_KEYWORD):
                if len(cgmes_profile) == 4:
                    opdm_profile_content[PMD_CGMES_PROFILE_KEYWORD] = cgmes_profile[:2] + '_' + cgmes_profile[2:]
            if original_file_name := opdm_profile_content.get(PMD_FILENAME_KEYWORD):
                new_name = Path(original_file_name).stem + '.zip'
                opdm_profile_content[PMD_FILENAME_KEYWORD] = new_name
                reference_value = generate_OPDM_ContentReference_from_filename(new_name)
                opdm_profile_content[PMD_CONTENT_REFERENCE_KEYWORD] = reference_value
            if (not boundary_value[PMD_CONTENT_REFERENCE_KEYWORD] or
                    (cgmes_profile == priority_profile and not boundary_reference)):
                if opdm_profile_content.get(PMD_CONTENT_REFERENCE_KEYWORD):
                    boundary_value[PMD_CONTENT_REFERENCE_KEYWORD] = opdm_profile_content[PMD_CONTENT_REFERENCE_KEYWORD]
            opdm_profile_content = meta_for_data[datum]
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
            # raise LocalFileLoaderError
    return list_of_files


class LocalInputType(Enum):
    """
    Enum for different data loads (igm and boundary)
    """
    BOUNDARY = 'boundary',
    IGM = 'igm'
    UNDEFINED = 'undefined'


def get_data_from_files(file_locations: list | str | dict,
                        get_type: LocalInputType = LocalInputType.IGM,
                        file_keywords: list = None):
    """
    Extracts and parses data to necessary profile
    :param file_locations: list of files or their locations, one element per subtopic_name
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
                    default_tso_name = f"{MISSING_TSO_NAME}-{tso_counter}"
                    tso_counter += 1
                    logger.warning(f"TSO name not found assigning default name as {default_tso_name}")
                    igm_value[PMD_TSO_KEYWORD] = default_tso_name
                all_models.append(igm_value)
    else:
        logger.error(f"Unsupported input")
        raise LocalFileLoaderError
    if len(all_models) == 0:
        logger.error(f"Data for igms were not valid, no igms were extracted")
        raise LocalFileLoaderError
    return all_models


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
    r = requests.get(url_to_zip, stream=True)
    if r.status_code != 200:
        raise RuntimeError(f"{url_to_zip} returned status code {r.status_code}, terminating download")
    file_size = int(r.headers.get('Content-Length', 0))
    downloaded_size = 0
    chunk_size = 1024
    logger.info(f"Downloading examples from {url_to_zip} to {loaded_file_name}")
    with open(loaded_file_name, 'wb') as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            downloaded_size += chunk_size
            sys.stdout.write(f"\rDownloaded {(100 * downloaded_size / file_size):.2f}%")
            sys.stdout.flush()
        # shutil.copyfileobj(r.raw, f)
    sys.stdout.write("\n")
    logger.info(f"Download completed")
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
                           local_folder_for_examples: str = PY_ENTSOE_EXAMPLES_LOCAL,
                           url_for_examples: str = PY_ENTSOE_EXAMPLES_EXTERNAL,
                           recursion_depth: int = RECURSION_LIMIT):
    """
    Checks if examples are present if no then downloads and extracts them
    :param use_root: use root folder for extraction
    :param local_folder_for_examples: path to the examples
    :param url_for_examples: path to online storage
    :param recursion_depth: the max allowed iterations for the recursion
    :param path_to_search: folder to search
    """
    # If root folder is not specified return search path
    if not local_folder_for_examples:
        return path_to_search
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
            full_file_name = download_zip_file(url_for_examples, local_folder_for_examples)
        # Now, there should be a zip present, extract it
        extract_zip_file(current_zip_file=full_file_name,
                         root_folder=local_folder_for_examples,
                         use_root=use_root,
                         max_depth=recursion_depth)
    # And try to find the necessary path
    return search_directory(local_folder_for_examples, path_to_search)


def check_if_filename_exists_in_list(existing_file_instances: list, new_file_instance):
    """
    Checks if filename is present in dict, if not then adds it
    Also checks version numbers, replaces the filename if the version number is bigger
    :param existing_file_instances: list of existing file names
    :param new_file_instance: new file name to be added
    """
    if isinstance(new_file_instance, str):
        reduced_file_name = os.path.basename(new_file_instance)
        existing_file_names = existing_file_instances
    else:
        try:
            reduced_file_name = os.path.basename(new_file_instance.name)
            existing_file_names = [file_instance.name for file_instance in existing_file_instances]
        except Exception:
            return existing_file_instances
    file_base = os.path.splitext(reduced_file_name)[0]
    file_base_dict = get_meta_from_filename(reduced_file_name)
    if any([file_base in existing_file_name for existing_file_name in existing_file_names]):
        return existing_file_instances
    new_version_number = file_base_dict.get(MODEL_VERSION_KEYWORD) or file_base_dict.get(PMD_VERSION_NUMBER_KEYWORD)
    file_base_reduced = file_base.removesuffix(new_version_number)

    similar = {file_name: get_meta_from_filename(os.path.basename(file_name))
               for file_name in existing_file_names if file_base_reduced in file_name}
    if similar:
        exists = {file: similar[file].get(MODEL_VERSION_KEYWORD) or similar[file].get(PMD_VERSION_NUMBER_KEYWORD)
                  for file in similar}
        max_file = max(exists, key=exists.get)
        if int(exists[max_file]) < int(new_version_number):
            if max_file in existing_file_instances:
                existing_file_instances.remove(max_file)
            else:
                try:
                    existing_file_instances = [file_instance for file_instance in existing_file_instances if
                                               max_file not in file_instance.name]
                except Exception:
                    logger.error(f"Unable to remove duplicates")
                    return existing_file_instances
        else:
            return existing_file_instances
    existing_file_instances.append(new_file_instance)
    return existing_file_instances


def group_files_by_origin(list_of_files: [], root_folder: str = None, allow_merging_entities: bool = False):
    """
    When input is a directory containing the .xml and .zip files for all the TSOs and boundaries as well and
    if files follow the standard name convention, then this one sorts them by TSOs and by boundaries
    The idea is that one subtopic_name can have only one type of file only once (e.g. one subtopic_name cannot have two
    'TP' files)
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
    for file_instance in list_of_files:
        if isinstance(file_instance, str):
            file_name = file_instance
            if root_folder is not None:
                file_instance = root_folder.removesuffix('/') + '/' + file_instance.removeprefix('/')
        else:
            try:
                file_name = file_instance.name
            except Exception:
                logger.error(f"Unknown input: {file_instance}")
                continue
        file_name = os.path.basename(file_name)
        file_extension = os.path.splitext(file_name)[-1]
        # Check if file is supported file
        if file_extension not in PREFERRED_FILE_TYPES:
            continue
        # Check if file supports standard naming convention, refer to helper.get_metadata_from_filename for more details
        file_name_meta = get_meta_from_filename(file_name)
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
        if not allow_merging_entities and merging_entity and tso_name not in SPECIAL_TSO_NAME:
            continue
        if not tso_name:
            tso_name = modeling_entity or merging_entity
        if tso_name and file_type_name:
            # Handle TSOs
            if file_type_name in igm_file_types:
                if tso_name not in tso_files.keys():
                    tso_files[tso_name] = []
                # Check if file without the extension is already present
                tso_files[tso_name] = check_if_filename_exists_in_list(existing_file_instances=tso_files[tso_name],
                                                                       new_file_instance=file_instance)
            # Handle boundaries
            elif file_type_name in boundary_file_types:
                if tso_name not in boundaries.keys():
                    boundaries[tso_name] = []
                # Check if file without the extension is already present
                boundaries[tso_name] = check_if_filename_exists_in_list(existing_file_instances=boundaries[tso_name],
                                                                        new_file_instance=file_instance)
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


def get_latest_boundary(path_to_directory: str | list = PY_MODEL_FOLDER_PATH,
                        local_folder_for_examples: str = PY_ENTSOE_EXAMPLES_LOCAL,
                        url_for_examples: str = PY_ENTSOE_EXAMPLES_EXTERNAL,
                        boundary_files_needed: list = None):
    """
     Gets list of files in directory and returns those who categorize to boundary in opdm format
     :param path_to_directory: path to directory from where to search
     :param local_folder_for_examples: path to the examples
     :param url_for_examples: path to online storage
     :param boundary_files_needed: specify explicitly the file types needed (escape pypowsybl "EQ" missing error)
     """
    if isinstance(path_to_directory, str):
        path_to_directory = [path_to_directory]
    all_boundaries = []
    for single_path in path_to_directory:
        try:
            full_path = check_and_get_examples(single_path,
                                               local_folder_for_examples=local_folder_for_examples,
                                               url_for_examples=url_for_examples)
        except Exception as ex:
            logger.error(f"FATAL ERROR WHEN GETTING FILES: {ex}")
            sys.exit()
        full_path = check_the_folder_path(full_path)
        file_names = next(os.walk(full_path), (None, None, []))[2]
        models_data, boundary_data = group_files_by_origin(list_of_files=file_names,
                                                           root_folder=full_path)
        if boundary_data:
            boundaries_transformed = get_data_from_files(file_locations=boundary_data,
                                                         get_type=LocalInputType.BOUNDARY,
                                                         file_keywords=BOUNDARY_FILE_TYPES)
            if boundaries_transformed:
                all_boundaries.extend(boundaries_transformed)
    if len(all_boundaries) == 0:
        logger.warning(f"No boundaries found")
        raise LocalFileLoaderError
    else:
        if len(all_boundaries) > 1:
            all_boundaries.sort(key=lambda x: parse_datetime(x.get(PMD_SCENARIO_DATE_KEYWORD)), reverse=True)
    boundary = all_boundaries[0]
    if boundary_files_needed:
        boundary = check_model_completeness(boundary, boundary_files_needed)
    return boundary


def get_latest_models_and_download(time_horizon: str = None,
                                   scenario_date: str = None,
                                   tso: str | list = None,
                                   path_to_directory: str | list = PY_MODEL_FOLDER_PATH,
                                   local_folder_for_examples: str = PY_ENTSOE_EXAMPLES_LOCAL,
                                   url_for_examples: str = PY_ENTSOE_EXAMPLES_EXTERNAL,
                                   allow_merging_entities: bool = False,
                                   igm_files_needed: list = None):
    """
     Gets list of files in directory and returns those who categorize to igm data in opdm format
     :param time_horizon: time horizon if given will be used for filtering
     :param scenario_date: scenario date if given will be used for filtering
     :param tso: if given will be used for filtering
     :param path_to_directory: path to directory from where to search
     :param local_folder_for_examples: path to the examples
     :param url_for_examples: path to online storage
     :param allow_merging_entities: true allow cases like TECNET-CE-ELIA to list of models
     :param igm_files_needed: specify explicitly the file types needed (escape pypowsybl "EQ" missing error)
     """
    if isinstance(path_to_directory, str):
        path_to_directory = [path_to_directory]
    models = []
    for single_path in path_to_directory:
        try:
            full_path = check_and_get_examples(single_path,
                                               local_folder_for_examples=local_folder_for_examples,
                                               url_for_examples=url_for_examples)
        except Exception as ex:
            logger.error(f"FATAL ERROR WHEN GETTING FILES: {ex}")
            sys.exit()
        full_path = check_the_folder_path(full_path)
        file_names = next(os.walk(full_path), (None, None, []))[2]
        models_data, boundary_data = group_files_by_origin(list_of_files=file_names,
                                                           root_folder=full_path,
                                                           allow_merging_entities=allow_merging_entities)
        if models_data:
            models_transformed = get_data_from_files(file_locations=models_data,
                                                     get_type=LocalInputType.IGM,
                                                     file_keywords=IGM_FILE_TYPES)
            models.extend(models_transformed)
    if igm_files_needed is not None:
        models = check_model_completeness(models, igm_files_needed)
    # if parameters were given filter the results by it
    if tso:
        if isinstance(tso, str):
            tso = [tso]
        models = [igm_model for igm_model in models if igm_model.get(PMD_TSO_KEYWORD) in tso]
    if time_horizon:
        models = [igm_model for igm_model in models if igm_model.get(PMD_TIME_HORIZON_KEYWORD) == time_horizon]
    if scenario_date:
        models = [igm_model for igm_model in models
                  if parse_datetime(igm_model.get(PMD_SCENARIO_DATE_KEYWORD)) == parse_datetime(scenario_date)]
    return models


def save_merged_model_to_local_storage(cgm_files,
                                       cgm_folder_name: str = None,
                                       local_storage_location: str = LOCAL_STORAGE_LOCATION):
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
    if not isinstance(cgm_files, list):
        cgm_files = [cgm_files]
    for cgm_file in cgm_files:
        try:
            file_name = cgm_file.name
        except AttributeError:
            file_name = str(uuid.uuid4())
            if zipfile.is_zipfile(cgm_file):
                file_name = file_name + '.zip'
        full_file_name = local_storage_location.removesuffix('/') + '/' + file_name.removeprefix('/')
        with open(full_file_name, 'wb') as f:
            if isinstance(cgm_file, BytesIO):
                f.write(cgm_file.getbuffer())
            else:
                f.write(cgm_file)


if __name__ == "__main__":

    import sys
    from emf.common.integrations.opdm import OPDM

    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout),
                  # Set up the log gatherer:
                  # topic name: currently used as a start of a file name
                  # send_it_to_elastic: send the triggered log entry to elastic
                  # upload_to_minio: upload log file to minio (parameters are defined in custom_logger.properties)
                  # report_on_command: trigger reporting explicitly
                  # logging policy: choose according to the need. Currently:
                  #   ALL_ENTRIES: gathers all log entries no matter of what
                  #   ENTRIES_IF_LEVEL_REACHED: outputs all the log when at least one entry was over level
                  #   ENTRIES_ON_LEVEL: gathers all entries that were at least on the level specified
                  # print_to_console: propagate log to parent
                  # reporting_level: level that triggers policy
                  # PyPowsyblLogGatheringHandler(topic_name='IGM_validation',
                  #                              send_to_elastic=False,
                  #                              upload_to_minio=False,
                  #                              save_local_storage=True,
                  #                              logging_policy=PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL,
                  #                              print_to_console=False,
                  #                              report_level=logging.WARNING)
                  ]
    )
    # Get the pypowsybl log handler from root
    log_handler = get_pypowsybl_log_handler()

    # Switch this to True if files from local storage are used
    load_data_from_local_storage = True
    try:
        if load_data_from_local_storage:
            # available_models, latest_boundary = get_local_files()
            # Change this according the test case to be used. Note that it must reference to the end folder that will
            # be used. Also, it must be unique enough do be distinguished from other folders (for example instead of
            # using 'Combinations' use 'TC1_T11_NonConform_L1/Combinations' etc.)
            # Some examples for
            #  https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/QoCDC_v3.2.1_test_models.zip
            # folder_to_study = 'apg_case'
            folder_to_study = 'TC3_T1_Conform'
            examples_path = '../../../loadflow_tool/example_models/'
            # Some examples for
            #   https://www.entsoe.eu/Documents/CIM_documents/Grid_Model_CIM/TestConfigurations_packageCASv2.0.zip
            # folder_to_study = ['CGMES_v2.4.15_MicroGridTestConfiguration_T1_BE_Complete_v2',
            #                    'CGMES_v2.4.15_MicroGridTestConfiguration_T1_NL_Complete_v2',
            #                    'Type1_T1/CGMES_v2.4.15_MicroGridTestConfiguration_BD_v2']
            # In general this function checks if the paths (path_to_directory) exist in ENTSOE_EXAMPLES_LOCAL,
            # if not then it tries to download and extract zip from ENTSOE_EXAMPLES_EXTERNAL. If it fails or path
            # is not still found it carries on as usual.
            # Note that zip can be downloaded and extracted but in this case it must be extracted to the path
            # path_to_directory: string or list, end of the path from where to load the files
            # (starting from ENTSOE_EXAMPLES_LOCAL). Note that these must be unique enough (errors are thrown when
            # two or more paths are found)
            # allow_merging_entities: Whether to allow merging entities, pypowsybl validation was not happy about that
            # igm_files_needed: in order for pypowsybl validation to work, at least these files should be present in
            # igm
            test_time_horizon = '1D'
            test_tsos = ['mavir', 'seps']
            test_scenario_date = '2021-09-21T09:30:00+00:00'
            available_models = get_latest_models_and_download(time_horizon=test_time_horizon,
                                                              scenario_date=test_scenario_date,
                                                              path_to_directory=folder_to_study,
                                                              local_folder_for_examples=examples_path,
                                                              tso=test_tsos)
            latest_boundary = get_latest_boundary(path_to_directory=folder_to_study,
                                                  local_folder_for_examples=examples_path)
        else:
            raise LocalFileLoaderError
    except FileNotFoundError:
        # if needed catch and handle LocalFileLoaderError separately
        logger.info(f"Fetching data from external resources")
        opdm = OPDM()
        latest_boundary = opdm.get_latest_boundary()
        available_models = opdm.get_latest_models_and_download(time_horizon='ID',
                                                               scenario_date='2024-04-05T22:30',
                                                               # subtopic_name='ELERING'
                                                               )

    validated_models = []
    # Validate models
    # NB! Validate_models starts rabbit listener by import
    for model in available_models:
        model_tso_name = model['pmd:TSO']
        # pypowsybl_log_gatherer.set_subtopic_name(tso)
        if log_handler:
            log_handler.set_sub_topic_name(model_tso_name)
        try:
            if isinstance(latest_boundary, dict):
                response = validate_model([model, latest_boundary])
            else:
                response = validate_model([model])
            model[VALIDATION_STATUS_KEYWORD] = response
            validated_models.append(model)
        except Exception as error:
            validated_models.append(model)
            logger.error(f"For {model.get('pmd:TSO')} validation failed", error)
    # Print validation statuses
    [print(dict(tso=model['pmd:TSO'], valid=model.get('VALIDATION_STATUS', {}).get('valid'),
                duration=model.get('VALIDATION_STATUS', {}).get('validation_duration_s'))) for model in
     validated_models]
