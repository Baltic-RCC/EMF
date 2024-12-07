import os

from emf.loadflow_tool.helper import metadata_from_filename, generate_OPDM_ContentReference_from_filename

import logging

logger = logging.getLogger(__name__)


OPDE_COMPONENT_KEYWORD = 'opde:Component'
OPDM_PROFILE_KEYWORD = 'opdm:Profile'
DATA_KEYWORD = 'DATA'
PMD_FILENAME_KEYWORD = 'pmd:fileName'
PMD_CGMES_PROFILE_KEYWORD = 'pmd:cgmesProfile'
PMD_MODEL_PART_REFERENCE_KEYWORD = 'pmd:modelPartReference'
PMD_MERGING_ENTITY_KEYWORD = 'pmd:mergingEntity'
PMD_MERGING_AREA_KEYWORD = 'pmd:mergingArea'
PMD_SCENARIO_DATE_KEYWORD = 'pmd:scenarioDate'
PMD_CONTENT_REFERENCE_KEYWORD = 'pmd:content-reference'
OPDE_OBJECT_TYPE_KEYWORD = 'opde:Object-Type'
PMD_TSO_KEYWORD = 'pmd:TSO'
PMD_VERSION_NUMBER_KEYWORD = "pmd:versionNumber"
PMD_TIME_HORIZON_KEYWORD = 'pmd:timeHorizon'
PMD_CREATION_DATE_KEYWORD = 'pmd:creationDate'
PMD_MODEL_ID_KEYWORD = 'pmd:modelid'
PMD_MODELING_AUTHORITY_SET_KEYWORD = 'pmd:modelingAuthoritySet'
BOUNDARY_OBJECT_TYPE = 'BDS'
IGM_OBJECT_TYPE = 'IGM'
CGM_OBJECT_TYPE = 'CGM'
MODEL_MESSAGE_TYPE_KEYWORD = 'Model.messageType'
MODEL_MODELING_ENTITY_KEYWORD = 'Model.modelingEntity'
MODEL_MERGING_ENTITY_KEYWORD = 'Model.mergingEntity'
MODEL_DOMAIN_KEYWORD = 'Model.domain'
MODEL_SCENARIO_TIME_KEYWORD = 'Model.scenarioTime'
MODEL_PROCESS_TYPE_KEYWORD = 'Model.processType'
MODEL_VERSION_KEYWORD = 'Model.version'
IGM_FILE_TYPES = ['_EQ_', '_TP_', '_SV_', '_SSH_']
BOUNDARY_FILE_TYPE_FIX = {'_EQ_BD_': '_EQBD_', '_TP_BD_': '_TPBD_'}
SPECIAL_TSO_NAME = ['ENTSO-E']
VALIDATION_STATUS_KEYWORD = 'VALIDATION_STATUS'
VALID_KEYWORD = 'valid'
NETWORK_KEYWORD = 'network'
NETWORK_META_KEYWORD = 'network_meta'
NETWORK_VALID_KEYWORD = 'network_valid'
SEPARATOR_SYMBOL = '/'
WINDOWS_SEPARATOR = '\\'


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
        meta_data = metadata_from_filename(os.path.basename(fixed_file_name))
        # Revert back cases where there is a '-' in TSO's name like ENTSO-E
        # Some very special fix for general zip in form scenario-date_time-horizon_tso_revision
        if meta_data[PMD_TIME_HORIZON_KEYWORD] == '':
            igm_file_type_list = [file_type.replace('_', '') for file_type in IGM_FILE_TYPES]
            boundary_file_type_list = [file_type.strip("_") for file_type in BOUNDARY_FILE_TYPE_FIX.values()]
            if ((meta_data[PMD_CGMES_PROFILE_KEYWORD] not in igm_file_type_list + boundary_file_type_list) and
                    (len(meta_data[PMD_MODEL_PART_REFERENCE_KEYWORD]) == 2)):
                meta_data[PMD_TIME_HORIZON_KEYWORD] = meta_data[PMD_MODEL_PART_REFERENCE_KEYWORD]
                meta_data[PMD_MODEL_PART_REFERENCE_KEYWORD] = meta_data[PMD_CGMES_PROFILE_KEYWORD]
        # End of very special fix
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
    # meta_data[PMD_CONTENT_REFERENCE_KEYWORD] = generate_OPDM_ContentReference_from_filename(file_name)
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
