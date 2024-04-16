from enum import Enum

import pypowsybl
import zeep.exceptions

import config
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import opdm, minio
from emf.common.integrations.elastic import Elastic
from emf.common.integrations.object_storage.object_storage import query_data, get_content
from emf.common.logging.custom_logger import SEPARATOR_SYMBOL
from emf.loadflow_tool.helper import (load_model, load_opdm_data, filename_from_metadata, export_model,
                                      NETWORK_KEYWORD, NETWORK_META_KEYWORD, get_metadata_from_filename)
from emf.loadflow_tool.validator import validate_model, get_local_entsoe_files, LocalFileLoaderError, VALID_KEYWORD, \
    VALIDATION_STATUS_KEYWORD, OPDE_COMPONENT_KEYWORD, MODEL_MESSAGE_TYPE, parse_boundary_message_type_profile, \
    OPDM_PROFILE_KEYWORD, DATA_KEYWORD
import logging
import json
from emf.loadflow_tool import loadflow_settings
import sys
from emf.common.integrations.opdm import OPDM
from aniso8601 import parse_datetime
import os
import triplets
import pandas
import datetime
from uuid import uuid4

from emf.model_retriever.model_retriever import HandlerModelsToMinio, HandlerMetadataToElastic, HandlerModelsValidator

# Update SSH

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.model_merge)

logging.basicConfig(
    format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

UPDATE_MAP = [
    {
        "from_class": "SvPowerFlow",
        "from_ID": "Terminal.ConductingEquipment",
        "from_attribute": "SvPowerFlow.p",
        "to_attribute": "EnergyConsumer.p",
    },
    {
        "from_class": "SvPowerFlow",
        "from_ID": "Terminal.ConductingEquipment",
        "from_attribute": "SvPowerFlow.q",
        "to_attribute": "EnergyConsumer.q",
    },
    {
        "from_class": "SvPowerFlow",
        "from_ID": "Terminal.ConductingEquipment",
        "from_attribute": "SvPowerFlow.p",
        "to_attribute": "RotatingMachine.p",
    },
    {
        "from_class": "SvPowerFlow",
        "from_ID": "Terminal.ConductingEquipment",
        "from_attribute": "SvPowerFlow.q",
        "to_attribute": "RotatingMachine.q",
    },
    {
        "from_class": "SvTapStep",
        "from_ID": "SvTapStep.TapChanger",
        "from_attribute": "SvTapStep.position",
        "to_attribute": "TapChanger.step",
    },
    {
        "from_class": "SvShuntCompensatorSections",
        "from_ID": "SvShuntCompensatorSections.ShuntCompensator",
        "from_attribute": "SvShuntCompensatorSections.sections",
        "to_attribute": "ShuntCompensator.sections",
    }
]

FILENAME_MASK = ("{scenarioTime:%Y%m%dT%H%MZ}_{processType}_"
                 "{mergingEntity}-{domain}-{forEntity}_{messageType}_{version:03d}")

NAMESPACE_MAP = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
    "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
    "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    # "cgmbp": "http://entsoe.eu/CIM/Extensions/CGM-BP/2020#"
}
RDF_MAP_JSON = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'entsoe_v2.4.15_2014-08-07.json')
PATTERN_WITHOUT_TIMEZONE = '%Y-%m-%dT%H:%M:%S'

CGM_CREATION_DATE_KEYWORD = "pmd:creationDate"
CGM_MERGING_ENTITY_KEYWORD = "pmd:mergingEntity"
CGM_MERGING_ENTITY = "BALTICRSC"
CGM_VERSION_NUMBER_KEYWORD = "pmd:versionNumber"
CGM_TIME_HORIZON_KEYWORD = 'pmd:timeHorizon'
CGM_MERGING_AREA_KEYWORD = 'pmd:mergingArea'
CGM_VALID_FROM_KEYWORD = 'pmd:validFrom'

DEFAULT_INDEX_NAME = "emfos-logs*"

# Variables used for local testing
TIME_HORIZON = '1D'
SCENARIO_DATE = '2024-03-14T09:30'
AREA = 'EU'
VERSION = "104"
PUBLISH_TO_OPDM = False
USE_LOCAL_FILES = True
LOCAL_FOLDER = 'TC3_T1_Conform'

PROCESS_ID_KEYWORD = "process_id"
RUN_ID_KEYWORD = 'run_id'
JOB_ID_KEYWORD = 'job_id'

FULL_PATH_KEYWORD = 'full_path'


class DownloadModels(Enum):
    """
    For determining from where to download files
    """
    OPDM = 1
    MINIO = 2
    OPDM_AND_MINIO = 3


def load_rdf_map(file_name: str = RDF_MAP_JSON):
    """
    loads rdf map file
    :param file_name: file from where to load
    :return: rdf map
    """
    with open(file_name, 'r') as file_object:
        rdf_map = json.load(file_object)
    return rdf_map


def check_dataframe(first_input=None, second_input=None):
    """
    Escapes first input if not given
    :param first_input: first element to be checked
    :param second_input: second element to be checked
    :return: first_input ro second_input (for dataframes)
    """
    if first_input is not None and isinstance(first_input, pandas.DataFrame):
        return first_input
    return second_input


def get_local_models(time_horizon: str = TIME_HORIZON,
                     scenario_date: str = SCENARIO_DATE,
                     use_local_files: bool = USE_LOCAL_FILES,
                     local_file_folder: str = LOCAL_FOLDER,
                     allow_merging_entities: bool = False,
                     igm_files_needed=None,
                     opdm_client: OPDM = None):
    """
    For local testing only. Takes the files from the folder specified that can be passed to the next steps. Fallback
    is set to getting the files from opdm
    :param time_horizon: time horizon of the igms
    :param scenario_date: the date of the scenario for which the igm was created
    :param use_local_files: true, uses local files
    :param local_file_folder: unique folder where to find files
    :param allow_merging_entities: true escapes already merged files
    :param igm_files_needed: specify specific igm files needed
    :param opdm_client: client for the opdm
    """
    if igm_files_needed is None:
        igm_files_needed = ['EQ']
    try:
        if use_local_files:
            available_models, latest_boundary = get_local_entsoe_files(path_to_directory=local_file_folder,
                                                                       allow_merging_entities=allow_merging_entities,
                                                                       igm_files_needed=igm_files_needed)
        else:
            raise LocalFileLoaderError
    except FileNotFoundError:
        logger.info(f"Getting data from OPDM")
        available_models, latest_boundary = get_models(time_horizon=time_horizon,
                                                       scenario_date=scenario_date,
                                                       opdm_client=opdm_client)
    return available_models, latest_boundary


def boundary_data_from_local_storage():
    pass


class CgmModelType(Enum):
    BOUNDARY = 1
    IGM = 2


def run_model_retriever_pipeline(opdm_models: dict | list,
                                 latest_boundary: dict = None,
                                 model_type: CgmModelType = CgmModelType.IGM):
    """
    Initializes model_retriever pipeline to download, validate and push the models to minio/elastic
    THIS IS A HACK!!! DO NOT USE IT ANYWHERE ELSE THAN IN TESTING MODE
    :param opdm_models: dictionary of opdm_models
    :param latest_boundary:
    :param model_type: specify whether the files are boundary data or igm data
    : return updated opdm models
    """
    minio_handler = HandlerModelsToMinio()
    validator_handler = HandlerModelsValidator()
    metadata_handler = HandlerMetadataToElastic()
    if isinstance(opdm_models, dict):
        opdm_models = [opdm_models]
    opdm_models = minio_handler.handle_reduced(opdm_objects=opdm_models)
    if model_type == CgmModelType.IGM:
        opdm_models = validator_handler.handle(opdm_objects=opdm_models, latest_boundary=latest_boundary)
    opdm_models = metadata_handler.handle(opdm_objects=opdm_models)
    return opdm_models


def get_latest_boundary(opdm_client: OPDM = None):
    """
    Tries to get the boundary data from OPDM, if not successful, fallback to Minio and take the latest
    Alternative would be to check depends on
    :param opdm_client: OPDM client
    """
    boundary_data = None
    try:
        opdm_client = opdm_client or OPDM()
        boundary_data = opdm_client.get_latest_boundary()
        # if model_retriever_pipeline:
        #     boundary_data = run_model_retriever_pipeline(opdm_models=boundary_data, model_type=CgmModelType.BOUNDARY)
        # raise zeep.exceptions.Fault
    except zeep.exceptions.Fault as fault:
        logger.error(f"Could not get boundary data from OPDM: {fault}")
        # boundary_data = get_boundary_data_from_minio()
        # should be query_data, but for now ask it minio
    except Exception as ex:
        logger.error(f"Undefined exception when getting boundary data: {ex}")
        # boundary_data = get_boundary_data_from_minio()
    finally:
        return boundary_data


def get_models(time_horizon: str = TIME_HORIZON,
               scenario_date: str = SCENARIO_DATE,
               tso_names: list | str = None,
               download_policy: DownloadModels = DownloadModels.OPDM_AND_MINIO,
               model_retriever_pipeline: bool = True,
               opdm_client: OPDM = None):
    """
    Gets models from opdm and/or minio
    NB! Priority is given to Minio!
    Workflow:
    1) Get models from opdm if selected
    2) Get models from minio if selected or opdm failed
    3) If requested from both, take data from minio and extend it from opdm
    4) By default get boundary from opdm
    5) Fallback: get boundary from minio
    :param time_horizon: time horizon of the igms
    :param scenario_date: the date of the scenario for which the igm was created
    :param tso_names: list or string of tso names
    :param download_policy: from where to download models
    :param model_retriever_pipeline
    :param opdm_client: client for the opdm
    """
    opdm_models = None
    minio_models = None
    # 1 Get boundary data
    boundary_data = get_latest_boundary(opdm_client=opdm_client)
    # 1 if opdm is selected, try to download from there
    if download_policy == DownloadModels.OPDM or download_policy == DownloadModels.OPDM_AND_MINIO:
        opdm_models = get_models_from_opdm(time_horizon=time_horizon,
                                           scenario_date=scenario_date,
                                           tso_names=tso_names,
                                           opdm_client=opdm_client)
        # Validate raw input models
        # opdm_models = validate_models(available_models=opdm_models, latest_boundary=boundary_data)
    # 2 if minio is selected or opdm failed, download data from there
    if download_policy == DownloadModels.MINIO or download_policy == DownloadModels.OPDM_AND_MINIO or not opdm_models:
        minio_models = get_models_from_elastic_minio(time_horizon=time_horizon,
                                                     scenario_date=scenario_date,
                                                     tso_names=tso_names)
        # If getting boundary failed try to get it from the dependencies
        if not boundary_data:
            boundary_data = get_boundary_from_dependencies(igm_models=minio_models)
    # If something was got from opdm, run through it model_retriever pipeline
    if download_policy == DownloadModels.OPDM:
        if model_retriever_pipeline and opdm_models:
            opdm_models = run_model_retriever_pipeline(opdm_models=opdm_models)
        igm_models = opdm_models or minio_models
    elif download_policy == DownloadModels.MINIO:
        igm_models = minio_models
    else:
        # 3. When merge is requested, give priority to minio, update it from opdm
        igm_models = minio_models
        existing_tso_names = [model.get('pmd:TSO') for model in minio_models]
        if opdm_models:
            additional_tso_models = [model for model in opdm_models if model.get('pmd:TSO') not in existing_tso_names]
            if model_retriever_pipeline and additional_tso_models:
                additional_tso_models = run_model_retriever_pipeline(opdm_models=additional_tso_models)
            igm_models.extend(additional_tso_models)
    return igm_models, boundary_data


def get_models_from_opdm(time_horizon: str, scenario_date: str, tso_names: list | str = None, opdm_client: OPDM = None):
    """
    Gets models from opdm
    :param time_horizon: time horizon of the igms
    :param scenario_date: the date of the scenario for which the igm was created
    :param tso_names: list or string of tso names, if given, only matching models are returned
    :param opdm_client: client for the opdm
    :return list of models if found, None otherwise
    """
    available_models = None
    try:
        opdm_client = opdm_client or OPDM()
        # latest_boundary = opdm_client.get_latest_boundary()
        scenario_date_iso = datetime.datetime.fromisoformat(scenario_date)
        converted_scenario_date = scenario_date_iso.strftime(PATTERN_WITHOUT_TIMEZONE)
        available_models = opdm_client.get_latest_models_and_download(time_horizon=time_horizon,
                                                                      scenario_date=converted_scenario_date)
        if tso_names:
            if isinstance(tso_names, str):
                tso_names = [tso_names]
            filtered_models = [model for model in available_models if model.get('pmd:TSO') in tso_names]
            available_models = filtered_models
        return available_models
    except zeep.exceptions.Fault as fault:
        logger.error(f"Could not connect to OPDM: {fault}")
    except Exception as ex:
        logger.error(f"Unknown exception when getting data from opdm: {ex}")
    finally:
        return available_models


def get_boundary_from_dependencies(igm_models: list):
    """
    Gets boundary data from dependencies
    Lists all dependencies from models, filters those which are BDS, takes the latest, unpacks it, downloads files to it
    and if everything went well then returns the result
    :param igm_models: list of igm models
    :return: boundary data if everything went successfully, None otherwise
    """
    # Get all dependencies
    dependencies = [model.get('opde:Dependencies', {}).get('opde:DependsOn') for model in igm_models]
    boundaries = [dependency for dependency in dependencies
                  if dependency.get('opdm:OPDMObject', {}).get('opde:Object-Type') == 'BDS']
    latest_date = max([parse_datetime(entry.get('opdm:OPDMObject', {}).get('pmd:scenarioDate'))
                       for entry in boundaries])
    latest_boundaries = [boundary for boundary in boundaries
                         if parse_datetime(boundary.get('opdm:OPDMObject', {}).get('pmd:scenarioDate')) == latest_date]
    if len(latest_boundaries) > 0 and (latest_boundary_value := (latest_boundaries[0]).get('opdm:OPDMObject')):
        latest_boundary_value = get_content(metadata=latest_boundary_value)
        if all(profile.get('opdm:Profile', {}).get('DATA')
               for profile in dict(latest_boundary_value).get('opde:Component', [])):
            return latest_boundary_value
    return None


def get_models_from_elastic_minio(time_horizon: str, scenario_date: str, tso_names: list | str = None):
    """
    Asks metadata from elastic, attaches files from minio
    NB! currently only those models are returned which have files in minio
    :param tso_names: list of tso's, if given only matching models are returned
    :param time_horizon: the time horizon
    :param scenario_date: the date requested
    :return: list of models
    """
    # 1 if no tso's are given shoot it as it is
    query = {'pmd:scenarioDate': scenario_date, 'valid': True}
    query_response = query_data(metadata_query=query, return_payload=True)
    if time_horizon == 'ID':
        # 00:00 to 23:00 or 01:00 to 24:00
        time_horizon = [f"{time_h:02}" for time_h in range(24)]
    else:
        time_horizon = [time_horizon]
    query_response = [response for response in query_response if response.get("pmd:timeHorizon") in time_horizon]
    if isinstance(tso_names, str):
        tso_names = [tso_names]
    if tso_names:
        query_response = [response for response in query_response if response.get("pmd:TSO") in tso_names]
    final_responses = []
    for response in query_response:
        if all(field.get('opdm:Profile').get('DATA') for field in response.get('opde:Component')):
            final_responses.append(response)
    return final_responses


def validate_models(available_models: list = None, latest_boundary: list = None):
    """
    Validates the raw output from the opdm
    :param available_models: list of igm models
    :param latest_boundary: dictionary containing the boundary data
    :return list of validated models
    """
    valid_models = []
    invalid_models = []
    # Validate models
    if not available_models or not latest_boundary:
        logger.error(f"Missing input data")
        return valid_models
    for model in available_models:

        try:
            response = validate_model([model, latest_boundary])
            model[VALIDATION_STATUS_KEYWORD] = response
            if response[VALID_KEYWORD]:
                valid_models.append(model)
            else:
                invalid_models.append(model)
        except:
            invalid_models.append(model)
            logger.error("Validation failed")
    return valid_models


def get_version_number_from_minio(minio_bucket: str,
                                  minio_client: minio.ObjectStorage = None,
                                  sub_folder: str = None,
                                  scenario_date: str = None,
                                  modeling_entity: str = None,
                                  time_horizon: str = None):
    """
    Gets file list from minio, explodes it and retrieves the biggest matched version number
    :param minio_client: if given
    :param minio_bucket: the name of the bucket
    :param sub_folder: prefix
    :param scenario_date: date of the merge
    :param modeling_entity: name of the merging entity
    :param time_horizon: the time horizon
    """
    new_version_number = 1
    try:
        exploded_results = get_filename_dataframe_from_minio(minio_bucket=minio_bucket,
                                                             minio_client=minio_client,
                                                             sub_folder=sub_folder)
        new_version_number = get_largest_version_from_filename_dataframe(exploded_results=exploded_results,
                                                                         scenario_date=scenario_date,
                                                                         time_horizon=time_horizon,
                                                                         modeling_entity=modeling_entity)
    except Exception as ex:
        logger.warning(f"Got minio error: {ex}, starting with version number {new_version_number:03}")
    return f"{new_version_number:03}"


def get_filename_dataframe_from_minio(minio_bucket: str,
                                      minio_client: minio.ObjectStorage = None,
                                      sub_folder: str = None):
    """
    Gets file list from minio bucket (prefix can be specified with sub folder) and converts to dataframe following
    the standard naming convention (see get_metadata_from_filename for more details)
    :param minio_client: if given
    :param minio_bucket: the name of the bucket
    :param sub_folder: prefix
    """
    minio_client = minio_client or minio.ObjectStorage()
    if sub_folder:
        list_of_files = minio_client.list_objects(bucket_name=minio_bucket,
                                                  prefix=sub_folder,
                                                  recursive=True)
    else:
        list_of_files = minio_client.list_objects(bucket_name=minio_bucket, recursive=True)
    file_name_list = []
    for file_name in list_of_files:
        try:
            # Escape prefixes
            if not file_name.object_name.endswith(SEPARATOR_SYMBOL):
                path_list = file_name.object_name.split(SEPARATOR_SYMBOL)
                file_metadata = get_metadata_from_filename(path_list[-1])
                file_metadata[FULL_PATH_KEYWORD] = file_name.object_name
                file_name_list.append(file_metadata)
        except ValueError:
            continue
        except Exception as ex:
            logger.warning(f"Exception when parsing the filename: {ex}")
            continue
    exploded_results = pandas.DataFrame(file_name_list)
    return exploded_results


def get_boundary_data_from_minio(minio_bucket: str = 'opdm-data', minio_client: minio.ObjectStorage = None):
    """
    Searches given bucket for boundary data (ENTSOE files) takes the last entries by message types
    :param minio_bucket: bucket where to search from
    :param minio_client: instance on minio ObjectStorage if given
    :return boundary data
    """
    minio_client = minio_client or minio.ObjectStorage()
    boundary_value = {OPDE_COMPONENT_KEYWORD: []}
    file_list = get_filename_dataframe_from_minio(minio_bucket=minio_bucket,
                                                  minio_client=minio_client)
    boundary_list = file_list[file_list['Model.modelingEntity'] == 'ENTSOE']
    filtered = boundary_list.loc[boundary_list.groupby('Model.messageType')['Model.scenarioTime'].idxmax()]
    # Check if input is valid
    if len(filtered.index) != 2 or sorted(filtered['Model.messageType']) != ['EQBD', 'TPBD']:
        return None
    filtered_elements = filtered.to_dict('records')
    for opdm_profile_content in filtered_elements:
        downloaded_file = minio_client.download_object(bucket_name=minio_bucket,
                                                       object_name=opdm_profile_content[FULL_PATH_KEYWORD])
        opdm_profile_content[MODEL_MESSAGE_TYPE] = parse_boundary_message_type_profile(
            opdm_profile_content[MODEL_MESSAGE_TYPE])
        opdm_profile_content[DATA_KEYWORD] = downloaded_file
        opdm_profile_content.pop(FULL_PATH_KEYWORD)
        boundary_value[OPDE_COMPONENT_KEYWORD].append({OPDM_PROFILE_KEYWORD: opdm_profile_content})
    return boundary_value


def get_version_number_from_elastic(index_name: str = DEFAULT_INDEX_NAME,
                                    start_looking: datetime.datetime | str = datetime.datetime.today(),
                                    scenario_date: str = None,
                                    time_horizon: str = None,
                                    modeling_entity: str = None):
    """
    Checks and gets the version number from elastic
    Note that it works only if logger.info(f"Publishing {instance_file.name} to OPDM")
    is used when publishing files to OPDM
    :param index_name: index from where to search
    :param start_looking: datetime instance from where to look, if not set then takes current day
    :param scenario_date: filter the file names by scenario date
    :param time_horizon: filter file names by time horizon
    :param modeling_entity: filter file names by modeling entity
    :return version number as a string
    """
    must_elements = []
    query_part = {"query_string": {"default_field": "message", "query": "*Publishing* AND *to OPDM"}}
    must_elements.append(query_part)
    new_version_number = 1
    if start_looking:
        if isinstance(start_looking, datetime.datetime):
            start_looking = start_looking.strftime("%Y-%m-%dT%H:%M:%S")
        range_part = {"range": {"log_timestamp": {"gte": start_looking}}}
        must_elements.append(range_part)
    previous_cgm_query = {"bool": {"must": must_elements}}
    try:
        elastic_client = Elastic()
        results = elastic_client.get_data(index=index_name,
                                          query=previous_cgm_query,
                                          fields=['message'])
        if results.empty:
            raise NoContentFromElasticException
        # Get the filenames and explode them
        exploded_results = (results["message"].
                            str.removesuffix(' to OPDM').
                            str.removeprefix('Publishing ').
                            map(get_metadata_from_filename).
                            apply(pandas.Series))
        # Filter the results if needed
        new_version_number = get_largest_version_from_filename_dataframe(exploded_results=exploded_results,
                                                                         scenario_date=scenario_date,
                                                                         time_horizon=time_horizon,
                                                                         modeling_entity=modeling_entity)
    except (NoContentFromElasticException, KeyError):
        logger.info(f"No previous entries found starting with version number {new_version_number:03}")
    except Exception as ex:
        logger.warning(f"Got elastic error: {ex}, starting with version number {new_version_number:03}")
    finally:
        return f"{new_version_number:03}"


def get_largest_version_from_filename_dataframe(exploded_results: pandas.DataFrame,
                                                scenario_date: str = None,
                                                time_horizon: str = None,
                                                modeling_entity: str = None):
    """
    Searches largest version number from a dict. Optionally the dict can be filtered beforehand
    :param exploded_results: the dictionary containing exploded filenames (used get_metadata_from_filename)
    :param scenario_date: optionally filter filenames by scenario date
    :param time_horizon: optionally filter filenames by time horizon
    :param modeling_entity: optionally filter filenames by checking if modelling entity is in the field
    :return: largest found file number or 1 if key error or not found
    """
    try:
        if modeling_entity is not None:
            exploded_results = exploded_results[exploded_results['Model.modelingEntity'].str.contains(modeling_entity)]
        if scenario_date is not None:
            scenario_date = f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}"
            exploded_results = exploded_results[exploded_results['Model.scenarioTime'].str.contains(scenario_date)]
        if time_horizon is not None:
            exploded_results = exploded_results[exploded_results['Model.processType'].str.contains(time_horizon)]
        # Get the largest version number and increment it by 1
        new_version_number = max(pandas.to_numeric(exploded_results["Model.version"])) + 1
        logger.info(f"Continuing with version number {new_version_number:03}")
    except KeyError as key_error:
        logger.info(f"{key_error}")
        new_version_number = 1
    return new_version_number


class NoContentFromElasticException(Exception):
    pass


class CgmModelComposer:
    """
    Class for gathering the data and running the merge function (copy from merge.py)
    """

    def __init__(self,
                 igm_models=None,
                 boundary_data=None,
                 version: str = VERSION,
                 time_horizon: str = TIME_HORIZON,
                 area: str = AREA,
                 scenario_date: str = SCENARIO_DATE,
                 merging_entity: str = CGM_MERGING_ENTITY,
                 namespace_map=None,
                 rdf_map_loc: str = RDF_MAP_JSON,
                 rabbit_data: dict = None):
        """
        Constructor, note that data gathering and filtering must be done beforehand
        This just stores and merges
        :param igm_models: the individual grid models of the tso's
        :param boundary_data: the boundary data of the region
        :param version: the version number to use for the merged model
        :param time_horizon: the time horizon for the merge
        :param area: the area of the merge
        :param scenario_date: the date of the scenario
        :param merging_entity: the author of the merged model
        :param namespace_map:
        :param rdf_map_loc:
        :param rabbit_data:
        """
        if namespace_map is None:
            namespace_map = NAMESPACE_MAP
        self.igm_models = igm_models
        if self.igm_models is None:
            self.igm_models = []
        self.boundary_data = boundary_data
        self.sv_data = None
        self.ssh_data = None

        self.time_horizon = time_horizon
        self.area = area
        self.scenario_date = scenario_date

        self._version = version
        self.merging_entity = merging_entity
        self._merged_model = None
        self._opdm_data = None
        self._opdm_object_meta = None
        self.namespace_map = namespace_map
        self.cgm = None
        self.rdf_map = load_rdf_map(rdf_map_loc)
        self.rabbit_data = rabbit_data

    @property
    def merged_model(self):
        """
        Gets merged model
        """
        if self._merged_model is None and self.igm_models and self.boundary_data:
            self._merged_model = load_model(self.igm_models + [self.boundary_data])
            # Run LF
            loadflow_report = pypowsybl.report.Reporter()
            loadflow_result = pypowsybl.loadflow.run_ac(network=self.merged_model[NETWORK_KEYWORD],
                                                        parameters=loadflow_settings.CGM_DEFAULT,
                                                        reporter=loadflow_report)
            # TODO check and report loadflow result
        return self._merged_model

    @property
    def opdm_data(self):
        """
        Gets opdm data (igm models and boundary data combined)
        """
        if isinstance(self._opdm_data, pandas.DataFrame):
            return self._opdm_data
        if self.igm_models and self.boundary_data:
            self._opdm_data = load_opdm_data(self.igm_models + [self.boundary_data])
        return self._opdm_data

    @property
    def opdm_object_meta(self):
        """
        Gets base for opdm object meta
        """
        if self._opdm_object_meta is None and self.merged_model is not None:
            sv_id = self.merged_model[NETWORK_META_KEYWORD]['id'].split("uuid:")[-1]
            self._opdm_object_meta = {'pmd:fullModel_ID': sv_id,
                                      'pmd:creationDate': f"{datetime.datetime.utcnow():%Y-%m-%dT%H:%M:%S.%fZ}",
                                      'pmd:timeHorizon': self.time_horizon,
                                      'pmd:cgmesProfile': 'SV',
                                      'pmd:contentType': 'CGMES',
                                      'pmd:modelPartReference': '',
                                      'pmd:mergingEntity': f"{self.merging_entity}",
                                      'pmd:mergingArea': self.area,
                                      'pmd:validFrom': f"{parse_datetime(self.scenario_date):%Y%m%dT%H%MZ}",
                                      'pmd:modelingAuthoritySet': 'http://www.baltic-rsc.eu/OperationalPlanning',
                                      'pmd:scenarioDate': f"{parse_datetime(self.scenario_date):%Y-%m-%dT%H:%M:00Z}",
                                      'pmd:modelid': sv_id,
                                      'pmd:description': f"""<MDE>
                                    <BP>{self.time_horizon}</BP>
                                    <TOOL>pypowsybl_{pypowsybl.__version__}</TOOL>
                                    <RSC>{self.merging_entity}</RSC>
                                    </MDE>""",
                                      'pmd:versionNumber': self.version,
                                      'file_type': "xml"}
        return self._opdm_object_meta

    @property
    def version(self):
        """
        Gets version
        """
        return self._version

    def set_sv_file(self,
                    merged_model=None,
                    opdm_object_meta=None):
        merged_model = merged_model or self.merged_model
        opdm_object_meta = opdm_object_meta or self.opdm_object_meta
        # export_report = pypowsybl.report.Reporter()
        exported_model = export_model(merged_model[NETWORK_KEYWORD], opdm_object_meta, ["SV"])
        logger.info(f"Exporting merged model to {exported_model.name}")
        # Load SV data
        sv_data = pandas.read_RDF([exported_model])
        # Update SV filename
        sv_data.set_VALUE_at_KEY(key='label', value=filename_from_metadata(opdm_object_meta))
        # Update SV description
        sv_data.set_VALUE_at_KEY(key='Model.description', value=opdm_object_meta['pmd:description'])
        # Update SV created time
        sv_data.set_VALUE_at_KEY(key='Model.created', value=opdm_object_meta['pmd:creationDate'])
        # Update SSH Model.scenarioTime
        sv_data.set_VALUE_at_KEY('Model.scenarioTime', opdm_object_meta['pmd:scenarioDate'])
        # Update SV metadata
        sv_data = triplets.cgmes_tools.update_FullModel_from_filename(sv_data)
        self.sv_data = sv_data
        return sv_data, opdm_object_meta

    def set_ssh_files(self,
                      valid_models=None,
                      latest_boundary=None,
                      sv_data=None,
                      opdm_object_meta=None,
                      update_map=None):

        valid_models = valid_models or self.igm_models
        latest_boundary = latest_boundary or self.boundary_data
        sv_data = check_dataframe(sv_data, self.sv_data)
        opdm_object_meta = opdm_object_meta or self.opdm_object_meta
        update_map = update_map or UPDATE_MAP

        ssh_data = load_opdm_data(valid_models, "SSH")
        ssh_data = triplets.cgmes_tools.update_FullModel_from_filename(ssh_data)

        # Update SSH Model.scenarioTime
        ssh_data.set_VALUE_at_KEY('Model.scenarioTime', opdm_object_meta['pmd:scenarioDate'])

        # Load full original data to fix issues
        data = load_opdm_data(valid_models + [latest_boundary])
        terminals = data.type_tableview("Terminal")

        # Update SSH data from SV
        updated_ssh_data = ssh_data.copy()
        for update in update_map:
            source_data = sv_data.type_tableview(update['from_class']).reset_index(drop=True)

            # Merge with terminal, if needed
            if terminal_reference := \
                    [column_name if ".Terminal" in column_name else None for column_name in source_data.columns][0]:
                source_data = source_data.merge(terminals, left_on=terminal_reference, right_on='ID')
                logger.debug(f"Added Terminals to {update['from_class']}")

            updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(
                source_data.rename(columns={update['from_ID']: 'ID', update['from_attribute']: update['to_attribute']})[
                    ['ID', update['to_attribute']]].set_index('ID').tableview_to_triplet(), add=False)

        # Generate new UUID for updated SSH
        updated_ssh_id_map = {}
        for old_id in updated_ssh_data.query("KEY == 'Type' and VALUE == 'FullModel'").ID.unique():
            new_id = str(uuid4())
            updated_ssh_id_map[old_id] = new_id
            logger.info(f"Assigned new UUID for updated SSH: {old_id} -> {new_id}")

        # Update SSH ID-s
        updated_ssh_data = updated_ssh_data.replace(updated_ssh_id_map)

        # Update in SV SSH references
        sv_data = sv_data.replace(updated_ssh_id_map)

        # Add SSH supersedes reference to old SSH
        ssh_supersedes_data = pandas.DataFrame(
            [{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
        ssh_supersedes_data['INSTANCE_ID'] = updated_ssh_data.query("KEY == 'Type'").merge(ssh_supersedes_data.ID)[
            'INSTANCE_ID']
        updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(ssh_supersedes_data)

        # Update SSH metadata
        updated_ssh_data = triplets.cgmes_tools.update_FullModel_from_dict(updated_ssh_data, {
            "Model.version": opdm_object_meta['pmd:versionNumber'],
            "Model.created": opdm_object_meta['pmd:creationDate'],
            "Model.mergingEntity": opdm_object_meta['pmd:mergingEntity'],
            "Model.domain": opdm_object_meta['pmd:mergingArea']
        })
        self.ssh_data = updated_ssh_data
        self.sv_data = sv_data
        return updated_ssh_data, sv_data

    def set_cgm(self, updated_ssh_data=None,
                sv_data=None,
                valid_models=None,
                latest_boundary=None,
                opdm_object_meta=None,
                filename_mask: str = FILENAME_MASK,
                namespace_map=None):
        # Update SSH filenames
        updated_ssh_data = check_dataframe(updated_ssh_data, self.ssh_data)
        sv_data = check_dataframe(sv_data, self.sv_data)
        valid_models = valid_models or self.igm_models
        latest_boundary = latest_boundary or self.boundary_data
        opdm_object_meta = opdm_object_meta or self.opdm_object_meta
        namespace_map = namespace_map or NAMESPACE_MAP
        data = load_opdm_data(valid_models + [latest_boundary])
        updated_ssh_data = triplets.cgmes_tools.update_filename_from_FullModel(updated_ssh_data,
                                                                               filename_mask=filename_mask)

        # Update SV metadata
        sv_metadata = {"Model.version": opdm_object_meta['pmd:versionNumber'],
                       "Model.created": opdm_object_meta['pmd:creationDate']}
        sv_data = triplets.cgmes_tools.update_FullModel_from_dict(sv_data, sv_metadata)

        # Fix SV - Remove Shunt Sections for EQV Shunts
        equiv_shunt = data.query("KEY == 'Type' and VALUE == 'EquivalentShunt'")
        if len(equiv_shunt) > 0:
            shunts_to_remove = sv_data.merge(sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").
                                             merge(equiv_shunt.ID,
                                                   left_on='VALUE',
                                                   right_on="ID", how='inner',
                                                   suffixes=('', '_EQVShunt')).ID)
            if len(shunts_to_remove) > 0:
                logger.warning(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
                sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

        # Fix SV - add missing SV Tap Steps
        ssh_tap_steps = updated_ssh_data.query("KEY == 'TapChanger.step'")
        sv_tap_steps = sv_data.query("KEY == 'SvTapStep.TapChanger'")
        missing_sv_tap_steps = ssh_tap_steps.merge(sv_tap_steps[['VALUE']], left_on='ID', right_on="VALUE", how='left',
                                                   indicator=True, suffixes=('', '_SV')).query("_merge == 'left_only'")

        tap_steps_to_be_added = []
        sv_instance_id = sv_data.INSTANCE_ID.iloc[0]
        for tap_changer in missing_sv_tap_steps.itertuples():
            id_value = str(uuid4())
            logger.warning(f'Missing SvTapStep for {tap_changer.ID}, adding SvTapStep {id_value} '
                           f'and taking tap value {tap_changer.VALUE} from SSH')
            tap_steps_to_be_added.extend([
                (id_value, 'Type', 'SvTapStep', sv_instance_id),
                (id_value, 'SvTapStep.TapChanger', tap_changer.ID, sv_instance_id),
                (id_value, 'SvTapStep.position', tap_changer.VALUE, sv_instance_id),
            ])

        sv_data = pandas.concat(
            [sv_data, pandas.DataFrame(tap_steps_to_be_added, columns=['ID', 'KEY', 'VALUE', 'INSTANCE_ID'])],
            ignore_index=True)

        export = (pandas.concat([updated_ssh_data, sv_data], ignore_index=True).
                  export_to_cimxml(rdf_map=self.rdf_map,
                                   namespace_map=namespace_map,
                                   export_undefined=False,
                                   export_type="xml_per_instance_zip_per_xml",
                                   debug=False,
                                   export_to_memory=True))
        self.cgm = export
        return export

    def publish_cgm(self,
                    export=None,
                    opdm_client: opdm.OPDM = None,
                    store_to_local: bool = True):
        """
        Copied here from merge.py. Handlers use their own methods for posting the results
        """
        export = export or self.cgm
        opdm_publication_responses = []

        for instance_file in export:
            if opdm_client is not None:
                logger.info(f"Publishing {instance_file.name} to OPDM")
                publication_response = opdm_client.publication_request(instance_file, "CGMES")
                opdm_publication_responses.append({"name": instance_file.name, "response": publication_response})
            if store_to_local:
                some_path = './merged_examples/'
                if not os.path.exists(some_path):
                    os.makedirs(some_path)
                full_file_name = some_path + instance_file.name
                with open(full_file_name, 'wb') as f:
                    f.write(instance_file.getbuffer())

    def compose_cgm(self):
        """
        Composes the cgm
        """
        tsos = [model.get('pmd:TSO') for model in self.igm_models]
        logger.info(f"Merge at {self.scenario_date}, "
                    f"time horizon: {self.time_horizon}, "
                    f"version: {self.version}, "
                    f"area: {self.area}, "
                    f"tsos: {', '.join(tsos)}")
        self.set_sv_file()
        self.set_ssh_files()
        self.set_cgm()

    def get_consolidated_metadata(self, rabbit_data: dict = None, additional_fields: dict = None):
        """
        Combines existing metadata with rabbit data for reporting
        NOTE! Change this
        """
        if not rabbit_data:
            rabbit_data = self.rabbit_data
        consolidated_data = self.opdm_object_meta
        consolidated_data[PROCESS_ID_KEYWORD] = rabbit_data.get(PROCESS_ID_KEYWORD)
        consolidated_data[RUN_ID_KEYWORD] = rabbit_data.get(RUN_ID_KEYWORD)
        consolidated_data[JOB_ID_KEYWORD] = rabbit_data.get(JOB_ID_KEYWORD)
        if additional_fields:
            consolidated_data.update(additional_fields)
        return consolidated_data

    def get_folder_name(self):
        model_date = f"{parse_datetime(self.scenario_date):%Y%m%dT%H%MZ}"
        operator_name = '-'.join([self.merging_entity, self.area])
        folder_name = '_'.join([model_date, self.time_horizon, operator_name, self._version])
        return folder_name


def testing_ground():
    pass


if __name__ == '__main__':
    # Run the entire pipeline in functions
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    testing_time_horizon = '1D'
    # testing_scenario_date = "2024-04-05T08:30:00+00:00"
    testing_scenario_date = "2024-04-12T21:30:00+00:00"
    take_data_from_local = False
    # wanted_tsos = ['50Hertz', 'APG', 'D4', 'D7', 'ELES', 'ELIA', 'LITGRID', 'SEPS', 'TERNA', 'TTG']
    # wanted_tsos = ['50Hertz', 'SEPS', 'TERNA', 'TTG']
    wanted_tsos = []
    if take_data_from_local:
        folder_to_study = 'test_case'
        igm_model_data, latest_boundary_data = get_local_entsoe_files(path_to_directory=folder_to_study,
                                                                      allow_merging_entities=False,
                                                                      igm_files_needed=['EQ'])
        if wanted_tsos:
            igm_model_data = [model for model in igm_model_data if model.get('pmd:TSO') in wanted_tsos]
    else:
        igm_model_data = get_models_from_elastic_minio(time_horizon=testing_time_horizon,
                                                       scenario_date=testing_scenario_date,
                                                       tso_names=wanted_tsos)
        latest_boundary_data = get_boundary_from_dependencies(igm_models=igm_model_data)

    # files = get_boundary_data_from_minio()
    # version_number_elastic = get_version_number_from_elastic(start_looking="2024-04-01T00:00:00",
    #                                                          modeling_entity=f"{CGM_MERGING_ENTITY}-EU",
    #                                                          time_horizon='ID',
    #                                                          scenario_date='2024-04-04T05:30:00')
    # print(version_number_elastic)
    # version_number_minio = get_version_number_from_minio(minio_bucket=EMF_OS_MINIO_BUCKET,
    #                                                      sub_folder=EMF_OS_MINIO_FOLDER,
    #                                                      modeling_entity=f"{CGM_MERGING_ENTITY}-EU",
    #                                                      time_horizon='ID',
    #                                                      scenario_date='2024-04-04T05:30:00')
    # print(version_number_minio)

    if not igm_model_data or not latest_boundary_data:
        logger.error(f"Terminating")
        sys.exit()
    cgm_input = CgmModelComposer(igm_models=igm_model_data,
                                 boundary_data=latest_boundary_data,
                                 time_horizon=testing_time_horizon,
                                 scenario_date=testing_scenario_date,
                                 area='EU',
                                 merging_entity='BALTICRSC',
                                 version='104')
    cgm_input.compose_cgm()
    # received_models, received_boundary = get_local_models()
    # received_models, received_boundary = get_models()
    # received_models, received_boundary = get_models(time_horizon='1D',
    #                                                 scenario_date='2024-04-05T22:30:00+00:00')
    # checked_models = validate_models(available_models=received_models, latest_boundary=received_boundary)
    # cgm_composer = CgmModelComposer(igm_models=checked_models,
    #                                 boundary_data=received_boundary,
    #                                 version=version_number)
    # cgm_composer.compose_cgm()
    # cgm_composer.publish_cgm(store_to_local=True)
