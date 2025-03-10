import logging
import config
from io import BytesIO
from zipfile import ZipFile
from typing import List
import json
import time
import pypowsybl
from emf.loadflow_tool.helper import attr_to_dict, load_model
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, opdm, minio_api
from emf.common.integrations.object_storage import models
from emf.common.converters import opdm_metadata_to_json
from emf.loadflow_tool.helper import load_opdm_data
from emf.loadflow_tool import loadflow_settings

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_validator.model_validator)


def validate_model_new(opdm_objects: List[dict],
                       loadflow_parameters=getattr(loadflow_settings, VALIDATION_LOAD_FLOW_SETTINGS),
                       run_element_validations: bool = True):

    # Define placeholder for validation report data
    validation_report = {}

    # Load network model
    start_time = time.time()
    network = load_model(opdm_objects=opdm_objects)

    # Run all network element validations
    if run_element_validations:
        validations = list(set(attr_to_dict(pypowsybl._pypowsybl.ValidationType).keys()) - set(["ALL", "name", "value"]))
        _validation_status = {}
        for validation in validations:
            validation_type = getattr(pypowsybl._pypowsybl.ValidationType, validation)
            logger.info(f"Running validation: {validation_type}")
            try:
                # TODO figure out how to store full validation results if needed. Currently only status is taken
                _validation_status[validation] = pypowsybl.loadflow.run_validation(network=network,
                                                                                   validation_types=[validation_type])._valid.__bool__()
            except Exception as error:
                logger.warning(f"Failed {validation_type} validation with error: {error}")
                continue
        validation_report["validations"] = _validation_status

    # Validate loadflow convergence
    logger.info(f"Solving load flow with settings: {VALIDATION_LOAD_FLOW_SETTINGS}")
    loadflow_report = pypowsybl.report.Reporter()
    loadflow_result = pypowsybl.loadflow.run_ac(network=network,
                                                parameters=loadflow_parameters,
                                                reporter=loadflow_report)

    # Parsing loadflow results
    validation_report["islands"] = len(loadflow_result)
    loadflow_result_dict = {}
    for island in loadflow_result:
        island_results = attr_to_dict(island)
        island_results['status'] = island_results.get('status').name
        island_results['distributed_active_power'] = 0.0 if math.isnan(island_results['distributed_active_power']) else island_results['distributed_active_power']
        loadflow_result_dict[f"component_{island.connected_component_num}"] = island_results
    model_data["loadflow_results"] = loadflow_result_dict

    # Validation status and duration
    model_valid = any([True if val["status"] == "CONVERGED" else False for key, val in loadflow_result_dict.items()])

    validation_report["duration_s"] = round(time.time() - start_time, 3)
    logger.info(f"Load flow validation status: {model_valid} [duration {model_data['validation_duration_s']}s]")


def validate_model(opdm_objects: List[dict],
                   loadflow_parameters=getattr(loadflow_settings, VALIDATION_LOAD_FLOW_SETTINGS),
                   run_element_validations: bool = True):

    # Load network model
    start_time = time.time()
    network = load_model(opdm_objects=opdm_objects)

    # Pre check
    opdm_model_triplets = get_opdm_data_from_models(model_data=opdm_objects)
    not_retained_switches = 0
    kirchhoff_first_law_detected = False
    check_non_retained_switches_val = json.loads(CHECK_NON_RETAINED_SWITCHES.lower())
    check_kirchhoff_first_law_val = json.loads(CHECK_KIRCHHOFF_FIRST_LAW.lower())
    if check_non_retained_switches_val:
        not_retained_switches = check_not_retained_switches_between_nodes(opdm_model_triplets)[1]
    # violated_nodes_pre = get_nodes_against_kirchhoff_first_law(original_models=opdm_model_triplets)
    # kirchhoff_first_law_detected = False if violated_nodes_pre.empty else True
    # End of pre check

    # Run all validations
    if run_element_validations:
        validations = list(set(attr_to_dict(pypowsybl._pypowsybl.ValidationType).keys()) - set(["ALL", "name", "value"]))

        model_data["validations"] = {}

        for validation in validations:
            validation_type = getattr(pypowsybl._pypowsybl.ValidationType, validation)
            logger.info(f"Running validation: {validation_type}")
            try:
                # TODO figure out how to store full validation results if needed. Currently only status is taken
                model_data["validations"][validation] = pypowsybl.loadflow.run_validation(network=network,
                                                                                          validation_types=[validation_type])._valid.__bool__()
            except Exception as error:
                logger.warning(f"Failed {validation_type} validation with error: {error}")
                continue

    # Validate if loadflow can be run
    logger.info(f"Solving load flow")
    loadflow_report = pypowsybl.report.Reporter()
    loadflow_result = pypowsybl.loadflow.run_ac(network=network,
                                                parameters=loadflow_parameters,
                                                reporter=loadflow_report)

    violated_nodes_post = pandas.DataFrame()
    if check_kirchhoff_first_law_val:
        # Export sv profile and check it for Kirchhoff 1st law
        export_parameters = {"iidm.export.cgmes.profiles": 'SV',
                             "iidm.export.cgmes.naming-strategy": "cgmes-fix-all-invalid-ids"}
        bytes_object = network.save_to_binary_buffer(format="CGMES",
                                                     parameters=export_parameters)
        bytes_object.name = f"{uuid.uuid4()}.zip"
        # Load SV data
        sv_data = pandas.read_RDF([bytes_object])
        # Check violations after loadflow
        violated_nodes_post = get_nodes_against_kirchhoff_first_law(original_models=opdm_model_triplets,
                                                                    cgm_sv_data=sv_data,
                                                                    nodes_only=True,
                                                                    consider_sv_injection=True)
        kirchhoff_first_law_detected = kirchhoff_first_law_detected or (False if violated_nodes_post.empty else True)
        # End of post check

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

    if check_non_retained_switches_val and not_retained_switches > 0:
        logger.error(f"Non retained switches triggered")
        model_valid = False
        model_data["not_retained_switches_between_tn_present"] = not_retained_switches
    if check_kirchhoff_first_law_val and kirchhoff_first_law_detected:
        logger.error(f"Kirchhoff first law triggered")
        model_valid = False
        kirchhoff_string = violated_nodes_post.to_string(index=False, header=False)
        kirchhoff_string = kirchhoff_string.replace('\n', ', ')
        model_data["Kirchhoff_first_law_error"] = kirchhoff_string

    model_data["valid"] = model_valid
    model_data["validation_duration_s"] = round(time.time() - start_time, 3)
    logger.info(f"Load flow validation status: {model_valid} [duration {model_data['validation_duration_s']}s]")

    # Get outages of the model
    try:
        model_data['outages'] = get_model_outages(network)
    except Exception as e:
        logger.error(f'Failed to log model outages: {e}')

    try:
        model_metadata = next(d for d in opdm_objects if d.get('opde:Object-Type') == 'IGM')
    except:
        logger.error("Failed to get model metadata")
        model_metadata = {'pmd:scenarioDate': '', 'pmd:timeHorizon': '', 'pmd:versionNumber': ''}

    model_data['@scenario_timestamp'] = model_metadata['pmd:scenarioDate']
    model_data['@time_horizon'] = model_metadata['pmd:timeHorizon']
    model_data['@version'] = model_metadata['pmd:versionNumber']
    model_data['tso'] = model_metadata['pmd:TSO']

    # Pop out pypowsybl network object
    model_data.pop('network')

    # Send validation data to Elastic
    try:
        response = elastic.Elastic.send_to_elastic(index=VALIDATOR_ELK_INDEX, json_message=model_data)
    except Exception as error:
        logger.error(f"Validation report sending to Elastic failed: {error}")

    return model_data


class HandlerModelsValidator:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()

    def handle(self, message, **kwargs):

        # Load OPDM metadata objects from binary to json
        opdm_objects = json.loads(message)

        # Get network models data from object storage
        opdm_objects = [models.get_content(metadata=opdm_object) for opdm_object in opdm_objects]

        # Get the latest boundary set for validation
        latest_boundary = models.get_latest_boundary()

        logger.info(f"Validation parameters used: {VALIDATION_LOAD_FLOW_SETTINGS}")

        # Run network model validation
        for opdm_object in opdm_objects:
            try:
                response = validate_model(opdm_objects=[opdm_object, latest_boundary])
                opdm_object["valid"] = response["valid"]  # taking only relevant data from validation step
            except Exception as error:
                logger.error(f"Models validator failed with exception: {error}", exc_info=True)
                opdm_object["valid"] = False

        return opdm_objects