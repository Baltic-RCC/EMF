import uuid

import pandas
import pypowsybl
import logging
import json
import time
import math
import config
from emf.loadflow_tool import loadflow_settings
from emf.loadflow_tool.helper import attr_to_dict, load_model, get_model_outages
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic
from emf.loadflow_tool.model_merger.merge_functions import get_opdm_data_from_models
from emf.loadflow_tool.model_validator.validator_functions import check_not_retained_switches_between_nodes, \
    get_nodes_against_kirchhoff_first_law

# Initialize custom logger
# custom_logger.initialize_custom_logger(extra={'worker': 'model-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.validator)

# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue


def validate_model(opdm_objects, loadflow_parameters=getattr(loadflow_settings, VALIDATION_LOAD_FLOW_SETTINGS), run_element_validations=True):
    # Load data
    start_time = time.time()
    model_data = load_model(opdm_objects=opdm_objects)
    network = model_data["network"]

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

    # Pop out pypowsybl network object
    model_data.pop('network')

    # Send validation data to Elastic
    try:
        response = elastic.Elastic.send_to_elastic(index=VALIDATOR_ELK_INDEX, json_message=model_data)
    except Exception as error:
        logger.error(f"Validation report sending to Elastic failed: {error}")

    return model_data


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

    latest_boundary = opdm.get_latest_boundary()
    available_models = opdm.get_latest_models_and_download(time_horizon='1D', scenario_date="2023-08-16T09:30")#, tso="ELERING")

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



