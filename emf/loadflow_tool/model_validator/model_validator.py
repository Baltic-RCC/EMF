import logging
import pandas as pd
import config
import json
import time
import math
import pypowsybl as pp
import uuid
from emf.loadflow_tool.helper import attr_to_dict, load_model, get_model_outages
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, minio_api
from emf.common.integrations.object_storage import models
from emf.loadflow_tool.helper import load_opdm_data
from emf.loadflow_tool import loadflow_settings
from emf.loadflow_tool.model_validator import validator_functions

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_validator.model_validator)


class PreLFValidator:

    def __init__(self, network: pd.DataFrame):
        self.network = network
        self.report = {'pre_validations': {}}

    def validate_non_retained_switches(self):
        # TODO Use carefully as this might change switch statuses in the original model
        # TODO Currently disabled returning modified data
        non_retained_switched_valid = True
        non_retained_switches = validator_functions.check_not_retained_switches_between_nodes(self.network)[1]
        if non_retained_switches:
            non_retained_switched_valid = False
        self.report['pre_validations']['non_retained_switches'] = non_retained_switched_valid

    def validate_kirchhoff_first_law(self):
        violated_nodes = validator_functions.get_nodes_against_kirchhoff_first_law(original_models=self.network)
        kirchhoff_first_law_valid = True if violated_nodes.empty else False
        self.report['pre_validations']['kirchhoff_first_law'] = kirchhoff_first_law_valid

    def run_validation(self):
        if json.loads(CHECK_NON_RETAINED_SWITCHES.lower()):
            self.validate_non_retained_switches()
        if json.loads(CHECK_KIRCHHOFF_FIRST_LAW.lower()):
            self.validate_kirchhoff_first_law()


class PostLFValidator:

    def __init__(self, network: pp.network, network_triplets: pd.DataFrame):
        self.network = network
        self.network_triplets = network_triplets
        self.loadflow_parameters = getattr(loadflow_settings, VALIDATION_LOAD_FLOW_SETTINGS)
        self.report = {'validations': {}}

    def validate_loadflow(self):
        """Validate load flow convergence"""
        logger.info(f"Solving load flow with settings: {VALIDATION_LOAD_FLOW_SETTINGS}")
        loadflow_report = pp.report.Reporter()
        loadflow_result = pp.loadflow.run_ac(network=self.network,
                                             parameters=self.loadflow_parameters,
                                             reporter=loadflow_report)

        # Parsing aggregated results
        self.report['components'] = len(loadflow_result)
        self.report['solved_components'] = len(loadflow_result)  # TODO
        self.report['converged_components'] = len([res for res in loadflow_result if res.status_text == 'Converged'])

        # Components results
        # TODO currently storing only main island results
        main_component = loadflow_result[0]
        component_results = attr_to_dict(main_component)
        component_results['status'] = component_results.get('status').value
        component_results['distributed_active_power'] = 0.0 if math.isnan(component_results['distributed_active_power'])\
            else component_results['distributed_active_power']
        self.report['loadflow'] = component_results

        # Validation status
        self.report['loadflow_parameters'] = VALIDATION_LOAD_FLOW_SETTINGS
        self.report['validations']['loadflow'] = True if main_component.status.value == 0 else False

    def validate_network_elements(self):
        """Run all network element validations"""
        validations = list(set(attr_to_dict(pp._pypowsybl.ValidationType).keys()) - set(["ALL", "name", "value"]))
        _status = {}
        for validation in validations:
            validation_type = getattr(pp._pypowsybl.ValidationType, validation)
            logger.info(f"Running validation: {validation_type}")
            try:
                # TODO figure out how to store full validation results if needed. Currently only status is taken
                _status[validation] = pp.loadflow.run_validation(network=self.network,
                                                                 validation_types=[validation_type]).valid.__bool__()
            except Exception as error:
                logger.warning(f"Failed {validation_type} validation with error: {error}")
                continue
        self.report['element_validation'] = _status

    def validate_kirchhoff_first_law(self):
        """Validates possible Kirchhoff first law errors after loadflow"""
        violated_nodes = pd.DataFrame()

        # Export sv profile and check it for Kirchhoff 1st law
        export_parameters = {"iidm.export.cgmes.profiles": 'SV',
                             "iidm.export.cgmes.naming-strategy": "cgmes-fix-all-invalid-ids"}
        bytes_object = self.network.save_to_binary_buffer(format="CGMES", parameters=export_parameters)
        bytes_object.name = f"{uuid.uuid4()}.zip"

        # Load SV data
        sv_data = pd.read_RDF([bytes_object])

        # Check violations after loadflow
        violated_nodes = validator_functions.get_nodes_against_kirchhoff_first_law(original_models=self.network_triplets,
                                                                                   cgm_sv_data=sv_data,
                                                                                   nodes_only=True,
                                                                                   consider_sv_injection=True)
        kirchhoff_first_law_valid = True if violated_nodes.empty else False
        self.report['validations']['kirchhoff_first_law'] = kirchhoff_first_law_valid

    def run_validation(self):
        self.validate_loadflow()
        self.validate_network_elements()
        if json.loads(CHECK_KIRCHHOFF_FIRST_LAW.lower()):
            self.validate_kirchhoff_first_law()


class HandlerModelsValidator:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()
        self.elastic_service = elastic.Elastic()

    def update_opdm_metadata_object(self, id: str, body: dict):
        search_query = {"ids": {"values": [id]}}
        response = self.elastic_service.client.search(index=f"{METADATA_ELK_INDEX}*", query=search_query, size=1)
        index = response['hits']['hits'][0]['_index']
        self.elastic_service.update_document(index=index, id=id, body=body)

    def handle(self, message: bytes, properties: dict, **kwargs):

        start_time = time.time()

        # Load OPDM metadata objects from binary to json
        opdm_objects = json.loads(message)

        # Get network models data from object storage
        opdm_objects = [models.get_content(metadata=opdm_object) for opdm_object in opdm_objects]

        # Get the latest boundary set for validation
        latest_boundary = models.get_latest_boundary()

        logger.info(f"Validation parameters used: {VALIDATION_LOAD_FLOW_SETTINGS}")

        # Run network model validations
        for opdm_object in opdm_objects:
            report = {}
            try:
                # Run pre-loadflow validations
                network_triplets = load_opdm_data(opdm_objects=[opdm_object, latest_boundary])
                pre_lf_validation = PreLFValidator(network=network_triplets)
                pre_lf_validation.run_validation()

                # Run post-loadflow validations
                network = load_model(opdm_objects=[opdm_object, latest_boundary])
                post_lf_validation = PostLFValidator(network=network, network_triplets=network_triplets)
                post_lf_validation.run_validation()
                model_outages = get_model_outages(network)

                # Collect both pre and post loadflow validation reports and merge
                report.update(pre_lf_validation.report)
                report.update(post_lf_validation.report)
                report.update(model_outages)

                # Include relevant metadata fields
                report['@scenario_timestamp'] = opdm_object['pmd:scenarioDate']
                report['@time_horizon'] = opdm_object['pmd:timeHorizon']
                report['@version'] = int(opdm_object['pmd:versionNumber'])
                report['content_reference'] = opdm_object['pmd:content-reference']
                report['tso'] = opdm_object['pmd:TSO']
                report['duration_s'] = round(time.time() - start_time, 3)

            except Exception as error:
                logger.error(f"Models validator failed with exception: {error}", exc_info=True)

            # Define model validity
            valid = all(report['validations'].values())

            # Update OPDM metadata object with validity status
            try:
                logger.info("Updating OPDM metadata in Elastic with model valid status")
                self.update_opdm_metadata_object(id=opdm_object['opde:Id'], body={'valid': valid})
            except Exception as error:
                logger.error(f"Updated OPDM metadata object failed: {error}")

            # Send validation report to Elastic
            try:
                response = self.elastic_service.send_to_elastic(index=VALIDATION_ELK_INDEX, json_message=report)
            except Exception as error:
                logger.error(f"Validation report sending to Elastic failed: {error}")

            logger.info(f"Model validation status: {valid} [duration {report['duration_s']}s]")

        return message, properties


if __name__ == "__main__":
    # TESTING
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
    available_models = opdm.get_latest_models_and_download(time_horizon='1D',
                                                           scenario_date="2025-01-01T09:30",
                                                           tso="AST")
    validated_models = []

    # Validate models
    for model in available_models:
        network_triplets = load_opdm_data(opdm_objects=[opdm_object, latest_boundary])
        network = load_model(opdm_objects=[model, latest_boundary])
        post_lf_validation = PostLFValidator(network=network, network_triplets=network_triplets)
        post_lf_validation.run_validation()

        model["validation_report"] = post_lf_validation.report
        validated_models.append(model)

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
