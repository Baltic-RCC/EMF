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

parse_app_properties(caller_globals=globals(), path=config.paths.model_validator.model_validator)

# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue





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
    available_models = opdm.get_latest_models_and_download(time_horizon='1D',
                                                           scenario_date="2025-01-01T09:30",
                                                           tso="AST")

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



