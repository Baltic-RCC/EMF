import pypowsybl
from helper import attr_to_dict, load_model
import logging
import json
import loadflow_settings
import time

logger = logging.getLogger(__name__)


# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue


def validate_model(opdm_objects, loadflow_parameters=loadflow_settings.CGM_RELAXED_2, run_element_validations=True):
    # Load data
    start_time = time.time()
    model_data = load_model(opdm_objects)
    network = model_data["NETWORK"]

    # Run all validations except SHUNTS, that does not work on pypowsybl 0.24.0
    if run_element_validations:
        validations = list(
            set(attr_to_dict(pypowsybl._pypowsybl.ValidationType).keys()) - set(["ALL", "name", "value", "SHUNTS"]))

        model_data["VALIDATIONS"] = {}

        for validation in validations:
            validation_type = getattr(pypowsybl._pypowsybl.ValidationType, validation)
            model_data["VALIDATIONS"][validation] = pypowsybl.loadflow.run_validation(network,
                                                                                      [validation_type])

    # Validate if PF can be run
    loadflow_report = pypowsybl.report.Reporter()
    loadflow_result = pypowsybl.loadflow.run_ac(network=network,
                                                parameters=loadflow_parameters,
                                                reporter=loadflow_report)

    loadflow_result_dict = [attr_to_dict(island) for island in loadflow_result]
    model_data["LOADFLOW_RESUTLS"] = loadflow_result_dict

    model_data["LOADFLOW_REPORT"] = json.loads(loadflow_report.to_json())
    model_data["LOADFLOW_REPORT_STR"] = str(loadflow_report)

    model_valid = any([True if island["status"].name == "CONVERGED" else False for island in loadflow_result_dict])

    model_data["VALID"] = model_valid

    model_data["VALIDATION_DURATION_S"] = time.time() - start_time

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
    [print(dict(tso=model['opdm:OPDMObject']['pmd:TSO'], valid=model.get('VALIDATION_STATUS', {}).get('VALID'), duration=model.get('VALIDATION_STATUS', {}).get('VALIDATION_DURATION_S'))) for model in validated_models]

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



