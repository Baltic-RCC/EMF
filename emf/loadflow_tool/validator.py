import pypowsybl
from helper import attr_to_dict, load_model
import logging
import json
import loadflow_settings

logger = logging.getLogger(__name__)


# TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
# note - multiple islands wo load or generation can be an issue


def validate_model(opdm_objects, run_element_validations=True):
    # Load data
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
                                                parameters=loadflow_settings.IGM_VALIDATION,
                                                reporter=loadflow_report)

    loadflow_result_dict = [attr_to_dict(island) for island in loadflow_result]
    model_data["LOADFLOW_RESUTLS"] = loadflow_result_dict

    model_data["LOADFLOW_REPORT"] = json.loads(loadflow_report.to_json())
    model_data["LOADFLOW_REPORT_STR"] = str(loadflow_report)

    model_valid = any([True if island["status"].name == "CONVERGED" else False for island in loadflow_result_dict])

    model_data["VALID"] = model_valid

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

    opdm_client = OPDM()

    latest_boundary = opdm_client.get_latest_boundary()
    available_models = opdm_client.get_latest_models_and_download(time_horizon='1D', scenario_date="2023-07-04T09:30")#, tso="ELERING")

    valid_models = []
    invalid_models = []

    # Validate models
    for model in available_models:

        try:
            response = validate_model([model, latest_boundary])
            model["VALIDATION_STATUS"] = response
            if response["VALID"]:
                valid_models.append(model)
            else:
                invalid_models.append(model)

        except Exception as error:
            invalid_models.append(model)
            logger.error("Validation failed", error)






