from zipfile import ZipFile
from uuid import uuid4
from io import BytesIO
from inspect import ismethod
import pypowsybl
import json
import logging


logger = logging.getLogger(__name__)

# TODO - Add comments and docstring
def package_for_pypowsybl(opdm_objects: list, return_zip: bool = False):
    """
    Method to transform OPDM objects into sufficient format binary buffer or zip package
    :param opdm_objects: list of OPDM objects
    :param return_zip: flag to save OPDM objects as zip package in local directory
    :return: binary buffer or zip package file name
    """
    output_object = BytesIO()
    if return_zip:
        output_object = f"{uuid4()}.zip"
        logging.info(f"Adding files to {output_object}")

    with ZipFile(output_object, "w") as global_zip:
        for opdm_components in opdm_objects:
            for instance in opdm_components['opdm:OPDMObject']['opde:Component']:
                with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                    for file_name in instance_zip.namelist():
                        logging.info(f"Adding file: {file_name}")
                        global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return output_object


def attr_to_dict(object):
    """
    Method to return class variables/attributes as dictionary
    Example: LimitViolation(subject_id='e49a61d1-632a-11ec-8166-00505691de36', subject_name='', limit_type=HIGH_VOLTAGE, limit=450.0, limit_name='', acceptable_duration=2147483647, limit_reduction=1.0, value=555.6890952917897, side=ONE)
    pypowsybl._pypowsybl.LimitViolation -> dict
    :param object: object
    :return: dict
    """

    attribs = [attr for attr in dir(object) if (not ismethod(getattr(object, attr)) and not attr.startswith("_"))]
    result_dict = {attr_key: getattr(object, attr_key) for attr_key in attribs}

    return result_dict


def load_model(opdm_objects):

    model_data = {} #"model_meta": opdm_objects}

    import_report = pypowsybl.report.Reporter()

    network = pypowsybl.network.load_from_binary_buffer(package_for_pypowsybl(opdm_objects),
                                     reporter=import_report,
#                                     parameters={"iidm.import.cgmes.store-cgmes-model-as-network-extension": True,
#                                                 "iidm.import.cgmes.create-active-power-control-extension": True,
#                                                 "iidm.import.cgmes.post-processors": ["EntsoeCategory"]}
                                     )

    logger.info(f"Loaded {network}")
    logger.debug(f'{import_report}')

    model_data["NETWORK_META"] = attr_to_dict(network)
    model_data["NETWORK"] = network
    model_data["NETWORK_VALID"] = network.validate().name

    model_data["IMPORT_REPORT"] = json.loads(import_report.to_json())
    model_data["IMPORT_REPORT_STR"] = str(import_report)

    return model_data
