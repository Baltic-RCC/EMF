from zipfile import ZipFile
from uuid import uuid4
from io import BytesIO
import logging


logger = logging.getLogger(__name__)

# TODO - Add comments and docstring
def package_for_pypowsybl(opdm_components):
    """
    Method to transform OPDM components into sufficient format zip package
    :param opdm_components:
    :return: zip package file name
    """
    with ZipFile(f"{uuid4()}.zip", "w") as global_zip:
        logging.info(f"Adding files to {global_zip.filename}")

        for instance in opdm_components:
            with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                for file_name in instance_zip.namelist():
                    logging.info(f"Adding file {file_name}")
                    global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return global_zip.filename


def pp_object_attr_parser(pp_object):
    """
    Method to get class variables in dictionary from powsybl object
    Example: LimitViolation(subject_id='e49a61d1-632a-11ec-8166-00505691de36', subject_name='', limit_type=HIGH_VOLTAGE, limit=450.0, limit_name='', acceptable_duration=2147483647, limit_reduction=1.0, value=555.6890952917897, side=ONE)
    pypowsybl._pypowsybl.LimitViolation -> dict
    :param pp_object: powsybl object
    :return: dict
    """
    attribs = [attr for attr in dir(pp_object) if not inspect.ismethod(attr) and not attr.startswith("__")]
    result_dict = {attr_key: pp_object.__getattribute__(attr_key) for attr_key in attribs}

    return result_dict