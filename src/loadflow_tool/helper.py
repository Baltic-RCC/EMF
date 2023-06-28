from zipfile import ZipFile
from uuid import uuid4
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

# TODO - Addcomments and docstring
def package_for_pypowsybl(opdm_components):

    with ZipFile(f"{uuid4()}.zip", "w") as global_zip:
        logging.info(f"Adding files to {global_zip.filename}")

        for instance in opdm_components:
            with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                for file_name in instance_zip.namelist():
                    logging.info(f"Adding file {file_name}")
                    global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return global_zip.filename