import logging
from io import BytesIO
import pandas as pd
import triplets
from lxml import etree
from emf.common.helpers.time import parse_datetime
from emf.common.helpers.utils import get_xml_from_zip
from emf.common.helpers.cgmes import get_metadata_from_rdfxml


logger = logging.getLogger(__name__)


def clean_data_from_opdm_objects(opdm_objects: list) -> list:
    for opdm_object in opdm_objects:
        for component in opdm_object['opde:Component']:
            component['opdm:Profile']['DATA'] = None

    return opdm_objects


def save_opdm_objects(opdm_objects: dict) -> list:
    """
    Function save OPDM objects on to local filesystem
    :param opdm_objects: list of OPDM objects
    :return: list of exported files
    """
    exported_files = []
    for opdm_components in opdm_objects:
        for instance in opdm_components['opde:Component']:
            file_name = instance['opdm:Profile']['pmd:fileName']
            logger.info(f'Saving: {file_name}')
            with open(file_name, 'wb') as instance_zip:
                instance_zip.write(instance['opdm:Profile']['DATA'])
            exported_files.append(file_name)

    return exported_files


def get_opdm_component_data_bytes(opdm_component: dict):
    data = BytesIO(opdm_component['opdm:Profile']['DATA'])
    data.name = opdm_component['opdm:Profile']['pmd:fileName']
    return data


def load_opdm_objects_to_triplets(opdm_objects: list[dict], profile: str | None = None):
    if profile:
        return pd.read_RDF([get_opdm_component_data_bytes(instance) for model in opdm_objects for instance in model['opde:Component'] if instance['opdm:Profile']['pmd:cgmesProfile'] == profile])
    return pd.read_RDF([get_opdm_component_data_bytes(instance) for model in opdm_objects for instance in model['opde:Component']])


def get_metadata_from_file_name(file_name: str, meta_separator: str = "_"):
    """
    Parse OPDM metadata from a filename string into a dictionary.

    Args:
        :param file_name: (str): The filename containing metadata separated by underscores
            and a file extension
        :param meta_separator: (str): How the elements are separated in the string, usually by "_"

    Returns:
        dict: Dictionary containing parsed metadata with OPDM-specific keys.

    Raises:
        AssertionError: If file_name is not a string.
        ValueError: If metadata parsing fails due to incorrect format.
    """
    # Constants for OPDM metadata keys
    VALID_FROM = 'pmd:validFrom'
    TIME_HORIZON = 'pmd:timeHorizon'
    CGMES_PROFILE = 'pmd:cgmesProfile'
    VERSION_NUMBER = 'pmd:versionNumber'
    MERGING_ENTITY = 'pmd:mergingEntity'
    MERGING_AREA = 'pmd:mergingArea'
    MODEL_PART = 'pmd:modelPartReference'
    TSO = "pmd:TSO"
    SOURCING_ACTOR = "pmd:sourcingActor"

    # Constant for file type
    FILE_TYPE = 'file_type'

    # Input validation
    assert isinstance(file_name, str), "file_name must be a string"

    metadata = {}  # Meta container

    # Split filename into name and extension
    file_name, metadata[FILE_TYPE] = file_name.split(".")
    meta_list = file_name.split(meta_separator)

    # Parse main metadata components
    if len(meta_list) == 4:
        metadata[VALID_FROM], model_authority, metadata[CGMES_PROFILE], metadata[VERSION_NUMBER] = meta_list
        metadata[TIME_HORIZON] = ""
    elif len(meta_list) == 5:
        metadata[VALID_FROM], metadata[TIME_HORIZON], model_authority, metadata[CGMES_PROFILE], metadata[VERSION_NUMBER] = meta_list
    else:
        logger.warning(f"Parsing error, number of allowed meta in filename is 4 or 5 separated by {meta_separator}: {file_name}")
        return metadata

    # Parse model authority components
    model_authority_list = model_authority.split("-")

    if len(model_authority_list) == 1:
        metadata[MODEL_PART] = model_authority
    elif len(model_authority_list) == 2:
        metadata[MERGING_ENTITY], metadata[MERGING_AREA] = model_authority_list
    elif len(model_authority_list) == 3:
        metadata[MERGING_ENTITY], metadata[MERGING_AREA], metadata[MODEL_PART] = model_authority_list
    else:
        logger.error(f"Parsing error {model_authority}")

    # Add aliases
    if metadata.get(MODEL_PART):
        metadata[TSO] = metadata[MODEL_PART]
        metadata[SOURCING_ACTOR] = metadata[MODEL_PART]

    return metadata


def generate_opdm_object_content_reference_from_filename(file_name: str, opdm_object_type: str = "CGMES"):
    """
    Generates the file path based on the given parameters and metadata extracted from the filename.

    Parameters:
    file_name (str): The name of the file from which metadata will be extracted.
    opdm_object_type (str): The type of OPDM object, defaults to "CGMES".

    Returns:
    str: Generated file path using the provided template and extracted metadata.
    """
    template = "{opdm_object_type}/{processType}/{modelingEntity}/{scenarioTime:%Y%m%d}/{scenarioTime:%H%M00}/{messageType}/{file_name}"

    meta = {key.split(".")[-1]: value for key, value in get_metadata_from_file_name(file_name).items()}
    meta["scenarioTime"] = parse_datetime(meta["scenarioTime"])
    meta["file_name"] = file_name
    meta["opdm_object_type"] = opdm_object_type

    return template.format(**meta)


def get_opdm_metadata_from_rdfxml(parsed_xml: etree._ElementTree):
    opdm_metadata_map = {
        "Model.mRID": ["opde:Id", "pmd:modelid", "pmd:fullModel_ID"],
        "Model.scenarioTime": ["pmd:scenarioDate"],
        "Model.created": ["pmd:creationDate"],
        "Model.description": ["pmd:description"],
        "Model.version": ["pmd:version"],
        "Model.DependentOn": ["opde:DependsOn"],
        "Model.profile": ["pmd:modelProfile"],
        "Model.modelingAuthoritySet": ["pmd:modelingAuthoritySet"],

    }

    metadata = {}
    for key, value in get_metadata_from_rdfxml(parsed_xml).items():
        if opdm_keys := opdm_metadata_map.get(key):
            for opdm_key in opdm_keys:
                metadata[opdm_key] = value

    return metadata


def create_opdm_objects(models: list, metadata: dict | None = None, key_profile: str = "SV") -> list:
    """
    Function to create OPDM object like structure in memory
    input models, is a nested list [[EQ, SSH, TP, SV], [EQ, SSH, TP, SC]] where upper list will become opdm:Object
    and nested will be opdm:Profile

    :return: list of OPDM objects
    """
    opdm_objects = []

    for model in models:
        opdm_object = {'opde:Component': []}

        for profile_instance in model:

            # Build profile instance metadata
            opdm_profile = get_metadata_from_file_name(profile_instance.name)
            opdm_profile.update(get_opdm_metadata_from_rdfxml(get_xml_from_zip(profile_instance)))
            opdm_profile['pmd:fileName'] = profile_instance.name
            opdm_profile["pmd:content-reference"] = generate_opdm_object_content_reference_from_filename(profile_instance.name)

            # Check if key profile and add to main object metadata
            if opdm_profile.get('pmd:cgmesProfile') == key_profile:
                opdm_object.update(opdm_profile)

            # Add model type - IGM/CGM
            # TODO develop logic to also define as CGM metadata object
            opdm_object['opde:Object-Type'] = "IGM"

            # Add DATA
            opdm_profile['DATA'] = profile_instance.getvalue()

            # Add component to main object
            opdm_object['opde:Component'].append({'opdm:Profile': opdm_profile})

        # If metadata provided, overwrite existing
        if metadata:
            opdm_object.update(metadata)

        opdm_objects.append(opdm_object)

    return opdm_objects


def filename_from_opdm_metadata(metadata: dict, file_type: str | None = None):
    model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:Area']}"
    file_name = f"{metadata['pmd:validFrom']}_{metadata['pmd:timeHorizon']}_{model_authority}_{metadata['pmd:cgmesProfile']}_{metadata['pmd:versionNumber']}"

    if file_type:
        file_name = f"{file_name}.{file_type}"

    return file_name


def filename_reduced_from_opdm_metadata(metadata: dict, file_type: str | None = None):
    valid_from = f"{metadata['pmd:validFrom']}"
    time_horizon = f"{metadata['pmd:timeHorizon']}"
    model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:Area']}"
    profile = f"{metadata['pmd:cgmesProfile']}"
    version_number = f"{metadata['pmd:versionNumber']}"
    file_name_reduced = '_'.join([valid_from, time_horizon, model_authority])
    # file_name = f"{metadata['pmd:validFrom']}_{metadata['pmd:timeHorizon']}_{model_authority}_{metadata['pmd:cgmesProfile']}_{metadata['pmd:versionNumber']}"
    #
    # if file_type:
    #     file_name = f"{file_name}.{file_type}"
    return file_name_reduced, version_number

if __name__ == "__main__":
    # Create OPDM objects
    opdm_object = create_opdm_objects([rdfzip_files])