from zipfile import ZipFile, ZIP_DEFLATED
from uuid import uuid4
from io import BytesIO
from inspect import ismethod
from typing import List
from datetime import datetime
import pypowsybl
import json
import logging
import pandas
import os
from lxml import etree
import triplets
import uuid
from aniso8601 import parse_datetime
import re

logger = logging.getLogger(__name__)

powsybl_default_export_settings = {
    "iidm.export.cgmes.base-name": "",
    "iidm.export.cgmes.cim-version": "",  # 14, 16, 100
    "iidm.export.cgmes.export-boundary-power-flows": "true",
    "iidm.export.cgmes.export-power-flows-for-switches": "true",
    "iidm.export.cgmes.naming-strategy": "identity",  # identity, cgmes, cgmes-fix-all-invalid-ids
    "iidm.export.cgmes.profiles": "EQ,TP,SSH,SV",
    "iidm.export.cgmes.boundary-EQ-identifier": "",
    "iidm.export.cgmes.boundary-TP-identifier": "",
    "iidm.export.cgmes.modeling-authority-set": "powsybl.org"
}


# TODO - Add comments and docstring
def package_for_pypowsybl(opdm_objects, return_zip: bool = False):
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
            for instance in opdm_components['opde:Component']:
                with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                    for file_name in instance_zip.namelist():
                        logging.info(f"Adding file: {file_name}")
                        global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return output_object


def save_opdm_objects(opdm_objects: list) -> list:
    """
    Function save OPDM objects on to local filesystem
    :param opdm_objects: list of OPDM objects
    :return: list of exported files
    """
    exported_files = []
    for opdm_components in opdm_objects:
        for instance in opdm_components['opde:Component']:
            file_name = instance['opdm:Profile']['pmd:fileName']
            logger.info(f'Saving - {file_name}')
            with open(file_name, 'wb') as instance_zip:
                instance_zip.write(instance['opdm:Profile']['DATA'])
            exported_files.append(file_name)

    return exported_files


def create_opdm_objects(models: list, metadata=None, key_profile="SV") -> list:
    """
    Function to create OPDM object like structure in memory
    input models, is a nested list [[EQ, SSH, TP, SV], [EQ, SSH, TP, SC]] where upper list will become opdm:Object and nested will be opdm:Profile

    :return: list of OPDM objects
    """
    opdm_objects = []

    for model in models:
        opdm_object = {'opde:Component': []}

        for profile_instance in model:

            # Build profile instance metadata
            opdm_profile = opdm_metadata_from_filename(profile_instance.name)
            opdm_profile.update(opdm_metadata_from_rdfxml(get_xml_from_zip(profile_instance)))
            opdm_profile['pmd:fileName'] = profile_instance.name
            opdm_profile["pmd:content-reference"] = generate_OPDM_ContentReference_from_filename(profile_instance.name)

            # Check if key profile and add to main object metadata
            if opdm_profile.get('pmd:cgmesProfile') == key_profile:
               opdm_object.update(opdm_profile)

            # Add DATA
            opdm_profile['DATA'] = profile_instance.getvalue()

            # Add component to main object
            opdm_object['opde:Component'].append({'opdm:Profile': opdm_profile})

        # If metadata provided, overwrite existing
        if metadata:
            opdm_object.update(metadata)

        opdm_objects.append(opdm_object)

    return opdm_objects


def attr_to_dict(instance: object, sanitize_to_strings: bool = False):
    """
    Method to return class variables/attributes as dictionary
    Example: LimitViolation(subject_id='e49a61d1-632a-11ec-8166-00505691de36', subject_name='', limit_type=HIGH_VOLTAGE, limit=450.0, limit_name='', acceptable_duration=2147483647, limit_reduction=1.0, value=555.6890952917897, side=ONE)
    pypowsybl._pypowsybl.LimitViolation -> dict
    :param instance: class instance
    :param sanitize_to_strings: flag to convert attributes to string type
    :return: dict
    """

    def convert_value(value):
        if isinstance(value, datetime):
            return value.isoformat() if sanitize_to_strings else value
        elif isinstance(value, list):
            return [convert_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: convert_value(v) for k, v in value.items()}
        elif hasattr(value, '__dict__'):
            return attr_to_dict(value, sanitize_to_strings)
        elif sanitize_to_strings:
            return str(value)
        return value

    attribs = [attr for attr in dir(instance) if (not callable(getattr(instance, attr)) and not attr.startswith("_"))]
    result_dict = {attr_key: convert_value(getattr(instance, attr_key)) for attr_key in attribs}

    return result_dict


def parse_pypowsybl_report(report: str):
    lines = report.replace('+', '').splitlines()
    all_network_dicts = []

    current_dict = None
    base_indent = None

    for line in lines:
        stripped_line = line.strip()

        # Identify "Network info" line and its indentation level
        if "Network info" in stripped_line:
            if current_dict is not None:
                # Save the current dictionary if a new "Network info" block starts
                all_network_dicts.append(current_dict)

            current_dict = {}
            base_indent = len(line) - len(stripped_line)
            continue

        if current_dict is not None:
            # Calculate the current line's indentation level relative to "Network info"
            current_indent = len(line) - len(line.lstrip())

            # Check for the specific phrase "Network has x buses and y branches"
            match = re.match(r"Network has (\d+) buses and (\d+) branches", stripped_line)
            if match:
                buses = int(match.group(1))
                branches = int(match.group(2))
                current_dict['buses'] = buses
                current_dict['branches'] = branches

            # Process lines with key-value pairs after ':'
            elif ':' in stripped_line:
                dict_name, key_values = stripped_line.split(':', 1)
                dict_name = dict_name.strip()
                key_values = key_values.strip()

                # Parse key-value pairs
                if '=' in key_values:
                    current_dict[dict_name] = {}
                    for pair in key_values.split(','):
                        key, value = map(str.strip, pair.split('='))
                        current_dict[dict_name][key] = value
                else:
                    # Handle plain strings after ':'
                    current_dict[dict_name] = key_values

            else:
                # Stop processing this block if indentation level is not greater than base_indent
                if current_indent <= base_indent and current_dict:
                    all_network_dicts.append(current_dict)
                    current_dict = None

    # Append the last dictionary if it exists
    if current_dict is not None and current_dict:
        all_network_dicts.append(current_dict)

    # Filter out empty dicts
    result = [n for n in all_network_dicts if n]

    return result


def get_network_elements(network: pypowsybl.network,
                         element_type: pypowsybl.network.ElementType,
                         all_attributes: bool = True,
                         attributes: List[str] = None,
                         **kwargs
                         ):

    _voltage_levels = network.get_voltage_levels(all_attributes=True).rename(columns={"name": "voltage_level_name"})
    _substations = network.get_substations(all_attributes=True).rename(columns={"name": "substation_name"})

    elements = network.get_elements(element_type=element_type, all_attributes=all_attributes, attributes=attributes, **kwargs)
    elements = elements.merge(_voltage_levels, left_on='voltage_level_id', right_index=True, suffixes=(None, '_voltage_level'))
    elements = elements.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))

    # Need to ensure that column 'isHvdc' is present if DANGLING_LINE type is requested
    if element_type is pypowsybl.network.ElementType.DANGLING_LINE:
        if 'isHvdc' not in elements.columns:
            elements['isHvdc'] = ''

    return elements


def get_slack_generators(network: pypowsybl.network):

    slack_terminals = network.get_extension('slackTerminal')
    slack_generators = get_network_elements(network=network,
                                            element_type=pypowsybl.network.ElementType.GENERATOR,
                                            all_attributes=True,
                                            id=slack_terminals['element_id'])

    return slack_generators


def get_connected_components_data(network: pypowsybl.network,
                                  bus_count_threshold: int | None = None,
                                  country_col_name: str = 'country'):
    buses = get_network_elements(network, pypowsybl.network.ElementType.BUS)
    data = buses.groupby('connected_component').agg(countries=(country_col_name, lambda x: list(x.unique())),
                                                    bus_count=('name', 'size'))
    if bus_count_threshold:
        data = data[data.bus_count > bus_count_threshold]

    return data.to_dict('index')


def load_model(opdm_objects: List[dict], parameters: dict = None, skip_default_parameters: bool = False):
    """
    Loads given list of models (opdm_objects) into pypowsybl using internal (known good) default_parameters
    Additional parameters can be specified as a dict in field parameters which will overwrite the default ones if keys
    are matching
    :param opdm_objects: list of dictionaries following the opdm model format
    :param parameters: dictionary of desired parameters for loading models to pypowsybl
    :param skip_default_parameters: skip the default parameters
    """
    default_parameters = {"iidm.import.cgmes.import-node-breaker-as-bus-breaker": 'true'}
    if not skip_default_parameters:
        if not parameters:
            parameters = default_parameters
        else:
            # Give a priority to parameters given from outside
            parameters = {**default_parameters, **parameters}

    import_report = pypowsybl.report.Reporter()
    network = pypowsybl.network.load_from_binary_buffer(
        buffer=package_for_pypowsybl(opdm_objects),
        reporter=import_report,
        parameters=parameters
        # parameters={
        #     "iidm.import.cgmes.store-cgmes-model-as-network-extension": 'true',
        #     "iidm.import.cgmes.create-active-power-control-extension": 'true',
        #     "iidm.import.cgmes.post-processors": ["EntsoeCategory"]}
    )

    logger.info(f"Loaded: {network}")
    logger.debug(f"{import_report}")

    return network


def opdmprofile_to_bytes(opdm_profile):
    data = BytesIO(opdm_profile['opdm:Profile']['DATA'])
    data.name = opdm_profile['opdm:Profile']['pmd:fileName']
    return data


def load_opdm_data(opdm_objects, profile=None):
    if profile:
        return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opde:Component'] if instance['opdm:Profile']['pmd:cgmesProfile'] == profile])
    return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opde:Component']])


def filename_from_opdm_metadata(metadata):

    model_part = metadata.get('pmd:modelPartReference', None)

    if model_part:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}-{model_part}"

    else:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}"

    file_name = f"{metadata['pmd:validFrom']}_{metadata['pmd:timeHorizon']}_{model_authority}_{metadata['pmd:cgmesProfile']}_{metadata['pmd:versionNumber']}"
    file_name = ".".join([file_name, metadata["file_type"]])

    return file_name


def opdm_metadata_from_filename(file_name: str, meta_separator: str = "_"):
    """
    Parse OPDM metadata from a filename string into a dictionary.

    Args:
        :param file_name: (str): The filename containing metadata separated by underscores
            and a file extension
        :param meta_separator: (str): How the elements are seperated in the string, usually by "_"

    Returns:
        dict: Dictionary containing parsed metadata with OPDM-specific keys.

    Raises:
        AssertionError: If file_name is not a string.
        ValueError: If metadata parsing fails due to incorrect format.
    """
    # Constants for opdm metadata keys
    VALID_FROM = 'pmd:validFrom'
    TIME_HORIZON = 'pmd:timeHorizon'
    CGMES_PROFILE = 'pmd:cgmesProfile'
    VERSION_NUMBER = 'pmd:versionNumber'
    MERGING_ENTITY = 'pmd:mergingEntity'
    MERGING_AREA = 'pmd:mergingArea'
    MODEL_PART = 'pmd:modelPartReference'

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
        logger.warning(f"Parsing error, number of allowed meta in filename is 4 or 5 separated by {meta_separator} -> {file_name}")
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

    return metadata

def metadata_from_rdfxml(parsed_xml: etree._ElementTree):
    """Parse model metadata form xml, returns a dictionary with metadata"""

    assert isinstance(parsed_xml, etree._ElementTree)

    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

    header = parsed_xml.find("{*}FullModel") # Verion update proof, as long as element name remains the same
    meta_elements = header.getchildren()

    # Add model ID from FullModel@about
    metadata = {"Model.mRID": header.attrib.get(f'{{{rdf}}}about').split(":")[-1]}

    # Add all other metadata
    for element in meta_elements:

        key = element.tag.split("}")[1]

        value = element.text if element.text else element.attrib.get(f"{{{rdf}}}resource")

        if existing_value := metadata.get(key):
            value = existing_value + [value] if isinstance(existing_value, list) else [existing_value, value]

        metadata[key] = value

    return metadata

def opdm_metadata_from_rdfxml(parsed_xml: etree._ElementTree):

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

    for key, value in metadata_from_rdfxml(parsed_xml).items():
        if opdm_keys := opdm_metadata_map.get(key):
            for opdm_key in opdm_keys:
                metadata[opdm_key] = value

    return metadata


# TODO
# "pmd:modelPartReference": "D7",
# "pmd:sourcingActor": "D7", # From filename
# "pmd:TSO": "D7", # From filename
# "pmd:fileName": "20250317T2230Z_14_D7_SSH_011.zip",
# "pmd:content-reference": "CGMES/14/D7/20250317/223000/SSH/20250317T2230Z_14_D7_SSH_011.zip",
# "pmd:Content-Type": "CGMES",


def get_xml_from_zip(zip_file_path):

    with ZipFile(zip_file_path, 'r') as zipfile_object:
        xml_file_name = zipfile_object.namelist()[0]
        file_bytes = zipfile_object.read(xml_file_name)
        # Convert bytes to file-like object for parsing
        xml_tree_object = etree.parse(BytesIO(file_bytes))

    return xml_tree_object


def zip_xml(xml_file_object):

    xml_file_name = xml_file_object.name
    xml_file_extension = xml_file_name.split(".")[-1]
    zip_file_name = xml_file_name.replace(f".{xml_file_extension}", ".zip")

    zip_file_object = BytesIO()
    zip_file_object.name = zip_file_name

    # Create and save ZIP
    with ZipFile(zip_file_object, 'w', ZIP_DEFLATED) as zipfile:
        zipfile.writestr(xml_file_name, xml_file_object.getvalue())

    return zip_file_object


def get_metadata_from_filename(file_name):

    # Separators
    file_type_separator           = "."
    meta_separator                = "_"
    entity_and_domain_separator   = "-"

    logger.debug(file_name)
    file_metadata = {}
    file_name, file_type = file_name.split(file_type_separator)

    # Parse file metadata
    file_meta_list = file_name.split(meta_separator)

    # Naming before QoDC 2.1, where EQ might not have processType
    if len(file_meta_list) == 4:

        file_metadata["Model.scenarioTime"],\
        file_metadata["Model.modelingEntity"],\
        file_metadata["Model.messageType"],\
        file_metadata["Model.version"] = file_meta_list
        file_metadata["Model.processType"] = ""

        logger.warning("Only 4 meta elements found, expecting 5, setting Model.processType to empty string")

    # Naming after QoDC 2.1, always 5 positions
    elif len(file_meta_list) == 5:

        file_metadata["Model.scenarioTime"],\
        file_metadata["Model.processType"],\
        file_metadata["Model.modelingEntity"],\
        file_metadata["Model.messageType"],\
        file_metadata["Model.version"] = file_meta_list

    else:
        logger.error("Non CGMES file {}".format(file_name))

    if file_metadata.get("Model.modelingEntity"):

        entity_and_area_list = file_metadata["Model.modelingEntity"].split(entity_and_domain_separator)

        if len(entity_and_area_list) == 1:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"] = "", "" # Set empty string for both
            file_metadata["Model.forEntity"] = entity_and_area_list[0]

        if len(entity_and_area_list) == 2:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"] = entity_and_area_list
            file_metadata["Model.forEntity"] = ""

        if len(entity_and_area_list) == 3:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"],\
            file_metadata["Model.forEntity"] = entity_and_area_list

    return file_metadata


def generate_OPDM_ContentReference_from_filename(file_name, opdm_object_type="CGMES"):
    """
    Generates the file path based on the given parameters and metadata extracted from the filename.

    Parameters:
    file_name (str): The name of the file from which metadata will be extracted.
    opdm_object_type (str): The type of OPDM object, defaults to "CGMES".

    Returns:
    str: Generated file path using the provided template and extracted metadata.

    Example:
    >>> file_name = "example_filename"
    >>> opdm_object_type = "CGMES"
    >>> generate_OPDM_ContentReference_from_filename(file_name, opdm_object_type)
    'CGMES/processType/modelingEntity/20240529/123000/messageType/example_filename'
    """
    template = "{opdm_object_type}/{processType}/{modelingEntity}/{scenarioTime:%Y%m%d}/{scenarioTime:%H%M00}/{messageType}/{file_name}"

    meta = {key.split(".")[-1]: value for key, value in get_metadata_from_filename(file_name).items()}
    meta["scenarioTime"] = parse_datetime(meta["scenarioTime"])
    meta["file_name"] = file_name
    meta["opdm_object_type"] = opdm_object_type

    return template.format(**meta)


def export_model(network: pypowsybl.network, opdm_object_meta, profiles=None):

    if profiles:
        profiles = ",".join([str(profile) for profile in profiles])
    else:
        profiles = "SV,SSH,TP,EQ"

    file_base_name = filename_from_opdm_metadata(opdm_object_meta).split(".xml")[0]

    bytes_object = network.save_to_binary_buffer(
        format="CGMES",
        parameters={
            "iidm.export.cgmes.modeling-authority-set": opdm_object_meta['pmd:modelingAuthoritySet'],
            "iidm.export.cgmes.base-name": file_base_name,
            "iidm.export.cgmes.profiles": profiles,
            # "iidm.export.cgmes.naming-strategy": "cgmes-fix-all-invalid-ids",  # identity, cgmes, cgmes-fix-all-invalid-ids
            "iidm.export.cgmes.export-sv-injections-for-slacks": "False",
        })

    bytes_object.name = f"{file_base_name}_{uuid.uuid4()}.zip"

    return bytes_object


def get_model_outages(network: pypowsybl.network):

    outage_log = []
    lines = network.get_elements(element_type=pypowsybl.network.ElementType.LINE, all_attributes=True).reset_index(names=['grid_id'])
    _voltage_levels = network.get_voltage_levels(all_attributes=True).rename(columns={"name": "voltage_level_name"})
    _substations = network.get_substations(all_attributes=True).rename(columns={"name": "substation_name"})
    lines = lines.merge(_voltage_levels, left_on='voltage_level1_id', right_index=True, suffixes=(None, '_voltage_level'))
    lines = lines.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))
    lines['element_type'] = 'Line'

    dlines = get_network_elements(network, pypowsybl.network.ElementType.DANGLING_LINE).reset_index(names=['grid_id'])
    dlines['element_type'] = 'Tieline'

    gens = get_network_elements(network, pypowsybl.network.ElementType.GENERATOR).reset_index(names=['grid_id'])
    gens['element_type'] = 'Generator'

    disconnected_lines = lines[(lines['connected1'] == False) | (lines['connected2'] == False)]
    disconnected_dlines = dlines[dlines['connected'] == False]
    disconnected_gens = gens[gens['connected'] == False]

    outage_log.extend(disconnected_lines[['grid_id', 'name', 'element_type', 'country']].to_dict('records'))
    outage_log.extend(disconnected_dlines[['grid_id', 'name', 'element_type', 'country']].to_dict('records'))
    outage_log.extend(disconnected_gens[['grid_id', 'name', 'element_type', 'country']].to_dict('records'))

    return outage_log


if __name__ == "__main__":

    example_file_path = r"C:\Users\kristjan.vilgo\OneDrive - Elering AS\Documents\GitHub\igm_publication\20250314T2330Z_1D_ELERING_012.zip"

    # Find all XML regardless the zip folder depth
    rdfxml_files = triplets.rdf_parser.find_all_xml([example_file_path])

    # Repackage to form zip(xml)
    rdfzip_files = []
    for xml in rdfxml_files:
        rdfzip_files.append(zip_xml(xml))

    # Create OPDM objects
    opdm_object = create_opdm_objects([rdfzip_files])






