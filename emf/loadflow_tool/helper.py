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

def create_opdm_objects(models: list, metadata=None) -> list:
    """
    Function to create OPDM object like sturcture in memory

    :return: list of OPDM objects
    """
    opdm_objects = []

    for model in models:
        opdm_object = {'opde:Component': []}

        if metadata:
            opdm_object.update(metadata)

        for profile_instance in model:

            opdm_profile = metadata_from_filename(profile_instance.name)
            opdm_profile['pmd:fileName'] = profile_instance.name
            opdm_profile['DATA'] = profile_instance.getvalue()

            opdm_object['opde:Component'].append({'opdm:Profile': opdm_profile})

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

    attribs = [attr for attr in dir(instance) if (not ismethod(getattr(instance, attr)) and not attr.startswith("_"))]
    result_dict = {attr_key: getattr(instance, attr_key) for attr_key in attribs}

    if sanitize_to_strings:
        sanitized_dict = {}
        for k, v in result_dict.items():
            if isinstance(v, datetime):
                sanitized_dict[k] = v.isoformat()
            else:
                sanitized_dict[k] = str(v)
        result_dict = sanitized_dict

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

    return elements


def get_slack_generators(network: pypowsybl.network):

    slack_terminals = network.get_extension('slackTerminal')
    slack_generators = get_network_elements(network=network,
                                            element_type=pypowsybl.network.ElementType.GENERATOR,
                                            all_attributes=True,
                                            id=slack_terminals['element_id'])

    return slack_generators


def get_connected_component_counts(network: pypowsybl.network, bus_count_threshold: int | None = None):
    counts = network.get_buses().connected_component.value_counts()
    if bus_count_threshold:
        counts = counts[counts > bus_count_threshold]
    return counts.to_dict()


def load_model(opdm_objects: List[dict], parameters: dict = None, skip_default_parameters: bool = False):
    """
    Loads given list of models (opdm_objects) into pypowsybl using internal (known good) default_parameters
    Additional parameters can be specified as a dict in field parameters which will overwrite the default ones if keys
    are matching
    :param opdm_objects: list of dictionaries following the opdm model format
    :param parameters: dictionary of desired parameters for loading models to pypowsybl
    :param skip_default_parameters: skip the default parameters
    """
    model_data = {}
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

    logger.info(f"Loaded {network}")
    logger.debug(f"{import_report}")

    # Network model object data
    model_data["network_meta"] = attr_to_dict(instance=network, sanitize_to_strings=True)
    model_data["network"] = network
    model_data["network_valid"] = network.validate().name

    # Network model import reporter data
    # model_data["import_report"] = json.loads(import_report.to_json())
    # model_data["import_report"] = str(import_report)

    return model_data


def opdmprofile_to_bytes(opdm_profile):
    data = BytesIO(opdm_profile['opdm:Profile']['DATA'])
    data.name = opdm_profile['opdm:Profile']['pmd:fileName']
    return data


def load_opdm_data(opdm_objects, profile=None):
    if profile:
        return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opde:Component'] if instance['opdm:Profile']['pmd:cgmesProfile'] == profile])
    return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opde:Component']])


def filename_from_metadata(metadata):

    model_part = metadata.get('pmd:modelPartReference', None)

    if model_part:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}-{model_part}"

    else:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}"

    file_name = f"{metadata['pmd:validFrom']}_{metadata['pmd:timeHorizon']}_{model_authority}_{metadata['pmd:cgmesProfile']}_{metadata['pmd:versionNumber']}"
    file_name = ".".join([file_name, metadata["file_type"]])

    return file_name

meta_separator = "_"


def metadata_from_filename(file_name):

    file_metadata = {} # Meta container

    file_name, file_metadata["file_type"] = file_name.split(".")
    meta_list = file_name.split(meta_separator)

    if len(meta_list) == 4:   #try: #if "_EQ_" in file_name or "_BD_" in file_name:

        file_metadata['pmd:validFrom'], model_authority, file_metadata['pmd:cgmesProfile'], file_metadata['pmd:versionNumber'] = meta_list
        file_metadata['pmd:timeHorizon'] = ""

    elif len(meta_list) == 5:

        file_metadata['pmd:validFrom'], file_metadata['pmd:timeHorizon'], model_authority, file_metadata['pmd:cgmesProfile'], file_metadata['pmd:versionNumber'] = meta_list

    else:
        logger.warning("Parsing error, number of allowed meta in filename is 4 or 5 separated by '_' -> {} ".format(file_name))

    model_authority_list = model_authority.split("-")

    if len(model_authority_list) == 1:
        file_metadata['pmd:modelPartReference'] = model_authority

    elif len(model_authority_list) == 2:
        file_metadata['pmd:mergingEntity'], file_metadata['pmd:mergingArea'] = model_authority_list

    elif len(model_authority_list) == 3:
        file_metadata['pmd:mergingEntity'], file_metadata['pmd:mergingArea'], file_metadata['pmd:modelPartReference'] = model_authority_list

    else:
        logger.error(f"Parsing error {model_authority}")

    return file_metadata

def get_xml_from_zip(zip_file_path):

    zipfile_object    = ZipFile(zip_file_path)
    xml_file_name     = zipfile_object.namelist()[0]
    file_unzipped     = zipfile_object.open(xml_file_name, mode="r")
    xml_tree_object   = etree.parse(file_unzipped)

    return xml_tree_object

def zip_xml_file(xml_etree_object, file_metadata, destination_bath):

    # Get meta and path
    file_metadata["file_type"] = "zip"
    zip_file_name = filename_from_metadata(file_metadata)

    file_metadata["file_type"] = "xml"
    xml_file_name = filename_from_metadata(file_metadata)

    zip_file_path = os.path.join(destination_bath, zip_file_name)

    # Create and save ZIP
    out_zipped_file = ZipFile(zip_file_path, 'w', ZIP_DEFLATED)
    out_zipped_file.writestr(xml_file_name, etree.tostring(xml_etree_object))#, pretty_print=True))
    out_zipped_file.close()

    return zip_file_path


def get_metadata_from_xml(parsed_xml):
    """Parse model metadata form xml, retruns a dictionary"""
    #parsed_xml = etree.parse(filepath_or_fileobject)

    header = parsed_xml.find("{*}FullModel")
    meta_elements = header.getchildren()

    # Add model ID
    meta_dict = {"mRID":header.attrib.values()[0].split(":")[-1]}

    # Add all other metadata
    for element in meta_elements:
        if element.text:
            meta_dict[element.tag.split("}")[1]] = element.text
        else:
            meta_dict[element.tag.split("}")[1]] = element.attrib.values()[0]

    return meta_dict


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

    if file_metadata.get("Model.modelingEntity", False):

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

    file_base_name = filename_from_metadata(opdm_object_meta).split(".xml")[0]

    bytes_object = network.save_to_binary_buffer(
        format="CGMES",
        parameters={
            "iidm.export.cgmes.modeling-authority-set": opdm_object_meta['pmd:modelingAuthoritySet'],
            "iidm.export.cgmes.base-name": file_base_name,
            "iidm.export.cgmes.profiles": profiles,
            "iidm.export.cgmes.naming-strategy": "cgmes-fix-all-invalid-ids",  # identity, cgmes, cgmes-fix-all-invalid-ids
            "iidm.export.cgmes.export-sv-injections-for-slacks": "False",
        })

    bytes_object.name = f"{file_base_name}_{uuid.uuid4()}.zip"

    return bytes_object
