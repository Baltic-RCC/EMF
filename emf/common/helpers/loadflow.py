import logging
import uuid
import re
import pandas as pd
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
import pypowsybl
from typing import List

logger = logging.getLogger(__name__)


def package_for_pypowsybl(opdm_objects, return_zip: bool = False):
    """
    Method to transform OPDM objects into sufficient format binary buffer or zip package
    :param opdm_objects: list of OPDM objects
    :param return_zip: flag to save OPDM objects as zip package in local directory
    :return: binary buffer or zip package file name
    """
    output_object = BytesIO()
    if return_zip:
        output_object = f"{uuid.uuid4()}.zip"
        logger.info(f"Adding files to {output_object}")

    with ZipFile(output_object, "w") as global_zip:
        for opdm_components in opdm_objects:
            for instance in opdm_components['opde:Component']:
                with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                    for file_name in instance_zip.namelist():
                        logger.info(f"Adding file: {file_name}")
                        global_zip.writestr(file_name, instance_zip.open(file_name).read())

    return output_object


def load_network_model(opdm_objects: List[dict], parameters: dict = None, skip_default_parameters: bool = False):
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


def get_model_outages(network: pypowsybl.network):

    # Get network elements
    lines = network.get_elements(element_type=pypowsybl.network.ElementType.LINE, all_attributes=True).reset_index(names=['grid_id'])
    _voltage_levels = network.get_voltage_levels(all_attributes=True).rename(columns={"name": "voltage_level_name"})
    _substations = network.get_substations(all_attributes=True).rename(columns={"name": "substation_name"})
    lines = lines.merge(_voltage_levels, left_on='voltage_level1_id', right_index=True, suffixes=(None, '_voltage_level'))
    lines = lines.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))
    lines['element_type'] = 'LINE'

    dlines = get_network_elements(network, pypowsybl.network.ElementType.DANGLING_LINE).reset_index(names=['grid_id'])
    dlines['element_type'] = 'DANGLING_LINE'

    gens = get_network_elements(network, pypowsybl.network.ElementType.GENERATOR).reset_index(names=['grid_id'])
    gens['element_type'] = 'GENERATOR'

    # Filter out disconnected elements
    disconnected_lines = lines[(lines['connected1'] == False) | (lines['connected2'] == False)]
    disconnected_dlines = dlines[dlines['connected'] == False]
    disconnected_gens = gens[gens['connected'] == False]

    # Filter out only 330kv and above
    disconnected_elements = pd.concat([disconnected_lines, disconnected_dlines, disconnected_gens])
    disconnected_elements = disconnected_elements[disconnected_elements['nominal_v'] >= 330]

    return disconnected_elements.to_dict('records')


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


if __name__ == "__main__":
    pass