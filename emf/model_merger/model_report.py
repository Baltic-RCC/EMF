"""
Merge benchmark/diagnostic report.

Builds an Excel workbook with one sheet per major CGMES element type (lines,
tie-lines, 2/3-winding transformers, buses, generators, loads, shunt
compensators) from a pypowsybl network, and can diff two such reports
against each other (useful for comparing two merge outputs of the same
scenario, e.g. before/after a fix, or two TSOs' merges).

Reusable API (network-based, no disk assumptions):
    generate_report_from_network(network, output_foldGENERATE_NETWORK_REPORTer=..., model_author=...) -> ModelReport
    compare_reports(report_a, report_b, output_folder=..., labels=(...)) -> str

Disk-based/CLI usage (loading raw CGMES files or previously saved reports
from local disk) lives in the __main__ block below - see:
    python -m emf.model_merger.model_report <path_or_report_1> [<path_or_report_2>]
"""
import os
from datetime import datetime

import pandas
import pypowsybl

from emf.common.loadflow_tool.load_files_general import check_and_create_the_folder_path

LINE_SHEET_NAME = 'line'
TIE_LINE_SHEET_NAME = 'tieline'
TWO_WINDINGS_TRANSFORMER_SHEET_NAME = 'two_windings_transformer'
THREE_WINDINGS_TRANSFORMER_SHEET_NAME = 'three_windings_transformer'
BUS_SHEET_NAME = 'bus'
GENERATOR_SHEET_NAME = 'generator'
LOAD_SHEET_NAME = 'load'
SHUNT_COMPENSATOR_SHEET_NAME = 'shunt_compensator'

ATTRIBUTE_KEYS = {LINE_SHEET_NAME: 'lines',
                  TIE_LINE_SHEET_NAME: 'tie_lines',
                  TWO_WINDINGS_TRANSFORMER_SHEET_NAME: 'two_windings_transformers',
                  THREE_WINDINGS_TRANSFORMER_SHEET_NAME: 'three_windings_transformers',
                  BUS_SHEET_NAME: 'buses',
                  GENERATOR_SHEET_NAME: 'generators',
                  LOAD_SHEET_NAME: 'loads',
                  SHUNT_COMPENSATOR_SHEET_NAME: 'shunt_compensators'}

# (sheet_name, index_columns, data_columns, name_columns) used both to build and to compare reports
COMPARISON_SPECS = [
    (LINE_SHEET_NAME, ['Line id', 'Side'], ['P', 'Q'], ['Line name', 'country']),
    (TIE_LINE_SHEET_NAME, ['TieLine id'], ['P', 'Q'], ['TieLine name', 'country', 'GEO Tags']),
    (TWO_WINDINGS_TRANSFORMER_SHEET_NAME, ['TF id', 'Side'], ['P', 'Q'], ['TF name', 'country', 'GEO Tags']),
    (THREE_WINDINGS_TRANSFORMER_SHEET_NAME, ['TF id', 'Side'], ['P', 'Q'], ['TF name', 'country', 'GEO Tags']),
    (BUS_SHEET_NAME, ['Bus id'], ['Bus V', 'Bus angle'], ['Bus name']),
    (GENERATOR_SHEET_NAME, ['Generator id'], ['P', 'Q'], ['Generator name', 'country']),
    (LOAD_SHEET_NAME, ['Load id'], ['P', 'Q'], ['Load name', 'country', 'GEO Tags']),
    (SHUNT_COMPENSATOR_SHEET_NAME, ['ShuntCompensator id'], ['Q'], ['ShuntCompensator name', 'country', 'GEO Tags']),
]


class ModelReport:
    """Container for the per-element-type report dataframes, with Excel read/write."""

    def __init__(self, full_file_name: str = None, model_author: str = None):
        self.model_author = model_author
        self.lines = pandas.DataFrame()
        self.tie_lines = pandas.DataFrame()
        self.two_windings_transformers = pandas.DataFrame()
        self.three_windings_transformers = pandas.DataFrame()
        self.buses = pandas.DataFrame()
        self.generators = pandas.DataFrame()
        self.loads = pandas.DataFrame()
        self.shunt_compensators = pandas.DataFrame()

        if full_file_name:
            self.load_from_excel(full_file_name=full_file_name)

    def save_to_excel(self, full_file_name: str):
        with pandas.ExcelWriter(full_file_name) as writer:
            for sheet_name, attribute_name in ATTRIBUTE_KEYS.items():
                getattr(self, attribute_name).to_excel(writer, sheet_name=sheet_name)

    def load_from_excel(self, full_file_name: str):
        with pandas.ExcelFile(full_file_name) as excel_content:
            for sheet_name in excel_content.sheet_names:
                attribute_name = ATTRIBUTE_KEYS.get(sheet_name)
                if attribute_name:
                    setattr(self, attribute_name, pandas.read_excel(excel_content, sheet_name=sheet_name))


def _build_element_reports(network: pypowsybl.network) -> dict:
    """Extracts and flattens the per-element-type dataframes used for the report sheets."""
    buses = network.get_elements(element_type=pypowsybl.network.ElementType.BUS, all_attributes=True).reset_index()
    voltage_level = network.get_elements(element_type=pypowsybl.network.ElementType.VOLTAGE_LEVEL,
                                         all_attributes=True).reset_index()
    substations = network.get_elements(element_type=pypowsybl.network.ElementType.SUBSTATION,
                                       all_attributes=True).reset_index()
    limits = network.get_elements(element_type=pypowsybl.network.ElementType.OPERATIONAL_LIMITS,
                                  all_attributes=True).reset_index()

    voltage_level = voltage_level.rename(columns={'id': 'Voltage level id',
                                                  'name': 'Voltage level name',
                                                  'substation_id': 'Substation Id',
                                                  'nominal_v': 'Nominal V',
                                                  'high_voltage_limit': 'High Voltage Limit',
                                                  'low_voltage_limit': 'Low Voltage Limit',
                                                  'fictitious': 'Fictitious',
                                                  'topology_kind': 'Topology kind'})
    substations = substations[['id', 'name', 'TSO', 'geo_tags', 'country', 'CGMES.subRegionId', 'CGMES.regionId']]
    substations = substations.rename(columns={'id': 'Substation Id',
                                              'geo_tags': 'GEO Tags',
                                              'name': 'Region name',
                                              'CGMES.subRegionId': 'Subregion Id',
                                              'CGMES.regionId': 'Region Id'})
    voltage_level_stations = voltage_level.merge(substations, on='Substation Id', how='left')

    line_limits = limits[(limits['element_type'] == 'LINE') & (limits['acceptable_duration'] == -1) &
                         (limits['type'] == 'CURRENT')]
    line_limits_reduced = line_limits[['group_name', 'value']].rename(columns={'value': 'Imax'})

    line_buses = buses[['id', 'name', 'v_mag', 'v_angle', 'connected_component', 'synchronous_component', 'fictitious']]
    line_buses = line_buses.rename(columns={'id': 'Bus id', 'name': 'Bus name', 'v_mag': 'Bus V', 'v_angle': 'Bus angle'})

    # Lines
    lines = network.get_elements(element_type=pypowsybl.network.ElementType.LINE,
                                 all_attributes=True).reset_index().sort_values('id')
    lines_side_one = lines[['id', 'name', 'p1', 'q1', 'i1', 'r', 'x', 'g1', 'b1',
                            'voltage_level1_id', 'bus1_id', 'connected1', 'selected_limits_group_1']].dropna()
    lines_side_one = lines_side_one.rename(columns={'id': 'Line id', 'name': 'Line name', 'p1': 'P', 'q1': 'Q',
                                                    'i1': 'I', 'voltage_level1_id': 'Voltage level id',
                                                    'bus1_id': 'Bus id', 'connected1': 'connected',
                                                    'selected_limits_group_1': 'group_name',
                                                    'r': 'R', 'x': 'X', 'g1': 'G', 'b1': 'B'})
    lines_side_one['Side'] = 1
    lines_side_two = lines[['id', 'name', 'r', 'x', 'g2', 'b2', 'p2', 'q2', 'i2', 'voltage_level2_id', 'bus2_id',
                            'connected2', 'selected_limits_group_2']].dropna()
    lines_side_two = lines_side_two.rename(columns={'id': 'Line id', 'name': 'Line name', 'p2': 'P', 'q2': 'Q',
                                                    'i2': 'I', 'voltage_level2_id': 'Voltage level id',
                                                    'bus2_id': 'Bus id', 'connected2': 'connected',
                                                    'selected_limits_group_2': 'group_name',
                                                    'r': 'R', 'x': 'X', 'g2': 'G', 'b2': 'B'})
    lines_side_two['Side'] = 2
    lines_report = pandas.concat([lines_side_one, lines_side_two]).reset_index().sort_values(by=['Line id', 'Side'])
    lines_report = lines_report.merge(line_buses, on='Bus id', how='left')
    lines_report = lines_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    lines_report = lines_report.merge(line_limits_reduced, on='group_name', how='left')
    lines_report = lines_report.drop(columns=['group_name']).sort_values(by=['Line id', 'Side'])
    final_lines_report = lines_report[['Line id', 'Line name', 'country', 'Side', 'P', 'Q', 'I', 'Imax', 'Nominal V',
                                       'Bus V', 'Bus angle', 'Bus id', 'Bus name', 'connected']]

    # Tie lines
    tie_lines = network.get_elements(element_type=pypowsybl.network.ElementType.TIE_LINE,
                                     all_attributes=True).reset_index()
    tie_lines_one = (tie_lines[['id', 'name', 'dangling_line1_id', 'pairing_key', 'ucte_xnode_code']]
                     .rename(columns={'dangling_line1_id': 'dangling_line_id'}))
    tie_lines_two = (tie_lines[['id', 'name', 'dangling_line2_id', 'pairing_key', 'ucte_xnode_code']]
                     .rename(columns={'dangling_line2_id': 'dangling_line_id'}))
    tie_lines_merged = pandas.concat([tie_lines_one, tie_lines_two])

    dangling_lines = network.get_elements(element_type=pypowsybl.network.ElementType.DANGLING_LINE,
                                          all_attributes=True).reset_index()
    dangling_lines = dangling_lines.rename(columns={'id': 'TieLine id',
                                                    'name': 'TieLine name',
                                                    'lineEnergyIdentificationCodeEIC': 'EIC',
                                                    'ucte_xnode_code': 'UCTE Xnode',
                                                    'boundary_p': 'Boundary P',
                                                    'boundary_q': 'Boundary Q',
                                                    'p0': 'P0', 'q0': 'Q0', 'p': 'P', 'q': 'Q',
                                                    'boundary_v_mag': 'Boundary V',
                                                    'boundary_v_angle': 'Boundary angle',
                                                    'i': 'I',
                                                    'voltage_level_id': 'Voltage level id',
                                                    'bus_id': 'Bus id',
                                                    'selected_limits_group': 'group_name',
                                                    'CGMES.Terminal': 'Terminal',
                                                    'isHvdc': 'HVDC',
                                                    'CGMES.Terminal_Boundary': 'Boundary Terminal',
                                                    'CGMES.TopologicalNode_Boundary': 'Boundary TopologicalNode',
                                                    'CGMES.EquivalentInjection': 'EquivalentInjection',
                                                    'CGMES.EquivalentInjectionTerminal': 'EquivalentInjection Terminal',
                                                    'CGMES.ConnectivityNode_Boundary': 'Boundary ConnectivityNode'})
    tie_lines_report = dangling_lines.merge(line_buses, on='Bus id', how='left')
    tie_lines_report = tie_lines_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    tie_lines_report = tie_lines_report.merge(line_limits_reduced, on='group_name', how='left')
    tie_lines_report = tie_lines_report.drop(columns=['group_name']).sort_values(by=['TieLine id'])
    tie_lines_report = tie_lines_report.merge(tie_lines_merged[['id', 'dangling_line_id']]
                                              .rename(columns={'dangling_line_id': 'TieLine id', 'id': 'Tie Line id'}),
                                              on='TieLine id', how='left')
    final_tie_lines_report = tie_lines_report[['TieLine id', 'TieLine name', 'country', 'EIC', 'UCTE Xnode',
                                               'Boundary P', 'Boundary Q', 'P', 'Q', 'Nominal V', 'Boundary V',
                                               'Bus V', 'Boundary angle', 'Bus angle', 'I', 'Imax', 'Bus id',
                                               'Bus name', 'Terminal', 'Boundary Terminal',
                                               'Boundary TopologicalNode', 'EquivalentInjection Terminal',
                                               'EquivalentInjection', 'Boundary ConnectivityNode', 'connected',
                                               'paired', 'HVDC', 'GEO Tags', 'Tie Line id', 'P0', 'Q0']]

    # 2-winding transformers
    two_windings_tf = network.get_elements(element_type=pypowsybl.network.ElementType.TWO_WINDINGS_TRANSFORMER,
                                           all_attributes=True).reset_index()
    two_windings_side_one = two_windings_tf[['id', 'name', 'p1', 'q1', 'i1', 'voltage_level1_id', 'bus1_id',
                                             'connected1', 'selected_limits_group_1']].rename(
        columns={'p1': 'P', 'q1': 'Q', 'i1': 'I', 'voltage_level1_id': 'Voltage level id', 'bus1_id': 'Bus id',
                'connected1': 'connected', 'selected_limits_group_1': 'group_name'})
    two_windings_side_one['Side'] = 1
    two_windings_side_two = two_windings_tf[['id', 'name', 'p2', 'q2', 'i2', 'voltage_level2_id', 'bus2_id',
                                             'connected2', 'selected_limits_group_2']].rename(
        columns={'p2': 'P', 'q2': 'Q', 'i2': 'I', 'voltage_level2_id': 'Voltage level id', 'bus2_id': 'Bus id',
                'connected2': 'connected', 'selected_limits_group_2': 'group_name'})
    two_windings_side_two['Side'] = 2
    two_windings_merged = pandas.concat([two_windings_side_one, two_windings_side_two]).sort_values(by=['id', 'Side'])
    two_windings_merged = two_windings_merged.rename(columns={'id': 'TF id', 'name': 'TF name'})
    two_windings_report = two_windings_merged.merge(line_buses, on='Bus id', how='left')
    two_windings_report = two_windings_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    two_windings_report = two_windings_report.merge(line_limits_reduced, on='group_name', how='left')
    two_windings_report = two_windings_report.drop(columns=['group_name']).sort_values(by=['TF id', 'Side'])
    final_two_windings_report = two_windings_report[['TF id', 'TF name', 'country', 'Side', 'P', 'Q', 'I', 'Imax',
                                                     'Nominal V', 'Bus V', 'Bus angle', 'Bus id', 'Bus name',
                                                     'connected', 'GEO Tags']]

    # 3-winding transformers
    three_windings_tf = network.get_elements(element_type=pypowsybl.network.ElementType.THREE_WINDINGS_TRANSFORMER,
                                             all_attributes=True).reset_index()
    three_windings_side_one = three_windings_tf[['id', 'name', 'p1', 'q1', 'i1', 'voltage_level1_id', 'bus1_id',
                                                 'connected1', 'selected_limits_group_1']].rename(
        columns={'p1': 'P', 'q1': 'Q', 'i1': 'I', 'voltage_level1_id': 'Voltage level id', 'bus1_id': 'Bus id',
                'connected1': 'connected', 'selected_limits_group_1': 'group_name'})
    three_windings_side_one['Side'] = 1
    three_windings_side_two = three_windings_tf[['id', 'name', 'p2', 'q2', 'i2', 'voltage_level2_id', 'bus2_id',
                                                 'connected2', 'selected_limits_group_2']].rename(
        columns={'p2': 'P', 'q2': 'Q', 'i2': 'I', 'voltage_level2_id': 'Voltage level id', 'bus2_id': 'Bus id',
                'connected2': 'connected', 'selected_limits_group_2': 'group_name'})
    three_windings_side_two['Side'] = 2
    three_windings_side_three = three_windings_tf[['id', 'name', 'p3', 'q3', 'i3', 'voltage_level3_id', 'bus3_id',
                                                    'connected3', 'selected_limits_group_3']].rename(
        columns={'p3': 'P', 'q3': 'Q', 'i3': 'I', 'voltage_level3_id': 'Voltage level id', 'bus3_id': 'Bus id',
                'connected3': 'connected', 'selected_limits_group_3': 'group_name'})
    three_windings_side_three['Side'] = 3
    three_windings_merged = pandas.concat([three_windings_side_one, three_windings_side_two,
                                           three_windings_side_three]).sort_values(by=['id', 'Side'])
    three_windings_merged = three_windings_merged.rename(columns={'id': 'TF id', 'name': 'TF name'})
    three_windings_report = three_windings_merged.merge(line_buses, on='Bus id', how='left')
    three_windings_report = three_windings_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    three_windings_report = three_windings_report.merge(line_limits_reduced, on='group_name', how='left')
    three_windings_report = three_windings_report.drop(columns=['group_name']).sort_values(by=['TF id', 'Side'])
    final_three_windings_report = three_windings_report[['TF id', 'TF name', 'country', 'Side', 'P', 'Q', 'I', 'Imax',
                                                         'Nominal V', 'Bus V', 'Bus angle', 'Bus id', 'Bus name',
                                                         'connected', 'GEO Tags']]

    # Buses
    buses = buses.rename(columns={'id': 'Bus id', 'name': 'Bus name', 'v_mag': 'Bus V', 'v_angle': 'Bus angle',
                                  'fictitious': 'Fictitious', 'connected_component': 'connected component',
                                  'synchronous_component': 'synchronous component',
                                  'voltage_level_id': 'Voltage level id'}).sort_values(by='Bus id').reset_index(drop=True)
    injections = network.get_elements(element_type=pypowsybl.network.ElementType.INJECTION,
                                      all_attributes=True).reset_index()
    injections = injections[['id', 'bus_id', 'p', 'q', 'i', 'type']].rename(
        columns={'id': 'Injection id', 'bus_id': 'Bus id', 'type': 'Injection type', 'p': 'Pinj', 'q': 'Qinj', 'i': 'Iinj'})
    buses_report = buses.merge(injections, on='Bus id', how='left')
    buses_report = buses_report.merge(voltage_level_stations.drop(columns=['Fictitious']), on='Voltage level id', how='left')
    final_buses_report = buses_report[['Bus id', 'Bus name', 'country', 'Nominal V', 'Bus V', 'Bus angle',
                                       'Injection type', 'Pinj', 'Qinj', 'Iinj', 'Injection id', 'Fictitious',
                                       'Voltage level name', 'Substation Id', 'High Voltage Limit',
                                       'Low Voltage Limit', 'connected component', 'synchronous component']]
    final_buses_report = final_buses_report.sort_values(by='Bus id').reset_index(drop=True)

    # Generators
    generators = network.get_elements(element_type=pypowsybl.network.ElementType.GENERATOR,
                                      all_attributes=True).reset_index()
    generators = generators.rename(columns={'id': 'Generator id', 'name': 'Generator name', 'target_p': 'Target P',
                                            'p': 'P', 'q': 'Q', 'i': 'I',
                                            'voltage_regulator_on': 'Voltage Regulator On', 'min_p': 'Min P',
                                            'max_p': 'Max P', 'voltage_level_id': 'Voltage level id',
                                            'reactive_limits_kind': 'Reactive Limits Kind', 'target_v': 'Target V',
                                            'target_q': 'Target Q', 'min_q': 'Min Q', 'max_q': 'Max Q',
                                            'min_q_at_target_p': 'Min Q at Target P',
                                            'max_q_at_target_p': 'Max Q at Target P', 'rated_s': 'Rated S',
                                            'regulated_element_id': 'Regulated Element id', 'bus_id': 'Bus id',
                                            'connected': 'Connected', 'fictitious': 'Fictitious',
                                            'CGMES.synchronousMachineType': 'Synchronous Machine Type',
                                            'CGMES.synchronousMachineOperatingMode': 'SynchronousMachineOperatingMode',
                                            'CGMES.GeneratingUnit': 'GeneratingUnit',
                                            'CGMES.RegulatingControl': 'RegulatingControl'})
    generators_report = generators.merge(voltage_level_stations.drop(columns=['Fictitious']), on='Voltage level id', how='left')
    final_generators_report = generators_report[['Generator id', 'Generator name', 'country', 'Target P', 'P', 'Q',
                                                 'I', 'Voltage Regulator On', 'Min P', 'Max P',
                                                 'Reactive Limits Kind', 'Target V', 'Target Q', 'Min Q', 'Max Q',
                                                 'Min Q at Target P', 'Max Q at Target P', 'Rated S',
                                                 'Regulated Element id', 'Bus id', 'Connected', 'Fictitious',
                                                 'Synchronous Machine Type', 'SynchronousMachineOperatingMode',
                                                 'GeneratingUnit', 'RegulatingControl']]
    final_generators_report = final_generators_report.sort_values(by='Generator id').reset_index(drop=True)

    # Loads
    loads = network.get_elements(element_type=pypowsybl.network.ElementType.LOAD, all_attributes=True).reset_index()
    loads = loads.rename(columns={'id': 'Load id', 'name': 'Load name', 'p0': 'P0', 'q0': 'Q0', 'p': 'P', 'q': 'Q',
                                  'i': 'I', 'voltage_level_id': 'Voltage level id', 'bus_id': 'Bus id',
                                  'connected': 'Connected', 'fictitious': 'Fictitious',
                                  'CGMES.originalClass': 'OriginalClass'})
    loads_report = loads.merge(voltage_level_stations.drop(columns=['Fictitious']), on='Voltage level id', how='left')
    loads_report = loads_report.merge(line_buses, on='Bus id', how='left')
    final_loads_report = loads_report[['Load id', 'Load name', 'country', 'P0', 'Q0', 'P', 'Q', 'I', 'Nominal V',
                                       'Bus V', 'Bus angle', 'Connected', 'Fictitious', 'OriginalClass', 'Bus name',
                                       'Voltage level id', 'Bus id', 'GEO Tags']]
    final_loads_report = final_loads_report.sort_values(by='Load id').reset_index(drop=True)

    # Shunt compensators
    shunt_compensators = network.get_elements(element_type=pypowsybl.network.ElementType.SHUNT_COMPENSATOR,
                                              all_attributes=True).reset_index()
    shunt_compensators = shunt_compensators.rename(columns={'id': 'ShuntCompensator id',
                                                            'name': 'ShuntCompensator name',
                                                            'model_type': 'Model type', 'p': 'P', 'q': 'Q', 'i': 'I',
                                                            'voltage_regulation_on': 'Voltage regulation on',
                                                            'regulating_bus_id': 'Regulating bus id',
                                                            'CGMES.RegulatingControl': 'RegulatingControl',
                                                            'target_v': 'Target V',
                                                            'target_deadband': 'Target Deadband', 'bus_id': 'Bus id',
                                                            'voltage_level_id': 'Voltage level id'})
    shunt_compensator_report = shunt_compensators.merge(voltage_level_stations, on='Voltage level id', how='left')
    shunt_compensator_report = shunt_compensator_report.merge(line_buses, on='Bus id', how='left')
    final_shunt_compensator_report = shunt_compensator_report[['ShuntCompensator id', 'ShuntCompensator name',
                                                               'country', 'Model type', 'P', 'Q', 'I',
                                                               'Voltage regulation on', 'Regulating bus id',
                                                               'RegulatingControl', 'Target V', 'Target Deadband',
                                                               'Bus id', 'Bus name', 'Bus V', 'Bus angle', 'Nominal V',
                                                               'GEO Tags']]
    final_shunt_compensator_report = final_shunt_compensator_report.sort_values(
        by='ShuntCompensator id').reset_index(drop=True)

    return {
        'lines': final_lines_report,
        'tie_lines': final_tie_lines_report,
        'two_windings_transformers': final_two_windings_report,
        'three_windings_transformers': final_three_windings_report,
        'buses': final_buses_report,
        'generators': final_generators_report,
        'loads': final_loads_report,
        'shunt_compensators': final_shunt_compensator_report,
    }


def generate_report_from_network(network: pypowsybl.network,
                                 output_folder: str = "./merge_reports",
                                 model_author: str = None,
                                 save: bool = True) -> ModelReport:
    """
    One-line entry point: build all element-type report sheets from an already-loaded
    pypowsybl network and, unless save=False, write them to a single Excel workbook.
    Usable directly from the merger with a live merged_model.network - no disk I/O needed.
    """
    report_instance = ModelReport(model_author=model_author)
    for attribute_name, dataframe in _build_element_reports(network).items():
        setattr(report_instance, attribute_name, dataframe)

    if save:
        folder_to_store = check_and_create_the_folder_path(output_folder)
        time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        author_part = f"{model_author}_" if model_author else ""
        full_file_name = f"{folder_to_store.removesuffix('/')}/merge_report_{author_part}{time_moment_now}.xlsx"
        report_instance.save_to_excel(full_file_name)

    return report_instance


def compare_two_dataframes(report_one: ModelReport,
                           report_two: ModelReport,
                           index_columns: list | str,
                           data_columns: list,
                           name_columns: list,
                           sheet_name: str,
                           writer: pandas.ExcelWriter):
    left_side = " " + (report_one.model_author or "A")
    right_side = " " + (report_two.model_author or "B")
    sides = [left_side, right_side]
    attribute_name = ATTRIBUTE_KEYS.get(sheet_name)
    left_dataframe = getattr(report_one, attribute_name)
    right_dataframe = getattr(report_two, attribute_name)
    if left_dataframe.empty and right_dataframe.empty:
        return

    merged_dataframe = left_dataframe.merge(right_dataframe, on=index_columns, how='outer',
                                            suffixes=(left_side, right_side), indicator=True).set_index(index_columns)
    both_all = merged_dataframe[merged_dataframe['_merge'] == 'both']
    left_only = merged_dataframe[merged_dataframe['_merge'] == 'left_only']
    right_only = merged_dataframe[merged_dataframe['_merge'] == 'right_only']
    count_dataframe = pandas.DataFrame([{'Both': len(both_all.index),
                                         left_side + ' unique': len(left_only.index),
                                         right_side + ' unique': len(right_only.index)}])

    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    worksheet.write_string(0, 0, "Count of elements")
    count_dataframe.to_excel(writer, sheet_name=sheet_name, startrow=1, startcol=0)
    row_counter = 4
    col_counter = 5

    if not left_only.empty:
        worksheet.write_string(row_counter, 0, f"Unique elements from {left_side}")
        row_counter += 1
        left_only_columns = [c for c in left_only.columns.to_list() if c.endswith(left_side) and ' id' not in c]
        left_only = left_only[left_only_columns].reset_index(drop=True)
        left_only.to_excel(writer, sheet_name=sheet_name, startrow=row_counter, startcol=0)
        col_counter = max(col_counter, len(left_only.columns.to_list()) + 2)
        row_counter = row_counter + 2 + len(left_only.index)

    if not right_only.empty:
        worksheet.write_string(row_counter, 0, f"Unique elements from {right_side}")
        row_counter += 1
        right_only_columns = [c for c in right_only.columns.to_list() if c.endswith(right_side) and ' id' not in c]
        right_only = right_only[right_only_columns].reset_index(drop=True)
        right_only.to_excel(writer, sheet_name=sheet_name, startrow=row_counter, startcol=0)
        col_counter = max(col_counter, len(right_only.columns.to_list()) + 2)

    calc_columns = {}
    name_left_columns = {name_column + left_side: name_column for name_column in name_columns}
    for data_column in data_columns:
        diff_column = data_column + '_diff'
        calc_columns[data_column] = diff_column
        both_all[diff_column] = abs(both_all[data_column + left_side] - both_all[data_column + right_side])
    diff_data_frame = both_all[list(name_left_columns.keys()) + list(calc_columns.values())].rename(columns=name_left_columns)
    diff_data_frame = diff_data_frame.reset_index(drop=True)
    worksheet.write_string(0, col_counter, "Calculated difference from common elements")
    diff_data_frame.to_excel(writer, sheet_name=sheet_name, startrow=1, startcol=col_counter)

    col_counter = col_counter + len(diff_data_frame.columns.to_list()) + 2
    diff_data_frame_description = diff_data_frame.describe().reset_index()
    worksheet.write_string(0, col_counter, "Statistics of difference of common elements")
    diff_data_frame_description.to_excel(writer, sheet_name=sheet_name, startrow=1, startcol=col_counter)
    row_counter = len(diff_data_frame_description.index) + 3
    largest_rows = 10
    for data_column in calc_columns.keys():
        max_rows = diff_data_frame.nlargest(largest_rows, columns=[calc_columns[data_column]])
        worksheet.write_string(row_counter, col_counter, f"{data_column} {largest_rows} rows")
        row_counter += 1
        max_rows.to_excel(writer, sheet_name=sheet_name, startrow=row_counter, startcol=col_counter)
        row_counter = row_counter + 2 + len(max_rows.index)

    corr_dataframe = both_all[[data_column + side for data_column in data_columns for side in sides]]
    corr_matrix = corr_dataframe.corr()
    worksheet.write_string(row_counter, col_counter, "Correlation matrix of difference of common elements")
    corr_matrix.to_excel(writer, sheet_name=sheet_name, startrow=row_counter + 1, startcol=col_counter)


def compare_reports(report_a: ModelReport,
                    report_b: ModelReport,
                    output_folder: str = "./merge_reports") -> str:
    """
    One-line entry point: diff two ModelReports (counts, unique elements per side,
    numeric diffs, top-10 largest diffs, correlation matrix) into one Excel workbook.
    """
    folder_to_store = check_and_create_the_folder_path(output_folder)
    time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    full_file_name = f"{folder_to_store.removesuffix('/')}/merge_report_comparison_{time_moment_now}.xlsx"
    with pandas.ExcelWriter(full_file_name) as writer:
        for sheet_name, index_columns, data_columns, name_columns in COMPARISON_SPECS:
            compare_two_dataframes(report_one=report_a, report_two=report_b, index_columns=index_columns,
                                   data_columns=data_columns, name_columns=name_columns, sheet_name=sheet_name,
                                   writer=writer)
    return full_file_name


if __name__ == '__main__':
    # Disk-based CLI usage: point it at one or two paths, each either a folder/zip of raw
    # CGMES files (loaded into a network via pypowsybl) or a previously saved .xlsx report.
    import sys

    def _load_report(path: str, label: str) -> ModelReport:
        if path.lower().endswith('.xlsx'):
            return ModelReport(full_file_name=path, model_author=label)
        network = pypowsybl.network.load(path, parameters={"iidm.import.cgmes.source-for-iidm-id": "rdfID"})
        return generate_report_from_network(network=network, model_author=label)

    paths = sys.argv[1:]
    if not paths:
        print("Usage: python -m emf.model_merger.model_report <path_or_report_1> [<path_or_report_2>]")
        sys.exit(1)

    reports = [_load_report(path, label=f"model{i + 1}") for i, path in enumerate(paths[:2])]

    if len(reports) == 2:
        output_file = compare_reports(report_a=reports[0], report_b=reports[1])
        print(f"Comparison saved to {output_file}")
    else:
        print("Report saved")