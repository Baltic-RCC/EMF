import copy
import os.path
import zipfile
from datetime import datetime
from os import listdir
from os.path import isfile, join, basename

import pandas
import pypowsybl
import triplets

from emf.common.integrations.object_storage import file_system
from emf.common.integrations.object_storage.file_system_general import get_meta_from_filename, \
    check_and_create_the_folder_path
from emf.loadflow_tool.helper import load_model


def get_list_of_xml_zip_files_from_dir(folder_name: str):
    only_files = [join(folder_name, file_name) for file_name in listdir(folder_name)
                  if isfile(join(folder_name, file_name))
                  and ((zipfile.is_zipfile(join(folder_name, file_name)) and not file_name.endswith('.xlsx'))
                       or file_name.endswith('.xml'))]
    return only_files


def parse_filenames(original_models: pandas.DataFrame):
    labels = original_models[original_models['KEY'] == 'label']
    labels_reduced = copy.deepcopy(labels)
    labels.loc[:, 'KEY'] = 'original_filename'
    labels_reduced['VALUE'] = labels_reduced['VALUE'].apply(lambda x: basename(x))
    original_models = original_models.update_triplet_from_triplet(labels_reduced)
    original_models = original_models.update_triplet_from_triplet(labels)
    original_models = triplets.cgmes_tools.update_FullModel_from_filename(original_models)
    return original_models


if __name__ == '__main__':

    # merged_model = r"../workgroup_merge/models/CGM_merge_one"
    # merged_model = r"../workgroup_merge/models/CGM_merge_two"
    merged_model = r"../workgroup_merge/models/CGM_merge_three"
    pypowsybl_file_list = get_list_of_xml_zip_files_from_dir(merged_model)
    # overwrite this if multiple rcc are in same folder
    rcc = None
    separate_merged_part = True

    sv_files = []

    if separate_merged_part and rcc:
        try:
            ssh_files = [file_name for file_name in pypowsybl_file_list
                         if (rcc in file_name) and ('_SSH_' in file_name)]
            sv_files = [file_name for file_name in pypowsybl_file_list
                        if (rcc in file_name) and ('_SV_' in file_name)]
            model_files = [file_name for file_name in pypowsybl_file_list
                           if (file_name not in ssh_files) and (file_name not in sv_files)]
            sv_data = pandas.read_RDF(sv_files)
            ssh_data = pandas.read_RDF(ssh_files)

            sv_data = parse_filenames(sv_data)
            ssh_data = parse_filenames(ssh_data)
        except Exception:
            sv_data = None
            ssh_data = None
            model_files = pypowsybl_file_list
    else:
        sv_data = None
        ssh_data = None
        model_files = pypowsybl_file_list
    model_data = pandas.read_RDF(model_files)
    model_data = parse_filenames(model_data)
    merged_ssh_profiles = []
    merged_sv_profiles = []
    list_of_models = file_system.get_latest_models_and_download(path_to_directory=merged_model,
                                                                allow_merging_entities=True,
                                                                local_folder_for_examples=None)
    latest_boundary = file_system.get_latest_boundary(path_to_directory=merged_model,
                                                      local_folder_for_examples=None)
    for model in list_of_models:
        merged_ssh_component = None
        for component in model.get('opde:Component', {}):
            if component.get('opdm:Profile', {}).get('pmd:mergingEntity'):
                if component.get('opdm:Profile', {}).get('pmd:cgmesProfile') == 'SSH':
                    merged_ssh_component = component
                if component.get('opdm:Profile', {}).get('pmd:cgmesProfile') == 'SV':
                    merged_sv_profiles.append(model)
        if merged_ssh_component:
            copied_model = copy.deepcopy(model)
            model.get('opde:Component', {}).remove(merged_ssh_component)
            copied_model['opde:Component'] = [merged_ssh_component]
            merged_ssh_profiles.append(copied_model)
    valid_models = [model for model in list_of_models if model not in merged_sv_profiles]
    input_models = valid_models + [latest_boundary] + merged_ssh_profiles + merged_sv_profiles
    parameters = {}
    merged_model = load_model(input_models, parameters=parameters, skip_default_parameters=True)
    network_instance = merged_model.get('network')
    buses = network_instance.get_elements(element_type=pypowsybl.network.ElementType.BUS,
                                          all_attributes=True).reset_index()
    voltage_level = network_instance.get_elements(element_type=pypowsybl.network.ElementType.VOLTAGE_LEVEL,
                                                  all_attributes=True).reset_index()
    substations = network_instance.get_elements(element_type=pypowsybl.network.ElementType.SUBSTATION,
                                                all_attributes=True).reset_index()
    areas = network_instance.get_elements(element_type=pypowsybl.network.ElementType.AREA,
                                          all_attributes=True).reset_index()
    limits = network_instance.get_elements(element_type=pypowsybl.network.ElementType.OPERATIONAL_LIMITS,
                                           all_attributes=True).reset_index()
    voltage_level = voltage_level.rename(columns={'id': 'Voltage level id',
                                                  'name': 'Voltage level name',
                                                  'substation_id': 'Substation Id',
                                                  'nominal_v': 'Nominal V',
                                                  'high_voltage_limit': 'High Voltage Limit',
                                                  'low_voltage_limit': 'Low Voltage Limit',
                                                  'fictitious': 'Fictitious',
                                                  'topology_kind': 'Topology kind'})
    substations = substations[['id', 'name', 'TSO', 'geo_tags', 'country',
                               # 'CGMES.regionName',
                               'CGMES.subRegionId', 'CGMES.regionId']]
    substations = substations.rename(columns={'id': 'Substation Id',
                                              'geo_tags': 'GEO Tags',
                                              'name': 'Region name',
                                              'CGMES.subRegionId': 'Subregion Id',
                                              'CGMES.regionId': 'Region Id'})
    voltage_level_stations = voltage_level.merge(substations, on='Substation Id', how='left')
    # Get lines
    line_limits = limits[(limits['element_type'] == 'LINE') & (limits['acceptable_duration'] == -1) &
                         (limits['type'] == 'CURRENT')]
    line_limits_reduced = line_limits[['group_name', 'value']].rename(columns={'value': 'Imax'})
    lines = network_instance.get_elements(element_type=pypowsybl.network.ElementType.LINE,
                                          all_attributes=True).reset_index().sort_values('id')
    lines_side_one = lines[['id', 'name', 'p1', 'q1', 'i1', 'r', 'x', 'g1', 'b1',
                            'voltage_level1_id', 'bus1_id', 'connected1', 'selected_limits_group_1']]
    lines_side_one = lines_side_one.dropna()
    line_buses = buses[['id', 'name', 'v_mag', 'v_angle', 'connected_component', 'synchronous_component', 'fictitious']]
    line_buses = line_buses.rename(columns={'id': 'Bus id', 'name': 'Bus name', 'v_mag': 'Bus V',
                                            'v_angle': 'Bus angle'})
    lines_side_one = lines_side_one.rename(columns={'id': 'Line id',
                                                    'name': 'Line name',
                                                    'p1': 'P',
                                                    'q1': 'Q',
                                                    'i1': 'I',
                                                    'voltage_level1_id': 'Voltage level id',
                                                    'bus1_id': 'Bus id',
                                                    'connected1': 'connected',
                                                    'selected_limits_group_1': 'group_name',
                                                    'r': 'R',
                                                    'x': 'X',
                                                    'g1': 'G',
                                                    'b1': 'B'})
    lines_side_one['Side'] = 1
    # lines_side_one = line_side_one.merge(line_limits[[]])
    lines_side_two = lines[['id', 'name', 'r', 'x', 'g2', 'b2', 'p2', 'q2', 'i2', 'voltage_level2_id', 'bus2_id',
                            'connected2', 'selected_limits_group_2']]
    lines_side_two = lines_side_two.dropna()
    lines_side_two = lines_side_two.rename(columns={'id': 'Line id',
                                                    'name': 'Line name',
                                                    'p2': 'P',
                                                    'q2': 'Q',
                                                    'i2': 'I',
                                                    'voltage_level2_id': 'Voltage level id',
                                                    'bus2_id': 'Bus id',
                                                    'connected2': 'connected',
                                                    'selected_limits_group_2': 'group_name',
                                                    'r': 'R',
                                                    'x': 'X',
                                                    'g2': 'G',
                                                    'b2': 'B'})
    lines_side_two['Side'] = 2
    lines_report = pandas.concat([lines_side_one, lines_side_two]).reset_index().sort_values(by=['Line id', 'Side'])
    lines_report = lines_report.merge(line_buses, on='Bus id', how='left')
    lines_report = lines_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    lines_report = lines_report.merge(line_limits_reduced, on='group_name', how='left')
    lines_report = lines_report.drop(columns=['group_name']).sort_values(by=['Line id', 'Side'])
    # Simplified report missing loading
    final_lines_report = lines_report[['Line id', 'Line name', 'country', 'Side', 'P', 'Q', 'I', 'Imax', 'Nominal V',
                                       'Bus V', 'Bus angle', 'Bus id', 'Bus name', 'connected']]

    # Get tie lines
    tie_lines = network_instance.get_elements(element_type=pypowsybl.network.ElementType.TIE_LINE,
                                              all_attributes=True).reset_index()
    tie_lines_one = (tie_lines[['id', 'name', 'dangling_line1_id', 'pairing_key', 'ucte_xnode_code']]
                     .rename(columns={'dangling_line1_id': 'dangling_line_id'}))
    tie_lines_two = (tie_lines[['id', 'name', 'dangling_line2_id', 'pairing_key', 'ucte_xnode_code']]
                     .rename(columns={'dangling_line2_id': 'dangling_line_id'}))
    tie_lines_merged = pandas.concat([tie_lines_one, tie_lines_two])

    dangling_lines = network_instance.get_elements(element_type=pypowsybl.network.ElementType.DANGLING_LINE,
                                                   all_attributes=True).reset_index()
    dangling_lines = dangling_lines.rename(columns={'id': 'TieLine id',
                                                    'name': 'TieLine name',
                                                    'lineEnergyIdentificationCodeEIC': 'EIC',
                                                    'ucte_xnode_code': 'UCTE Xnode',
                                                    'boundary_p': 'Boundary P',
                                                    'boundary_q': 'Boundary Q',
                                                    'p0': 'P0',
                                                    'q0': 'Q0',
                                                    'p': 'P',
                                                    'q': 'Q',
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
                                                    'CGMES.ConnectivityNode_Boundary': 'Boundary ConnectivityNode'
                                                    })
    tie_lines_report = dangling_lines.merge(line_buses, on='Bus id', how='left')
    tie_lines_report = tie_lines_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    tie_lines_report = tie_lines_report.merge(line_limits_reduced, on='group_name', how='left')
    tie_lines_report = tie_lines_report.drop(columns=['group_name']).sort_values(by=['TieLine id'])
    tie_lines_report = tie_lines_report.merge(tie_lines_merged[['id', 'dangling_line_id']]
                                              .rename(columns={'dangling_line_id': 'TieLine id', 'id': 'Tie Line id'}),
                                              on='TieLine id', how='left')

    # Simplified report missing loading, HVDC and Imax
    final_tie_lines_report = tie_lines_report[['TieLine id', 'TieLine name', 'country', 'EIC', 'UCTE Xnode',
                                               'Boundary P', 'Boundary Q', 'P', 'Q',
                                               'Nominal V', 'Boundary V', 'Bus V',
                                               'Boundary angle', 'Bus angle',
                                               'I', 'Imax',
                                               'Bus id', 'Bus name',
                                               'Terminal', 'Boundary Terminal',
                                               'Boundary TopologicalNode', 'EquivalentInjection Terminal',
                                               'EquivalentInjection', 'Boundary ConnectivityNode',
                                               'connected', 'paired', 'HVDC', 'GEO Tags', 'Tie Line id', 'P0', 'Q0']]

    # transformers
    two_windings_tf = network_instance.get_elements(element_type=pypowsybl.network.ElementType.TWO_WINDINGS_TRANSFORMER,
                                                    all_attributes=True).reset_index()
    two_windings_side_one = two_windings_tf[['id', 'name', 'p1', 'q1', 'i1',
                                             'voltage_level1_id', 'bus1_id',
                                             'connected1',
                                             'selected_limits_group_1']].rename(columns={'p1': 'P',
                                                                                         'q1': 'Q',
                                                                                         'i1': 'I',
                                                                                         'voltage_level1_id': 'Voltage level id',
                                                                                         'bus1_id': 'Bus id',
                                                                                         'connected1': 'connected',
                                                                                         'selected_limits_group_1': 'group_name'})
    two_windings_side_one['Side'] = 1
    two_windings_side_two = two_windings_tf[['id', 'name', 'p2', 'q2', 'i2',
                                             'voltage_level2_id', 'bus2_id',
                                             'connected2',
                                             'selected_limits_group_2']].rename(columns={'p2': 'P',
                                                                                         'q2': 'Q',
                                                                                         'i2': 'I',
                                                                                         'voltage_level2_id': 'Voltage level id',
                                                                                         'bus2_id': 'Bus id',
                                                                                         'connected2': 'connected',
                                                                                         'selected_limits_group_2': 'group_name'})
    two_windings_side_two['Side'] = 2
    two_windings_merged = pandas.concat([two_windings_side_one, two_windings_side_two]).sort_values(by=['id', 'Side'])
    two_windings_merged = two_windings_merged.rename(columns={'id': 'TF id',
                                                              'name': 'TF name'})
    two_windings_report = two_windings_merged.merge(line_buses, on='Bus id', how='left')
    two_windings_report = two_windings_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    two_windings_report = two_windings_report.merge(line_limits_reduced, on='group_name', how='left')
    two_windings_report = two_windings_report.drop(columns=['group_name']).sort_values(by=['TF id', 'Side'])

    final_two_windings_report = two_windings_report[['TF id', 'TF name', 'country', 'Side',
                                                     'P', 'Q', 'I', 'Imax', 'Nominal V',
                                                     'Bus V', 'Bus angle', 'Bus id', 'Bus name', 'connected',
                                                     'GEO Tags']]

    three_windings_tf = network_instance.get_elements(
        element_type=pypowsybl.network.ElementType.THREE_WINDINGS_TRANSFORMER,
        all_attributes=True).reset_index()
    three_windings_side_one = three_windings_tf[['id', 'name', 'p1', 'q1', 'i1',
                                                 'voltage_level1_id', 'bus1_id',
                                                 'connected1',
                                                 'selected_limits_group_1']].rename(columns={'p1': 'P',
                                                                                             'q1': 'Q',
                                                                                             'i1': 'I',
                                                                                             'voltage_level1_id': 'Voltage level id',
                                                                                             'bus1_id': 'Bus id',
                                                                                             'connected1': 'connected',
                                                                                             'selected_limits_group_1': 'group_name'})
    three_windings_side_one['Side'] = 1
    three_windings_side_two = three_windings_tf[['id', 'name', 'p2', 'q2', 'i2',
                                                 'voltage_level2_id', 'bus2_id',
                                                 'connected2',
                                                 'selected_limits_group_2']].rename(columns={'p2': 'P',
                                                                                             'q2': 'Q',
                                                                                             'i2': 'I',
                                                                                             'voltage_level2_id': 'Voltage level id',
                                                                                             'bus2_id': 'Bus id',
                                                                                             'connected2': 'connected',
                                                                                             'selected_limits_group_2': 'group_name'})
    three_windings_side_two['Side'] = 2
    three_windings_side_three = three_windings_tf[['id', 'name', 'p3', 'q3', 'i3',
                                                   'voltage_level3_id', 'bus3_id',
                                                   'connected3',
                                                   'selected_limits_group_3']].rename(columns={'p3': 'P',
                                                                                               'q3': 'Q',
                                                                                               'i3': 'I',
                                                                                               'voltage_level3_id': 'Voltage level id',
                                                                                               'bus3_id': 'Bus id',
                                                                                               'connected3': 'connected',
                                                                                               'selected_limits_group_3': 'group_name'})
    three_windings_side_three['Side'] = 3
    three_windings_merged = pandas.concat([three_windings_side_one, three_windings_side_two,
                                           three_windings_side_three]).sort_values(by=['id', 'Side'])
    three_windings_merged = three_windings_merged.rename(columns={'id': 'TF id', 'name': 'TF name'})
    three_windings_report = three_windings_merged.merge(line_buses, on='Bus id', how='left')
    three_windings_report = three_windings_report.merge(voltage_level_stations, on='Voltage level id', how='left')
    three_windings_report = three_windings_report.merge(line_limits_reduced, on='group_name', how='left')
    three_windings_report = three_windings_report.drop(columns=['group_name']).sort_values(by=['TF id', 'Side'])

    final_three_windings_report = three_windings_report[['TF id', 'TF name', 'country', 'Side',
                                                         'P', 'Q', 'I', 'Imax', 'Nominal V',
                                                         'Bus V', 'Bus angle', 'Bus id', 'Bus name', 'connected',
                                                         'GEO Tags']]

    buses = network_instance.get_elements(element_type=pypowsybl.network.ElementType.BUS,
                                          all_attributes=True).reset_index()
    buses = buses.rename(columns={'id': 'Bus id',
                                  'name': 'Bus name',
                                  'v_mag': 'Bus V',
                                  'v_angle': 'Bus angle',
                                  'fictitious': 'Fictitious',
                                  'connected_component': 'connected component',
                                  'synchronous_component': 'synchronous component',
                                  'voltage_level_id': 'Voltage level id'})
    buses = buses.sort_values(by='Bus id').reset_index(drop=True)
    injections = network_instance.get_elements(element_type=pypowsybl.network.ElementType.INJECTION,
                                               all_attributes=True).reset_index()
    injections = injections[['id', 'bus_id', 'p', 'q', 'i', 'type']].rename(columns={'id': 'Injection id',
                                                                                     'bus_id': 'Bus id',
                                                                                     'type': 'Injection type',
                                                                                     'p': 'Pinj',
                                                                                     'q': 'Qinj',
                                                                                     'i': 'Iinj'})
    buses_report = buses.merge(injections, on='Bus id', how='left')
    buses_report = buses_report.merge(voltage_level_stations.drop(columns=['Fictitious']), on='Voltage level id',
                                      how='left')
    final_buses_report = buses_report[['Bus id', 'Bus name', 'country', 'Nominal V', 'Bus V', 'Bus angle',
                                       'Injection type', 'Pinj', 'Qinj', 'Iinj', 'Injection id',
                                       'Fictitious', 'Voltage level name', 'Substation Id',
                                       'High Voltage Limit', 'Low Voltage Limit', 'connected component',
                                       'synchronous component'
                                       ]]
    final_buses_report = final_buses_report.sort_values(by='Bus id').reset_index(drop=True)
    generators = network_instance.get_elements(element_type=pypowsybl.network.ElementType.GENERATOR,
                                               all_attributes=True).reset_index()
    generators = generators.rename(columns={'id': 'Generator id',
                                            'name': 'Generator name',
                                            'target_p': 'Target P',
                                            'p': 'P',
                                            'q': 'Q',
                                            'i': 'I',
                                            'voltage_regulator_on': 'Voltage Regulator On',
                                            'min_p': 'Min P',
                                            'max_p': 'Max P',
                                            'voltage_level_id': 'Voltage level id',
                                            'reactive_limits_kind': 'Reactive Limits Kind',
                                            'target_v': 'Target V',
                                            'target_q': 'Target Q',
                                            'min_q': 'Min Q',
                                            'max_q': 'Max Q',
                                            'min_q_at_target_p': 'Min Q at Target P',
                                            'max_q_at_target_p': 'Max Q at Target P',
                                            'rated_s': 'Rated S',
                                            'regulated_element_id': 'Regulated Element id',
                                            'bus_id': 'Bus id',
                                            'connected': 'Connected',
                                            'fictitious': 'Fictitious',
                                            'CGMES.synchronousMachineType': 'Synchronous Machine Type',
                                            'CGMES.synchronousMachineOperatingMode': 'SynchronousMachineOperatingMode',
                                            'CGMES.GeneratingUnit': 'GeneratingUnit',
                                            'CGMES.RegulatingControl': 'RegulatingControl'
                                            })
    generators_report = generators.merge(voltage_level_stations.drop(columns=['Fictitious']),
                                         on='Voltage level id', how='left')
    final_generators_report = generators_report[['Generator id', 'Generator name', 'country',
                                                 'Target P', 'P', 'Q', 'I',
                                                 'Voltage Regulator On', 'Min P', 'Max P', 'Reactive Limits Kind',
                                                 'Target V', 'Target Q', 'Min Q', 'Max Q',
                                                 'Min Q at Target P', 'Max Q at Target P', 'Rated S',
                                                 'Regulated Element id', 'Bus id', 'Connected', 'Fictitious',
                                                 'Synchronous Machine Type', 'SynchronousMachineOperatingMode',
                                                 'GeneratingUnit', 'RegulatingControl']]
    final_generators_report = final_generators_report.sort_values(by='Generator id').reset_index(drop=True)

    loads = network_instance.get_elements(element_type=pypowsybl.network.ElementType.LOAD,
                                          all_attributes=True).reset_index()
    loads = loads.rename(columns={'id': 'Load id',
                                  'name': 'Load name',
                                  'p0': 'P0',
                                  'q0': 'Q0',
                                  'p': 'P',
                                  'q': 'Q',
                                  'i': 'I',
                                  'voltage_level_id': 'Voltage level id',
                                  'bus_id': 'Bus id',
                                  'connected': 'Connected',
                                  'fictitious': 'Fictitious',
                                  'CGMES.originalClass': 'OriginalClass'})
    loads_report = loads.merge(voltage_level_stations.drop(columns=['Fictitious']),
                               on='Voltage level id', how='left')
    loads_report = loads_report.merge(line_buses, on='Bus id', how='left')
    final_loads_report = loads_report[['Load id', 'Load name', 'country', 'P0', 'Q0', 'P', 'Q', 'I', 'Nominal V',
                                       'Bus V', 'Bus angle', 'Connected', 'Fictitious', 'OriginalClass', 'Bus name',
                                       'Voltage level id', 'Bus id', 'GEO Tags']]
    final_loads_report = final_loads_report.sort_values(by='Load id').reset_index(drop=True)

    shunt_compensators = network_instance.get_elements(element_type=pypowsybl.network.ElementType.SHUNT_COMPENSATOR,
                                                       all_attributes=True).reset_index()
    shunt_compensators = shunt_compensators.rename(columns={'id': 'ShuntCompensator id',
                                                            'name': 'ShuntCompensator name',
                                                            'model_type': 'Model type',
                                                            'p': 'P',
                                                            'q': 'Q',
                                                            'i': 'I',
                                                            'voltage_regulation_on': 'Voltage regulation on',
                                                            'regulating_bus_id': 'Regulating bus id',
                                                            'CGMES.RegulatingControl': 'RegulatingControl',
                                                            'target_v': 'Target V',
                                                            'target_deadband': 'Target Deadband',
                                                            'bus_id': 'Bus id',
                                                            'voltage_level_id': 'Voltage level id'})
    shunt_compensator_report = shunt_compensators.merge(voltage_level_stations, on='Voltage level id', how='left')
    shunt_compensator_report = shunt_compensator_report.merge(line_buses, on='Bus id', how='left')
    final_shunt_compensator_report = shunt_compensator_report[['ShuntCompensator id', 'ShuntCompensator name',
                                                               'country', 'Model type', 'P', 'Q', 'I',
                                                               'Voltage regulation on', 'Regulating bus id',
                                                               'RegulatingControl', 'Target V', 'Target Deadband',
                                                               'Bus id', 'Bus name', 'Bus V', 'Bus angle', 'Nominal V',
                                                               'GEO Tags']]
    final_shunt_compensator_report = (final_shunt_compensator_report.sort_values(by='ShuntCompensator id')
                                      .reset_index(drop=True))
    sv_file_name = None
    folder_to_store = r"../workgroup_merge/reports"
    folder_to_store = check_and_create_the_folder_path(folder_to_store)
    time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    file_name_all = f"fixes_{time_moment_now}.csv"
    try:
        sv_file_name = os.path.basename(str(sv_files[0]))
    except Exception:
        sv_file_name = [file_name for file_name in pypowsybl_file_list if '_SV_' in file_name][0]
        if sv_file_name:
            sv_file_name = os.path.basename(sv_file_name)
    sv_meta = {}
    if sv_file_name:
        sv_meta = get_meta_from_filename(sv_file_name)
    full_file_name = f"merge_report_{sv_meta.get('pmd:mergingEntity', '1')}_{time_moment_now}.xlsx"
    full_file_name = folder_to_store.removesuffix('/') + '/' + full_file_name.removeprefix('/')
    print(f"Saving {full_file_name}")
    with pandas.ExcelWriter(full_file_name) as writer:
        final_lines_report.to_excel(writer, sheet_name='line')
        final_tie_lines_report.to_excel(writer, sheet_name='tieline')
        final_two_windings_report.to_excel(writer, sheet_name='two_windings_transformer')
        final_three_windings_report.to_excel(writer, sheet_name='three_windings_transformer')
        final_buses_report.to_excel(writer, sheet_name='bus')
        buses.to_excel(writer, sheet_name='bus_original')
        final_generators_report.to_excel(writer, sheet_name='generator')
        final_loads_report.to_excel(writer, sheet_name='load')
        final_shunt_compensator_report.to_excel(writer, sheet_name='shunt_compensator')

    print("Done")
