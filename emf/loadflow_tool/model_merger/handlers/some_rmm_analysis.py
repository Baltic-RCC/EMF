import zipfile
from os import listdir
from os.path import isfile, join

import pandas
import triplets


def get_list_of_xml_zip_files_from_dir(folder_name: str):
    only_files = [join(folder_name, file_name) for file_name in listdir(folder_name)
                  if isfile(join(folder_name, file_name))
                  and ((zipfile.is_zipfile(join(folder_name, file_name)) and not file_name.endswith('.xlsx'))
                       or file_name.endswith('.xml'))]
    return only_files


def get_regional_tie_lines(original_data: pandas.DataFrame, regions: list, columns: list = None):
    line = (original_data.type_tableview('Line')
            .rename_axis('ID')
            .reset_index()
            .rename(columns={'IdentifiedObject.description': 'line.description'}))
    if columns:
        line = line[columns]
    tie_lines = line[line['line.description'].str.contains('tie line')]
    regs = [' ' + region.upper() for region in regions]
    regional_tie_lines = tie_lines[tie_lines['line.description'].str.upper().str.contains('|'.join(regs))]
    return regional_tie_lines


def get_tie_lines(original_data: pandas.DataFrame, regions: list, voltages: list,
                  cgm_ssh_data: pandas.DataFrame = None):
    regional_tie_lines = get_regional_tie_lines(original_data=original_data,
                                                regions=regions,
                                                columns=['ID', 'line.description'])
    topo_nodes = (original_data.type_tableview('TopologicalNode')
                  .rename_axis('TopologicalNode')
                  .reset_index()
                  .rename(columns={'IdentifiedObject.description':
                                       'Description'}))[['TopologicalNode',
                                                         'Description',
                                                         'TopologicalNode.ConnectivityNodeContainer',
                                                         # 'TopologicalNode.boundaryPoint',
                                                         'TopologicalNode.fromEndName',
                                                         'TopologicalNode.fromEndIsoCode',
                                                         'TopologicalNode.toEndName',
                                                         'TopologicalNode.toEndIsoCode']]
    tie_line_nodes = topo_nodes.merge(regional_tie_lines[['ID']]
                                      .rename(columns={'ID': 'TopologicalNode.ConnectivityNodeContainer'}),
                                      on='TopologicalNode.ConnectivityNodeContainer')
    tie_line_nodes = tie_line_nodes[tie_line_nodes['Description'].str.contains('|'.join(voltages))]
    terminals = (original_data.type_tableview('Terminal')
                 .rename_axis('Terminal').reset_index()
                 .rename(columns={'Terminal.TopologicalNode': 'TopologicalNode',
                                  'IdentifiedObject.name': 'Terminal.name',
                                  'ACDCTerminal.connected': 'pre_terminal.connected'}))[['Terminal',
                                                                                         'Terminal.name',
                                                                                         'pre_terminal.connected',
                                                                                         'TopologicalNode']]
    columns = ['Description',
               'TopologicalNode.fromEndName',
               'TopologicalNode.fromEndIsoCode',
               'TopologicalNode.toEndName',
               'TopologicalNode.toEndIsoCode',
               'Terminal.name',
               'pre_terminal.connected']
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        new_terminals = cgm_ssh_data[cgm_ssh_data['KEY'] == 'ACDCTerminal.connected'][['ID', 'VALUE']]
        terminals = terminals.merge(new_terminals.rename(columns={'ID': 'Terminal',
                                                                  'VALUE': 'post_terminal.connected'}),
                                    on='Terminal')
        columns.append('post_terminal.connected')
        node_terminals = tie_line_nodes.merge(terminals, on='TopologicalNode')
        # Filter those terminals that are not connected
        node_terminals = node_terminals[((node_terminals['pre_terminal.connected'] == 'false') |
                                         (node_terminals['post_terminal.connected'] == 'false'))]
    else:
        node_terminals = tie_line_nodes.merge(terminals, on='TopologicalNode')
        # Filter those terminals that are not connected
        node_terminals = node_terminals[(node_terminals['pre_terminal.connected'] == 'false')]
    final_result = node_terminals[columns]
    return final_result


def get_equivalent_injections(original_data: pandas.DataFrame,
                              cgm_ssh_data: pandas.DataFrame, regions: list):
    injections = (original_data.type_tableview('EquivalentInjection')
                  .rename_axis('EquivalentInjection').reset_index())
    region_names = get_regions_for_file_names(original_data=original_data, regions=regions)
    instances = original_data[(original_data['KEY'] == "Type") & (original_data['VALUE'] == "EquivalentInjection")]
    instances = instances.merge(region_names, on='INSTANCE_ID')[['ID', 'region']]
    instances = instances.drop_duplicates(subset='ID', keep='last')
    injections = injections.rename(columns={'IdentifiedObject.description': 'EquivalentInjection.Description',
                                            'IdentifiedObject.name': 'EquivalentInjection.Name'})
    injections = injections[['EquivalentInjection', 'EquivalentInjection.Name', 'EquivalentInjection.Description',
                             'Equipment.EquipmentContainer', 'EquivalentInjection.p', 'EquivalentInjection.q']]
    columns = ['EquivalentInjection.Name',
               # 'EquivalentInjection.Description',
               'region', 'line.name', 'line.description']

    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        post_injections = (cgm_ssh_data.type_tableview('EquivalentInjection')
                           .rename_axis('EquivalentInjection').reset_index())[['EquivalentInjection',
                                                                               'EquivalentInjection.p',
                                                                               'EquivalentInjection.q']]
        injections = injections.merge(post_injections, on='EquivalentInjection',
                                      how='outer', suffixes=('_pre', '_post'))
        columns.extend(['EquivalentInjection.p_pre', 'EquivalentInjection.q_pre',
                        'EquivalentInjection.p_post', 'EquivalentInjection.q_post'])
    else:
        columns.extend(['EquivalentInjection.p', 'EquivalentInjection.q'])
    injections = injections.merge(instances.rename(columns={'ID': 'EquivalentInjection'}), on='EquivalentInjection',
                                  how='left')
    regional_tie_lines = get_regional_tie_lines(original_data=original_data, regions=regions,
                                                columns=['ID', 'line.description', 'IdentifiedObject.name'])
    line_injections = injections.merge(regional_tie_lines
                                       .rename(columns={'ID': 'Equipment.EquipmentContainer',
                                                        'IdentifiedObject.name': 'line.name'}),
                                       on='Equipment.EquipmentContainer', how='right')
    line_injections = line_injections.sort_values('Equipment.EquipmentContainer')
    by_ru = line_injections[line_injections['line.description'].str.upper().str.contains('|'.join([' RU', ' BY']))]
    not_by_ru = pandas.concat([line_injections, by_ru]).drop_duplicates(keep=False)
    final_result = not_by_ru[columns]
    return final_result


# def get_equivalent_injections_2(original_data: pandas.DataFrame, cgm_sv_data: pandas.DataFrame,
#                                 cgm_ssh_data: pandas.DataFrame, regions: list):
#     cn_nodes = original_data.type_tableview('ConnectivityNode').rename_axis('ConnectivityNode').reset_index()
#     cn_nodes = cn_nodes[cn_nodes['ConnectivityNode.boundaryPoint'] == 'true']
#     region_nodes = cn_nodes[(cn_nodes['ConnectivityNode.fromEndIsoCode'].str.upper().str.contains('|'.join(regions)))
#                             | (cn_nodes['ConnectivityNode.toEndIsoCode'].str.upper().str.contains('|'.join(regions)))]


def filter_lines_by_destinations(row_value, destinations: list):
    for destination_pair in destinations:
        if all(x in row_value for x in destination_pair):
            return True
    return False


def get_active_flows_on_lines(original_data: pandas.DataFrame,
                              destinations: list,
                              cgm_sv_data: pandas.DataFrame = None):
    lines = original_data.type_tableview('Line').rename_axis('Line').reset_index()
    ac_line_segments = original_data.type_tableview('ACLineSegment').rename_axis('ACLineSegment').reset_index()
    ac_line_filter = ac_line_segments[ac_line_segments.apply(lambda x:
                                                             filter_lines_by_destinations(x['IdentifiedObject.name'],
                                                                                          destinations), axis=1)]
    line_filter = lines[lines.apply(lambda x: filter_lines_by_destinations(x['IdentifiedObject.name'], destinations),
                                    axis=1)]
    line_filter = line_filter.rename(columns={'IdentifiedObject.name': 'Line.name',
                                              'IdentifiedObject.shortName': 'Line.shortName'})[['Line',
                                                                                                'Line.name',
                                                                                                'Line.shortName']]
    lines_to_ac = line_filter.merge(ac_line_segments, left_on='Line', right_on='Equipment.EquipmentContainer',
                                    how='left')
    lines_to_ac = lines_to_ac[['Line', 'ACLineSegment', 'Line.name', 'Line.shortName']]
    ac_line_filter = ac_line_filter.rename(columns={'IdentifiedObject.name': 'Line.name',
                                                    'IdentifiedObject.shortName': 'Line.shortName'})[['ACLineSegment',
                                                                                                      'Line.name',
                                                                                                      'Line.shortName']]
    all_lines = pandas.concat([lines_to_ac, ac_line_filter])
    terminals = (original_data.type_tableview('Terminal').rename_axis('Terminal').reset_index()
                 .rename(columns={'IdentifiedObject.name': 'Terminal.name'}))
    terminals = terminals[['Terminal', 'Terminal.name',
                           'ACDCTerminal.connected', 'Terminal.ConductingEquipment']]
    all_lines = all_lines.merge(terminals.rename(columns={'Terminal.ConductingEquipment': 'ACLineSegment'}),
                                on='ACLineSegment', how='left')
    pre_power_flows = (original_data.type_tableview('SvPowerFlow').reset_index()
                       .rename(columns={'SvPowerFlow.Terminal': 'Terminal'}))
    all_lines = all_lines.merge(pre_power_flows, on='Terminal', how='left')
    columns = [
        # 'ACLineSegment',
        'Line.name', 'Line.shortName',
        'Terminal.name', 'ACDCTerminal.connected']
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        post_power_flows = (cgm_sv_data.type_tableview('SvPowerFlow').reset_index()
                            .rename(columns={'SvPowerFlow.Terminal': 'Terminal'}))
        all_lines = all_lines.merge(post_power_flows, on='Terminal', how='left', suffixes=('_pre', '_post'))
        columns.extend(['SvPowerFlow.p_pre', 'SvPowerFlow.q_pre', 'SvPowerFlow.p_post', 'SvPowerFlow.q_post'])
    else:
        columns.extend(['SvPowerFlow.p', 'SvPowerFlow.q'])
    all_lines = all_lines.dropna(subset=['ACLineSegment'])
    final_result = all_lines[columns]
    return final_result


def get_stations(original_data: pandas.DataFrame, stations: list):
    substations = (original_data.type_tableview('Substation')
                   .rename_axis('Substation.ID').reset_index()
                   .rename(columns={'IdentifiedObject.name': 'Substation.name'}))[['Substation.ID', 'Substation.name']]
    stations = [station.lower() for station in stations]
    substations_filtered = substations[substations['Substation.name'].str.lower().str.contains('|'.join(stations))]
    return substations_filtered


def get_hvdc_lines(original_data: pandas.DataFrame,
                   hvdc_line_list: list,
                   cgm_sv_data: pandas.DataFrame = None):
    hvdc_lines_formatted = ['HVDC ' + line_name for line_name in hvdc_line_list]
    line_names = [line_name.lower() for line_name in hvdc_line_list]
    hvdc_lines = original_data.type_tableview('Line').rename_axis('Line').reset_index()
    hvdc_lines = hvdc_lines[hvdc_lines['IdentifiedObject.description'].str.lower().str.contains('|'.join(line_names))]
    hvdc_lines = hvdc_lines.rename(columns={'IdentifiedObject.description': 'Line.name'})[['Line', 'Line.name']]
    con_nodes = original_data.type_tableview('ConnectivityNode').rename_axis('ConnectivityNode').reset_index()
    con_nodes = con_nodes.rename(columns={'IdentifiedObject.name': 'Node',
                                          'ConnectivityNode.ConnectivityNodeContainer': 'Line'})[['ConnectivityNode',
                                                                                                  'Line', 'Node']]
    con_nodes = con_nodes.merge(hvdc_lines, on='Line')
    terminals = original_data.type_tableview('Terminal').rename_axis('Terminal').reset_index()
    terminals = terminals.rename(columns={'IdentifiedObject.name': 'Terminal.name',
                                          'Terminal.ConnectivityNode': 'ConnectivityNode'})[['Terminal',
                                                                                             'Terminal.name',
                                                                                             'ConnectivityNode',
                                                                                             'ACDCTerminal.connected']]
    terminal_nodes = terminals.merge(con_nodes, on='ConnectivityNode')
    power_flows = original_data.type_tableview('SvPowerFlow').rename_axis('SvPowerFlow').reset_index()
    power_flows = power_flows.rename(columns={'SvPowerFlow.Terminal': 'Terminal'})[['Terminal',
                                                                                    # 'SvPowerFlow',
                                                                                    'SvPowerFlow.p',
                                                                                    'SvPowerFlow.q']]
    node_flows = terminal_nodes.merge(power_flows, on='Terminal', how='left')
    columns = ['Line.name', 'Terminal.name', 'ACDCTerminal.connected']
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        new_flows = cgm_sv_data.type_tableview('SvPowerFlow').rename_axis('SvPowerFlow').reset_index()
        new_flows = power_flows.rename(columns={'SvPowerFlow.Terminal': 'Terminal'})[['Terminal',
                                                                                      # 'SvPowerFlow',
                                                                                      'SvPowerFlow.p',
                                                                                      'SvPowerFlow.q']]
        node_flows = node_flows.merge(new_flows, on='Terminal', how='left', suffixes=('_pre', '_post'))
        columns.extend(['SvPowerFlow.p_pre', 'SvPowerFlow.q_pre', 'SvPowerFlow.p_post', 'SvPowerFlow.q_post'])
    else:
        columns.extend(['SvPowerFlow.p', 'SvPowerFlow.q'])
    final_result = node_flows[columns]
    final_result = final_result.sort_values(['Line.name'])
    return final_result


def get_regions_for_file_names(original_data: pandas.DataFrame, regions: list):
    regions = [region.upper() for region in regions]
    region_instances = original_data[(original_data['KEY'] == 'Type')
                                     & ((original_data['VALUE'] == 'ControlArea')
                                        | (original_data['VALUE'] == 'LoadArea')
                                        | (original_data['VALUE'] == 'GeographicalRegion'))][['ID']]
    region_names = (original_data[original_data['KEY'] == 'IdentifiedObject.name']).merge(region_instances, on='ID')
    region_names = (region_names.rename(columns={'VALUE': 'region'})
                    .merge(pandas.DataFrame(data=regions, columns=['region']), on='region'))[['region', 'INSTANCE_ID']]
    return region_names


def get_load(original_data: pandas.DataFrame, cgm_ssh_data: pandas.DataFrame, regions: list):
    conform_loads = (original_data.type_tableview('ConformLoad').rename_axis('Load').reset_index()
                     .rename(columns={'ConformLoad.LoadGroup': 'LoadGroup',
                                      'EnergyConsumer.p': 'ConformLoad.p',
                                      'EnergyConsumer.q': 'ConformLoad.q'}))[['Load', 'LoadGroup',
                                                                              'ConformLoad.p', 'ConformLoad.q']]
    non_conform_loads = (original_data.type_tableview('NonConformLoad').rename_axis('Load').reset_index()
                         .rename(columns={'NonConformLoad.LoadGroup': 'LoadGroup',
                                          'EnergyConsumer.p': 'NonConformLoad.p',
                                          'EnergyConsumer.q': 'NonConformLoad.q'}))[['Load', 'LoadGroup',
                                                                                     'NonConformLoad.p',
                                                                                     'NonConformLoad.q']]
    all_loads = pandas.concat([conform_loads, non_conform_loads])
    load_groups = original_data[original_data['KEY'] == 'LoadGroup.SubLoadArea'][['ID', 'INSTANCE_ID']]
    region_names = get_regions_for_file_names(original_data=original_data, regions=regions)
    load_groups = load_groups.merge(region_names, on='INSTANCE_ID').rename(columns={'ID': 'LoadGroup'})[['LoadGroup',
                                                                                                         'region']]
    all_loads = all_loads.merge(load_groups, on='LoadGroup')
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        conform_new = (original_data.type_tableview('ConformLoad').rename_axis('Load').reset_index()
                       .rename(columns={'EnergyConsumer.p': 'ConformLoad.p',
                                        'EnergyConsumer.q': 'ConformLoad.q'}))[['Load', 'ConformLoad.p',
                                                                                'ConformLoad.q']]
        non_conform_new = (original_data.type_tableview('NonConformLoad').rename_axis('Load').reset_index()
                           .rename(columns={'NonConformLoad.LoadGroup': 'LoadGroup',
                                            'EnergyConsumer.p': 'NonConformLoad.p',
                                            'EnergyConsumer.q': 'NonConformLoad.q'}))[['Load',
                                                                                       'NonConformLoad.p',
                                                                                       'NonConformLoad.q']]
        new_loads = pandas.concat([conform_new, non_conform_new])
        all_loads = all_loads.merge(new_loads, on='Load', how='left', suffixes=('_pre', '_post'))
        summed_loads = ((all_loads.groupby('region')[['ConformLoad.p_pre', 'ConformLoad.q_pre',
                                                      'ConformLoad.p_post', 'ConformLoad.q_post',
                                                      'NonConformLoad.p_pre', 'NonConformLoad.q_pre',
                                                      'NonConformLoad.p_post', 'NonConformLoad.q_post']].sum())
                        .rename_axis('region').reset_index())
    else:
        summed_loads = ((all_loads.groupby('region')[['ConformLoad.p', 'ConformLoad.q',
                                                      'NonConformLoad.p', 'NonConformLoad.q']].sum())
                        .rename_axis('region').reset_index())
    return summed_loads


def get_generation(original_data: pandas.DataFrame, cgm_ssh_data: pandas.DataFrame, regions: list):
    region_names = get_regions_for_file_names(original_data=original_data, regions=regions)
    all_generating_units = original_data[(original_data['KEY'] == 'Type')
                                         & (original_data['VALUE'].str.lower().str
                                            .contains('|'.join(['generating', 'unit'])))]
    regional_gen_units = (all_generating_units.merge(region_names, on='INSTANCE_ID')
                          .rename(columns={'ID': 'GeneratingUnit',
                                           'VALUE': 'GeneratingUnit.Type'}))[['GeneratingUnit',
                                                                              'GeneratingUnit.Type',
                                                                              'region']]
    sync_machines = (original_data.type_tableview('SynchronousMachine')
                     .rename_axis('SynchronousMachine').reset_index()
                     .rename(columns={'IdentifiedObject.name': 'SynchronousMachine.name',
                                      'RotatingMachine.GeneratingUnit': 'GeneratingUnit'}))
    sync_machines = sync_machines[['SynchronousMachine',
                                   'SynchronousMachine.name',
                                   'GeneratingUnit',
                                   'RotatingMachine.p',
                                   'RotatingMachine.q',
                                   # 'SynchronousMachine.type'
                                   'SynchronousMachine.operatingMode']]
    generators = sync_machines.merge(regional_gen_units, on='GeneratingUnit')
    generators = generators[generators['SynchronousMachine.operatingMode'].str.contains('generator')]
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        new_generations = (cgm_ssh_data.type_tableview('SynchronousMachine')
                           .rename_axis('SynchronousMachine').reset_index())[['SynchronousMachine',
                                                                              'RotatingMachine.p',
                                                                              'RotatingMachine.q',
                                                                              'SynchronousMachine.operatingMode']]
        new_generators = new_generations[new_generations['SynchronousMachine.operatingMode'].str.contains('generator')]
        generators = generators.merge(new_generators[['SynchronousMachine',
                                                      'RotatingMachine.p',
                                                      'RotatingMachine.q', ]], on='SynchronousMachine', how='left',
                                      suffixes=('_pre', '_post'))
        summed_generation = ((generators.groupby('region')[['RotatingMachine.p_pre',
                                                            'RotatingMachine.q_pre',
                                                            'RotatingMachine.p_post',
                                                            'RotatingMachine.q_post']].sum())
                             .rename_axis('region').reset_index())
    else:
        summed_generation = ((generators.groupby('region')[['RotatingMachine.p',
                                                            'RotatingMachine.q']].sum())
                             .rename_axis('region').reset_index())
    return summed_generation


def get_voltage_levels(original_data: pandas.DataFrame, stations: list, levels: list,
                       cgm_sv_data: pandas.DataFrame = None):
    substations_filtered = get_stations(original_data=original_data, stations=stations)
    all_voltages = (original_data.type_tableview('VoltageLevel')
                    .rename_axis('VoltageLevel.ID').reset_index()
                    .rename(columns={'IdentifiedObject.name': 'VoltageLevel.name',
                                     'VoltageLevel.Substation': 'Substation.ID'}))[['VoltageLevel.ID',
                                                                                    'VoltageLevel.name',
                                                                                    # 'VoltageLevel.BaseVoltage',
                                                                                    'Substation.ID']]
    voltages = all_voltages.merge(substations_filtered, on='Substation.ID')
    voltages = voltages[voltages['VoltageLevel.name'].str.lower().str.contains('|'.join(levels))]
    voltages = voltages.sort_values(['Substation.name'])
    topological_nodes = (original_data.type_tableview('TopologicalNode')
                         .rename_axis('TopologicalNode.ID')
                         .reset_index())[['TopologicalNode.ID', 'TopologicalNode.ConnectivityNodeContainer']]
    station_nodes = (topological_nodes.rename(columns={'TopologicalNode.ConnectivityNodeContainer': 'VoltageLevel.ID'})
                     .merge(voltages, on='VoltageLevel.ID'))
    pre_voltages = (original_data.type_tableview('SvVoltage')
                    .rename_axis('SvVoltage.pre.ID').reset_index()
                    .rename(columns={'SvVoltage.TopologicalNode': 'TopologicalNode.ID'}))[['TopologicalNode.ID',
                                                                                           'SvVoltage.angle',
                                                                                           'SvVoltage.v']]
    station_voltages = station_nodes.merge(pre_voltages, on='TopologicalNode.ID')
    columns = ['Substation.name', 'VoltageLevel.name']
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        post_voltages = (cgm_sv_data.type_tableview('SvVoltage')
                         .rename_axis('SvVoltage.post.ID').reset_index()
                         .rename(columns={'SvVoltage.TopologicalNode': 'TopologicalNode.ID'}))[['TopologicalNode.ID',
                                                                                                'SvVoltage.angle',
                                                                                                'SvVoltage.v']]
        station_voltages = station_voltages.merge(post_voltages, on='TopologicalNode.ID', suffixes=('_pre', '_post'))
        columns.extend(['SvVoltage.v_pre', 'SvVoltage.angle_pre', 'SvVoltage.v_post', 'SvVoltage.angle_post'])
        station_voltages = station_voltages[(abs(station_voltages['SvVoltage.v_pre']) != 0) |
                                            (abs(station_voltages['SvVoltage.v_post']) != 0)]
    else:
        columns.extend(['SvVoltage.v', 'SvVoltage.angle'])
        station_voltages = station_voltages[(abs(station_voltages['SvVoltage.v']) != 0)]
    station_voltages = station_voltages.sort_values(['Substation.name'])
    final_values = station_voltages[columns]
    return final_values


def get_transformers(original_data: pandas.DataFrame,
                     stations: list,
                     cgm_sv_data: pandas.DataFrame = None,
                     cgm_ssh_data: pandas.DataFrame = None):
    substations = get_stations(original_data=original_data, stations=stations)
    substations = substations.rename(columns={'Substation.ID': 'Equipment.EquipmentContainer'})
    transformers = original_data.type_tableview('PowerTransformer').rename_axis('Transformer').reset_index()
    station_transformers = substations.merge(transformers, on='Equipment.EquipmentContainer', how='left')
    station_transformers = (station_transformers
                            .rename(columns={'IdentifiedObject.name': 'Transformer.name',
                                             'IdentifiedObject.description': 'Transformer.description'}))
    station_transformers = station_transformers[['Transformer', 'Substation.name', 'Transformer.name']]
    transformer_ends = (original_data.type_tableview('PowerTransformerEnd').rename_axis('TransformerEnd').reset_index())
    transformer_ends = transformer_ends.rename(columns={'PowerTransformerEnd.PowerTransformer': 'Transformer'})
    transformer_ends = transformer_ends[['TransformerEnd', 'Transformer', 'TransformerEnd.endNumber']]
    station_transformers = station_transformers.merge(transformer_ends, on='Transformer', how='left')
    tap_changers = original_data.type_tableview('RatioTapChanger').rename_axis('RatioTapChanger').reset_index()
    tap_changers = tap_changers.rename(columns={'RatioTapChanger.TransformerEnd': 'TransformerEnd',
                                                'IdentifiedObject.name': 'TapChanger.name'})
    tap_changers = tap_changers[['RatioTapChanger', 'TransformerEnd',
                                 'TapChanger.name',
                                 # 'TapChanger.controlEnabled',
                                 'TapChanger.lowStep', 'TapChanger.highStep',
                                 'TapChanger.neutralStep', 'TapChanger.normalStep',
                                 'TapChanger.step']]
    station_transformers = station_transformers.merge(tap_changers, on='TransformerEnd', how='left')
    columns = ['Substation.name', 'Transformer.name', 'TransformerEnd.endNumber',
               'TapChanger.name', 'TapChanger.lowStep', 'TapChanger.highStep',
               'TapChanger.neutralStep', 'TapChanger.normalStep']
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        new_steps = cgm_ssh_data.type_tableview('RatioTapChanger').rename_axis('RatioTapChanger').reset_index()
        new_steps = new_steps[['RatioTapChanger', 'TapChanger.controlEnabled', 'TapChanger.step']]
        station_transformers = station_transformers.merge(new_steps, on='RatioTapChanger', suffixes=('_pre', '_post'))
        columns.extend(['TapChanger.step_pre', 'TapChanger.step_post'])
    else:
        columns.append('TapChanger.step')
    old_positions = (original_data.type_tableview('SvTapStep').reset_index()
                     .rename(columns={'SvTapStep.TapChanger': 'RatioTapChanger'}))[['RatioTapChanger',
                                                                                    'SvTapStep.position']]
    station_transformers = station_transformers.merge(old_positions, on='RatioTapChanger', how='left')
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        new_positions = (cgm_sv_data.type_tableview('SvTapStep').reset_index()
                         .rename(columns={'SvTapStep.TapChanger': 'RatioTapChanger'}))[['RatioTapChanger',
                                                                                        'SvTapStep.position']]
        station_transformers = station_transformers.merge(new_positions, on='RatioTapChanger', how='left',
                                                          suffixes=('_pre', '_post'))
        columns.extend(['SvTapStep.position_pre', 'SvTapStep.position_post'])
    else:
        columns.append('SvTapStep.position')
    final_result = station_transformers[columns]
    return final_result


if __name__ == '__main__':
    merged_model = './CGM1D_123_20240820T0130Z_EU_RMM'
    pypowsybl_file_list = get_list_of_xml_zip_files_from_dir(merged_model)
    rcc = 'BALTICRSC'
    separate_merged_part = True

    if separate_merged_part:
        ssh_files = [file_name for file_name in pypowsybl_file_list
                     if (rcc in file_name) and ('_SSH_' in file_name)]
        sv_files = [file_name for file_name in pypowsybl_file_list
                    if (rcc in file_name) and ('_SV_' in file_name)]
        model_files = [file_name for file_name in pypowsybl_file_list
                       if (file_name not in ssh_files) and (file_name not in sv_files)]
        sv_data = pandas.read_RDF(sv_files)
        ssh_data = pandas.read_RDF(ssh_files)
    else:
        sv_data = None
        ssh_data = None
        model_files = pypowsybl_file_list
    model_data = pandas.read_RDF(model_files)
    # points 1-4
    # HVDC lines
    some_hvdc_lines = ['Litpol', 'Nordbalt', 'Estlink 1', 'Estlink 2']
    hvdc_result = get_hvdc_lines(original_data=model_data,
                                 cgm_sv_data=sv_data,
                                 hvdc_line_list=some_hvdc_lines)
    print('HVDC lines')
    print(hvdc_result.to_string())
    # points 5 -7
    generation_for_region = get_generation(original_data=model_data,
                                           cgm_ssh_data=ssh_data,
                                           regions=['EE', 'LV', 'LT'])
    print('Generation')
    print(generation_for_region.to_string())
    # points 8-10
    print("Loads")
    loads_for_region = get_load(original_data=model_data,
                                cgm_ssh_data=ssh_data,
                                regions=['EE', 'LV', 'LT'])
    print(loads_for_region.to_string())
    # points 11-13
    test_tie_lines = get_tie_lines(original_data=model_data, cgm_ssh_data=ssh_data, regions=['EE', 'LV', 'LT'],
                                   voltages=['330'])
    print('Disconnected tie lines:')
    print(test_tie_lines.to_string())
    # points 14-16
    injections1 = get_equivalent_injections(original_data=model_data,
                                            cgm_ssh_data=ssh_data,
                                            regions=['EE', 'LV', 'LT'])
    print('Equivalent injections')
    print(injections1.to_string())
    # points 17-25

    some_lines = [('Bitenai', 'Sovietskas 1'),
                  ('Bitenai', 'Sovietskas 2'),
                  ('Klaipeda', 'Grobine'),
                  # ('Musa', 'Viskali'), # was not able to find it
                  ('Panevezys', 'Aizkriaukle'),
                  ('IAE', 'Liksna'),
                  ('RigaTec2', 'Kilingi-Nõmme'),
                  ('Tartu', 'Valmiera'),
                  ('Valmiera', 'Tsirguliina')]
    test_lines = get_active_flows_on_lines(original_data=model_data, cgm_sv_data=sv_data, destinations=some_lines)
    if isinstance(sv_data, pandas.DataFrame) and not sv_data.empty:
        summed_lines = (test_lines.groupby('Line.name')[['SvPowerFlow.p_pre',
                                                         'SvPowerFlow.q_pre',
                                                         'SvPowerFlow.p_post',
                                                         'SvPowerFlow.q_post']].sum()).rename_axis('Line').reset_index()
    else:
        summed_lines = (test_lines.groupby('Line.name')[['SvPowerFlow.p',
                                                         'SvPowerFlow.q']].sum()).rename_axis('Line').reset_index()
    print('Active flows on lines')
    print('all data')
    print(test_lines.to_string())
    print('Flows summed per line')
    print(summed_lines.to_string())
    # points 26-35
    voltage_level_stations = ['K-N�mme', 'Paide', 'Tsirguliina', 'Liksna', 'Valmiera',
                              'Grobina', 'Klaipeda', 'Alytus', 'Utena', 'Siauliai']
    voltage_levels = ['110', '330']
    voltage_levels = get_voltage_levels(original_data=model_data,
                                        cgm_sv_data=sv_data,
                                        stations=voltage_level_stations,
                                        levels=voltage_levels)
    print('Voltage levels at stations')
    print(voltage_levels.to_string())
    # points 36-41
    some_stations = ['Klaipeda', 'Panevezys', 'Viskali', 'Valmiera', 'Kiisa', 'Tartu']
    steps = get_transformers(original_data=model_data, cgm_sv_data=sv_data, cgm_ssh_data=ssh_data,
                             stations=some_stations)
    print('Transformer taps')
    print(steps.to_string())

    print("Done")
