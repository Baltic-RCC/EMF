"""
Need to calculate
- Active and reactive load for each area
- Generation per area
- x-border flows on merged model
- AC NP
- NP (AC NP and HVDC)
- voltage/limit violations
- outages
"""
import os
import sys
import zipfile
from datetime import datetime
from enum import Enum
from os import listdir
from os.path import isfile, join

import pandas
import triplets
import logging

from emf.common.loadflow_tool.helper import get_metadata_from_filename

logger = logging.getLogger(__name__)


def get_list_of_xml_zip_files_from_dir(folder_name: str):
    """
    Lists zip files from folder, useful when running from local
    :param folder_name: name of the folder
    :return list of file names
    """
    only_files = [join(folder_name, file_name) for file_name in listdir(folder_name)
                  if isfile(join(folder_name, file_name))
                  and ((zipfile.is_zipfile(join(folder_name, file_name)) and not file_name.endswith('.xlsx'))
                       or file_name.endswith('.xml'))]
    return only_files


FILE_IDENTIFIER = 'Model.modelingAuthoritySet'
REGION_NAME_KEYWORD = 'region'


class RegionTypeForFileName(Enum):
    """
    As there are not a good common way to get region names to files then there are some options
    """
    TSO_NAMES = 1
    CONTROL_AREAS = 2
    REGIONS = 3


def normalize_modeling_entity(input_string: str, delimiter: str = '-'):
    """
    For getting tso name from file name (for merged cgm and individual igm)
    :param input_string: file name or part of it
    :param delimiter: character for separating parts
    :return tso name
    """
    entity_components = input_string.split(delimiter)
    if len(entity_components) == 3:
        input_string = entity_components[2]
    elif len(entity_components) == 2:
        input_string = entity_components[0]
    return input_string


def get_tso_names_for_file_names(original_data: pandas.DataFrame):
    """
    Gets tso names to files
    :param original_data: igm/cgm in triplets format
    :return dataframe with file ids and tso names
    """
    labels = original_data[original_data['KEY'] == 'label']
    labels['VALUE'] = labels['VALUE'].apply(lambda x: os.path.basename(x))
    original_data = triplets.rdf_parser.update_triplet_from_triplet(original_data, labels)
    original_data = triplets.cgmes_tools.update_FullModel_from_filename(original_data)
    modeling_entities = original_data[original_data['KEY'] == 'Model.modelingEntity']
    regions_to_ids = modeling_entities[['VALUE', 'INSTANCE_ID']].rename(columns={'VALUE': REGION_NAME_KEYWORD})
    regions_to_ids[REGION_NAME_KEYWORD] = (regions_to_ids[REGION_NAME_KEYWORD]
                                           .apply(lambda x: normalize_modeling_entity(x)))
    return regions_to_ids


def get_region_names_for_file_names(original_data: pandas.DataFrame):
    """
    Tries to get region names to files
    :param original_data: igm/cgm in triplets format
    :return dataframe with file ids and region names
    """
    try:
        geographical_regions = (original_data
                                .type_tableview('GeographicalRegion')
                                .rename(columns={'IdentifiedObject.name': REGION_NAME_KEYWORD,
                                                 'IdentifiedObject.description': 'Description',
                                                 'IdentifiedObject.energyIdentCodeEic': 'energyIdentCodeEic',
                                                 'NetworkRegion.masUri': FILE_IDENTIFIER
                                                 })
                                .reset_index())[[FILE_IDENTIFIER,
                                                 'Description',
                                                 'energyIdentCodeEic',
                                                 REGION_NAME_KEYWORD]]
        geographical_regions[FILE_IDENTIFIER] = geographical_regions[FILE_IDENTIFIER].astype(str).str.lower()
        file_ids = original_data[original_data['KEY'] == FILE_IDENTIFIER]
        file_ids.loc[:, 'VALUE'] = file_ids['VALUE'].astype(str).str.lower()
        regions_to_ids = (file_ids[['VALUE', 'INSTANCE_ID']]
                          .rename(columns={'VALUE': FILE_IDENTIFIER})
                          .merge(geographical_regions,
                                 on=FILE_IDENTIFIER, how='left'))
    except KeyError:
        regions_to_ids = pandas.DataFrame()
    return regions_to_ids


def get_control_areas_for_file_names(original_data: pandas.DataFrame):
    """
    Tries to get control area names to files
    :param original_data: igm/cgm in triplets format
    :return dataframe with file ids and control area names
    """
    control_area_names = original_data.type_tableview('ControlArea').reset_index()

    interchange_type = "http://iec.ch/TC57/2013/CIM-schema-cim16#ControlAreaTypeKind.Interchange"
    control_area_names = control_area_names[control_area_names['ControlArea.type'] == interchange_type]
    modeling_authorities = (((original_data[original_data['KEY'] == FILE_IDENTIFIER])[['VALUE', 'INSTANCE_ID']])
                            .rename(columns={'VALUE': FILE_IDENTIFIER}))
    control_area_ids = ((original_data[(original_data['KEY'] == 'Type') &
                                       (original_data['VALUE'] == 'ControlArea')])[['ID', 'INSTANCE_ID']])
    control_area_ids = (control_area_ids.merge(modeling_authorities,
                                               on='INSTANCE_ID', how='left')[['ID', FILE_IDENTIFIER]]).drop_duplicates()
    control_area_names = control_area_names.merge(control_area_ids, on='ID', how='left')
    regions_to_ids = (control_area_names.rename(columns={'IdentifiedObject.name':
                                                             REGION_NAME_KEYWORD}))[[REGION_NAME_KEYWORD,
                                                                                     FILE_IDENTIFIER]]
    regions_to_ids = regions_to_ids.merge(modeling_authorities, on=FILE_IDENTIFIER)[[REGION_NAME_KEYWORD,
                                                                                     'INSTANCE_ID']]
    return regions_to_ids


def get_identifiers_for_file_names(original_data: pandas.DataFrame,
                                 return_type: RegionTypeForFileName = RegionTypeForFileName.TSO_NAMES):
    """
    Tries to get identifiers (tso names or regions or control area names) to files
    :param original_data: igm/cgm in triplets format
    :param return_type: specify which type (tso name, region name, control area name) should be used
    :return dataframe with file ids and control area names
    """
    match return_type:
        case RegionTypeForFileName.TSO_NAMES:
            regions_to_ids = get_tso_names_for_file_names(original_data)
        case RegionTypeForFileName.CONTROL_AREAS:
            regions_to_ids = get_control_areas_for_file_names(original_data)
        case RegionTypeForFileName.REGIONS:
            regions_to_ids = get_region_names_for_file_names(original_data)
        case _:
            regions_to_ids = get_tso_names_for_file_names(original_data)
    if regions_to_ids.empty:
        regions_to_ids = get_tso_names_for_file_names(original_data)
    modeling_authorities = (((original_data[original_data['KEY'] == FILE_IDENTIFIER])[['VALUE', 'INSTANCE_ID']])
                            .rename(columns={'VALUE': FILE_IDENTIFIER}))
    regions_to_ids = regions_to_ids.merge(modeling_authorities, on='INSTANCE_ID', how='left')
    return regions_to_ids


def sum_grouped_rows(x: pandas.DataFrame, columns: list = None):
    """
    Sums grouped columns
    :param x: input data
    :param columns: list of columns
    :return dataframe with columns summed
    """
    if not columns:
        x = x.apply(pandas.to_numeric, errors='coerce')
        return x.sum()
    else:
        x[columns] = x[columns].apply(pandas.to_numeric, errors='coerce', axis=1)
        return x[columns].sum()


def query_summed_value_by_instance_id(models_data: pandas.DataFrame, field_name: str):
    """
    Sums dataframe by field indicated and by INSTANCE_ID
    :param models_data: input data
    :param field_name: field to be summed
    :return dataframe with columns summed
    """
    response = ((((models_data.query("KEY ==@field_name")).
                  rename(columns={'VALUE': field_name}))[[field_name, 'INSTANCE_ID']])
                .groupby(['INSTANCE_ID'])[field_name].apply(lambda x: sum_grouped_rows(x)))
    response = response.drop(columns=['INSTANCE_ID']).reset_index()
    return response


def attach_file_identifiers(models_data: pandas.DataFrame, output_data, file_identifier: str = FILE_IDENTIFIER):
    """
    Sums dataframe by field indicated and by INSTANCE_ID
    :param models_data: input data
    :param output_data: dataframe where to add file identifiers
    :param file_identifier: File identifier string
    :return updated output data
    """
    model_authorities = (models_data.query('KEY == @file_identifier')
                         .rename(columns={'VALUE': FILE_IDENTIFIER}))[[file_identifier, 'INSTANCE_ID']]
    output_data = output_data.merge(model_authorities, on='INSTANCE_ID', how='left')
    output_data = output_data.drop(columns=['INSTANCE_ID'])
    return output_data


def sum_fields_from_triplets(original_data: pandas.DataFrame,
                             field_names: list | str):
    """
    Sums dataframe by fields indicated
    :param original_data: input data
    :param field_names: dataframe where to add file identifiers
    :return summed output data
    """
    original_output = pandas.DataFrame()
    if isinstance(field_names, str):
        field_names = [field_names]
    for field in field_names:
        field_output = query_summed_value_by_instance_id(models_data=original_data, field_name=field)
        if original_output.empty:
            original_output = field_output
        else:
            original_output = original_output.merge(field_output, on='INSTANCE_ID')
    original_output = attach_file_identifiers(models_data=original_data, output_data=original_output)
    return original_output


def get_generation_reduced(original_data: pandas.DataFrame,
                           cgm_ssh_data: pandas.DataFrame = pandas.DataFrame(),
                           regions: list | pandas.DataFrame = pandas.DataFrame()):
    """
    Gets summed generation for regions
    :param original_data: input data
    :param cgm_ssh_data: cgm ssh in triplets if provided
    :param regions: dataframe with region identifiers if provided
    :return summed output data
    """
    sum_names = ['RotatingMachine.p', 'RotatingMachine.q']
    producers = sum_fields_from_triplets(original_data=original_data,
                                         field_names=sum_names)
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        cgm_producers = sum_fields_from_triplets(original_data=cgm_ssh_data,
                                                 field_names=sum_names)
        if not producers.empty:
            producers = producers.merge(cgm_producers, on=FILE_IDENTIFIER, how='outer', suffixes=('_igm', '_cgm'))
            sum_names = [column_name + suffix for column_name in sum_names for suffix in ('_igm', '_cgm')]
        else:
            producers = cgm_producers
    if not isinstance(regions, pandas.DataFrame) or regions.empty:
        regions = get_identifiers_for_file_names(original_data)
    if not regions.empty:
        producers[FILE_IDENTIFIER] = producers[FILE_IDENTIFIER].astype(str).str.lower()
        regions[FILE_IDENTIFIER] = regions[FILE_IDENTIFIER].astype(str).str.lower()
        producers = producers.merge(regions[[REGION_NAME_KEYWORD, FILE_IDENTIFIER]]
                                    .drop_duplicates(keep='last'), on=FILE_IDENTIFIER, how='left')
        sum_names = [REGION_NAME_KEYWORD] + sum_names
    return producers[sum_names]


def get_load_reduced(original_data: pandas.DataFrame,
                     cgm_ssh_data: pandas.DataFrame = pandas.DataFrame(),
                     regions: list | pandas.DataFrame = pandas.DataFrame()):
    """
    Gets summed load for regions
    :param original_data: input data
    :param cgm_ssh_data: cgm ssh in triplets if provided
    :param regions: dataframe with region identifiers if provided
    :return summed output data
    """
    sum_names = ['EnergyConsumer.p', 'EnergyConsumer.q']
    consumers = sum_fields_from_triplets(original_data=original_data,
                                         field_names=sum_names)
    if isinstance(cgm_ssh_data, pandas.DataFrame) and not cgm_ssh_data.empty:
        cgm_consumers = sum_fields_from_triplets(original_data=cgm_ssh_data,
                                                 field_names=sum_names)
        if not consumers.empty:
            consumers = consumers.merge(cgm_consumers, on=FILE_IDENTIFIER, how='outer', suffixes=('_igm', '_cgm'))
            sum_names = [column_name + suffix for column_name in sum_names for suffix in ('_igm', '_cgm')]
        else:
            consumers = cgm_consumers
    if not isinstance(regions, pandas.DataFrame) or regions.empty:
        regions = get_identifiers_for_file_names(original_data)
    if not regions.empty:
        consumers[FILE_IDENTIFIER] = consumers[FILE_IDENTIFIER].astype(str).str.lower()
        regions[FILE_IDENTIFIER] = regions[FILE_IDENTIFIER].astype(str).str.lower()
        consumers = consumers.merge(regions[[REGION_NAME_KEYWORD, FILE_IDENTIFIER]]
                                    .drop_duplicates(keep='last'), on=FILE_IDENTIFIER, how='left')
        sum_names = [REGION_NAME_KEYWORD] + sum_names
    return consumers[sum_names]


def get_net_position_values(original_data: pandas.DataFrame,
                            cgm_ssh_data: pandas.DataFrame,
                            cgm_sv_data: pandas.DataFrame,
                            regions: list | pandas.DataFrame):
    """
    An attempt to calculate the net interchange 2 values and check them against those provided in ssh profiles
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param original_data: original profiles
    :param regions: specify threshold if needed
    :return (updated) ssh profiles
    """
    try:
        control_areas = (original_data.type_tableview('ControlArea')
                         .rename_axis('ControlArea')
                         .reset_index())[['ControlArea', 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                          'IdentifiedObject.energyIdentCodeEic', 'IdentifiedObject.name']]
    except KeyError:
        control_areas = original_data.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        ssh_areas = cgm_ssh_data.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        control_areas = control_areas.merge(ssh_areas, on='ControlArea')[['ControlArea', 'ControlArea.netInterchange',
                                                                          'ControlArea.pTolerance',
                                                                          'IdentifiedObject.energyIdentCodeEic',
                                                                          'IdentifiedObject.name']]
    control_area_regions = original_data[(original_data['KEY'] == 'Type') & (original_data['VALUE'] == 'ControlArea')][['ID', 'INSTANCE_ID']]

    control_area_regions = control_area_regions.merge(regions, on='INSTANCE_ID', how='left')
    control_area_regions = control_area_regions.drop(columns=['INSTANCE_ID'])
    control_area_regions = control_area_regions.drop_duplicates(keep='last')
    control_areas = control_areas.merge(control_area_regions.rename(columns={'ID': 'ControlArea'}), on='ControlArea', how='left')
    tie_flows = (original_data.type_tableview('TieFlow')
                 .rename_axis('TieFlow').rename(columns={'TieFlow.ControlArea': 'ControlArea',
                                                         'TieFlow.Terminal': 'Terminal'})
                 .reset_index())[['ControlArea', 'Terminal', 'TieFlow.positiveFlowIn']]
    tie_flows = tie_flows.merge(control_areas[['ControlArea']], on='ControlArea')
    try:
        terminals = (original_data.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal', 'ACDCTerminal.connected']]
    except KeyError:
        terminals = (original_data.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal']]
    tie_flows = tie_flows.merge(terminals, on='Terminal')
    try:
        power_flows_pre = (original_data.type_tableview('SvPowerFlow')
                           .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                           .reset_index())[['Terminal', 'SvPowerFlow.p']]
        tie_flows = tie_flows.merge(power_flows_pre, on='Terminal', how='left')
    except Exception:
        logger.error(f"Was not able to get tie flows from original models")
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        power_flows_post = (cgm_sv_data.type_tableview('SvPowerFlow')
                            .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                            .reset_index())[['Terminal', 'SvPowerFlow.p']]

        tie_flows = tie_flows.merge(power_flows_post, on='Terminal', how='left',
                                    suffixes=('_pre', '_post'))
    try:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p_pre', 'SvPowerFlow.p_post']]
                              .agg(lambda x: pandas.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
    except KeyError:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p']]
                              .agg(lambda x: pandas.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
        # tie_flows_grouped = tie_flows_grouped.rename(columns={'SvPowerFlow.p': 'SvPowerFlow.p_post'})
    tie_flows_grouped = control_areas.merge(tie_flows_grouped, on='ControlArea')
    try:
        net_positions = tie_flows_grouped[[REGION_NAME_KEYWORD, 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                           'SvPowerFlow.p_pre', 'SvPowerFlow.p_post']]
    except KeyError:
        net_positions = tie_flows_grouped[[REGION_NAME_KEYWORD, 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                           'SvPowerFlow.p']]
    return net_positions


def merge_sort_strings(row, col1, col2, delimiter='-'):
    """
    For re-arranging column names
    :param row: input data (series)
    :param col1: first column name
    :param col2: second column name
    :param delimiter: delimiter
    :return new column heading
    """
    return delimiter.join(sorted([row[col1], row[col2]]))


def get_tieflow_data_reduced(original_data: pandas.DataFrame,
                             cgm_ssh_data: pandas.DataFrame = pandas.DataFrame(),
                             cgm_sv_data: pandas.DataFrame=pandas.DataFrame(),
                             regions: list | pandas.DataFrame = pandas.DataFrame()):
    """
    For getting tieflow data
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param original_data: original profiles
    :param regions: specify threshold if needed
    :return ac, dc and ac/dc positions
    """

    logger.info("Getting Tieflow Data")
    control_area_index = 'TieFlow.ControlArea'
    tie_flow_index = 'TieFlow'
    terminal_index = 'TieFlow.Terminal'
    topology_node_index = 'Terminal.TopologicalNode'
    tn_line_index = 'TopologicalNode.ConnectivityNodeContainer'
    powerflow_index = 'PowerFlow'
    cgm_suffixes = ('_igm', '_cgm')

    if not isinstance(regions, pandas.DataFrame) or regions.empty:
        regions = get_identifiers_for_file_names(original_data)
    try:
        control_areas = (original_data.type_tableview('ControlArea')
                         .rename_axis(control_area_index)
                         .rename(columns={'IdentifiedObject.name': 'ControlArea.name',
                                          'IdentifiedObject.energyIdentCodeEic': 'ControlArea.EIC'
                                          })
                         .reset_index())[[control_area_index, 'ControlArea.netInterchange',
                                          'ControlArea.pTolerance', 'ControlArea.type',
                                          'ControlArea.name', 'ControlArea.EIC']]
    except KeyError:
        control_areas = (cgm_ssh_data.type_tableview('ControlArea')
                         .rename_axis(control_area_index)
                         .rename(columns={'IdentifiedObject.name': 'ControlArea.name',
                                          # 'IdentifiedObject.energyIdentCodeEic': 'ControlArea.EIC'
                                          })
                         .reset_index())[[control_area_index,
                                          'ControlArea.netInterchange',
                                          'ControlArea.pTolerance',
                                          # 'ControlArea.type',
                                          'ControlArea.name',
                                          # 'ControlArea.EIC'
                                          ]]

    tie_flows = original_data.type_tableview('TieFlow').rename_axis(tie_flow_index).reset_index().drop(columns=['Type'])

    tie_flow_names = (original_data[original_data['KEY'] == 'Type']).merge(tie_flows[[tie_flow_index]],
                                                                           left_on='ID',
                                                                           right_on=tie_flow_index)[[tie_flow_index,
                                                                                                     'INSTANCE_ID']]
    tie_flows = tie_flows.merge(tie_flow_names, on=tie_flow_index).merge(regions, on='INSTANCE_ID')


    terminals = (original_data.type_tableview('Terminal')
                 .rename_axis(terminal_index)
                 .reset_index())[[terminal_index,
                                  'ACDCTerminal.connected',
                                  'Terminal.ConductingEquipment',
                                  # 'Terminal.ConnectivityNode',
                                  'Terminal.TopologicalNode'
                                  ]]

    boundary_nodes = original_data.type_tableview('TopologicalNode').rename_axis(topology_node_index).reset_index()
    boundary_tn_nodes = boundary_nodes[boundary_nodes['TopologicalNode.boundaryPoint'] == 'true']

    tn_lines = (original_data.type_tableview('Line')
                .rename_axis(tn_line_index)
                .rename(columns={'IdentifiedObject.description': 'Line.description'})
                .reset_index())[[tn_line_index, 'Line.description']]

    power_flows = (original_data.type_tableview('SvPowerFlow')
                   .rename_axis(powerflow_index)
                   .reset_index()
                   .rename(columns={'SvPowerFlow.Terminal': terminal_index})
                   .drop(columns=['Type']))

    flow_columns = ["SvPowerFlow.p", "SvPowerFlow.q"]
    if isinstance(cgm_sv_data, pandas.DataFrame) and not cgm_sv_data.empty:
        cgm_power_flows = (cgm_sv_data.type_tableview('SvPowerFlow')
                           .rename_axis(powerflow_index)
                           .reset_index()).rename(columns={'SvPowerFlow.Terminal': terminal_index})[[terminal_index,
                                                                                                     'SvPowerFlow.p',
                                                                                                     'SvPowerFlow.q']]
        power_flows = power_flows.merge(cgm_power_flows, on=terminal_index, how='left', suffixes=cgm_suffixes)
        flow_columns = [flow_column + suffix for flow_column in flow_columns for suffix in cgm_suffixes]

    merge_side = 'left'

    area_tie_flows = control_areas.merge(tie_flows, on=control_area_index)
    tie_flow_terminals = area_tie_flows.merge(terminals, on=terminal_index, how=merge_side)
    tn_tie_flow_terminal_nodes = tie_flow_terminals.merge(boundary_tn_nodes, on=topology_node_index, how=merge_side)
    tn_tie_flow_lines = tn_tie_flow_terminal_nodes.merge(tn_lines, on=tn_line_index, how=merge_side)
    tn_tie_flow_lines["BoundaryPoint.isDirectCurrent"] = tn_tie_flow_lines["Line.description"].str.startswith("HVDC")
    tn_tie_flow_lines['cross_border'] = (tn_tie_flow_lines
                                         .apply(lambda row: merge_sort_strings(row,
                                                                               col1='TopologicalNode.fromEndIsoCode',
                                                                               col2='TopologicalNode.toEndIsoCode'),
                                                axis=1))
    tn_tie_flow_flows = tn_tie_flow_lines.merge(power_flows, on=terminal_index, how='left')
    full_flow_columns = ['region'] + flow_columns
    ac_positions =  (tn_tie_flow_flows.query("`BoundaryPoint.isDirectCurrent` == False")[full_flow_columns]
                     .groupby('region').sum().reset_index())
    dc_positions = (tn_tie_flow_flows.query("`BoundaryPoint.isDirectCurrent` == True")[full_flow_columns]
                    .groupby('region').sum().reset_index())
    ac_dc_positions = (tn_tie_flow_flows[full_flow_columns]
                       .groupby('region').sum().reset_index().rename(columns={'region': 'Thunderstruck'}))
    return ac_positions, dc_positions, ac_dc_positions


def get_statistics(initial_model_data, cgm_sv_data: pandas.DataFrame = None, cgm_ssh_data: pandas.DataFrame = None):
    """
    Gets something from inputs
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param initial_model_data: original profiles
    :return some dict with values
    """

    labels = get_identifiers_for_file_names(original_data=initial_model_data).drop_duplicates(keep='last')

    "------------------------------------------------------------------------------------------------------------------"
    loads = get_load_reduced(original_data=initial_model_data, cgm_ssh_data=cgm_ssh_data, regions=labels)
    loads = pandas.DataFrame(data=[*loads.values, ['Total', *loads.sum(numeric_only=True).values]],
                             columns=loads.columns)

    generations = get_generation_reduced(original_data=initial_model_data,  cgm_ssh_data=cgm_ssh_data, regions=labels)

    generations = pandas.DataFrame(data=[*generations.values, ['Total', *generations.sum(numeric_only=True).values]],
                                   columns=generations.columns)
    "-----------------------------------------------------------------------------------------------------------------"

    ac_results, dc_results, ac_dc_results = get_tieflow_data_reduced(original_data=initial_model_data,
                                                                     cgm_ssh_data=cgm_ssh_data,
                                                                     cgm_sv_data=cgm_sv_data,
                                                                     regions=labels)

    net_results = get_net_position_values(original_data=initial_model_data,
                                          cgm_ssh_data=cgm_ssh_data,
                                          cgm_sv_data=cgm_sv_data,
                                          regions=labels)
    all_results = {"Generation": generations,
                   "Loads": loads,
                   "AC for the regions": ac_results,
                   "DC for the regions": dc_results,
                   "AC/DC for the regions": ac_dc_results,
                   "Net positions": net_results}
    return all_results


def save_and_get_statistics(initial_model_data,
                            sheet_name: str,
                            output_excel_writer: pandas.ExcelWriter,
                            cgm_sv_data: pandas.DataFrame = None,
                            cgm_ssh_data: pandas.DataFrame = None,
                            ):
    """
    Gets something from inputs and saves to excel file indicated
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param initial_model_data: original profiles
    :param sheet_name: sheet name where to store the data
    :param output_excel_writer: excel writer instance
    """
    all_results = get_statistics(initial_model_data=initial_model_data,
                                 cgm_sv_data=cgm_sv_data,
                                 cgm_ssh_data=cgm_ssh_data)

    sheet_name = os.path.basename(sheet_name)
    import re
    rx = re.compile(r'^[^-]+')
    components = sheet_name.split('_')
    filtered_components = [match.group(0) for string in components for match in [rx.search(string)] if match]
    if len(filtered_components) > 10:
        sheet_name = '_'.join(filtered_components[0:5])
    else:
        sheet_name = '_'.join(filtered_components)
    sheet_name = sheet_name[:32]
    workbook = output_excel_writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    # worksheet = writer.sheets[sheet_name]
    row_counter = 0
    col_counter = 0

    for result_name, results in all_results.items():
        worksheet.write_string(row_counter, col_counter, result_name)
        row_counter = row_counter + 1
        results.to_excel(output_excel_writer, sheet_name=sheet_name, startrow=row_counter, startcol=col_counter)
        row_counter = row_counter + 3 + len(results)


def load_files_from_local_storage(input_file_set):
    """
    Reads folder into triplets, separates cgm part of igm if applicable
    :param input_file_set: input folder
    :return model data, cgm ssh and sv profiles (if exists)
    """
    pypowsybl_file_list = get_list_of_xml_zip_files_from_dir(input_file_set)
    ssh_files = None
    sv_files = None
    cgm_ssh_data = None
    cgm_sv_data = None

    all_contents = []
    for file_name in pypowsybl_file_list:
        base_path = os.path.basename(file_name)
        contents = get_metadata_from_filename(base_path)
        contents['full_filename'] = file_name
        all_contents.append(contents)

    file_dataframe = pandas.DataFrame(all_contents)
    rcc_instances = [name_field for name_field in file_dataframe['Model.mergingEntity'].unique()
                     if name_field and isinstance(name_field, str)]
    rcc_instance = None
    if len(rcc_instances) == 1:
        rcc_instance = rcc_instances[0]
    separate_merged_part = True
    if rcc_instance:
        ssh_files = [file_name for file_name in pypowsybl_file_list
                     if (rcc_instance not in file_name) and ('_SSH_' in file_name)]
        sv_files = [file_name for file_name in pypowsybl_file_list
                    if (rcc_instance not in file_name) and ('_SV_' in file_name)]
    if not ssh_files and not sv_files:
        separate_merged_part = False
    model_files = pypowsybl_file_list

    if separate_merged_part:
        ssh_files = [file_name for file_name in pypowsybl_file_list
                     if (rcc_instance in file_name) and ('_SSH_' in file_name)]
        sv_files = [file_name for file_name in pypowsybl_file_list
                    if (rcc_instance in file_name) and ('_SV_' in file_name)]
        model_files = [file_name for file_name in pypowsybl_file_list
                       if (file_name not in ssh_files) and (file_name not in sv_files)]
        cgm_sv_data = pandas.read_RDF(sv_files)
        cgm_ssh_data = pandas.read_RDF(ssh_files)
    rest_model_data = pandas.read_RDF(model_files)
    return rest_model_data, cgm_ssh_data, cgm_sv_data


if __name__ == '__main__':

    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Specify path to RMM
    all_sets = r"./model_merger/schedules_comp"
    sub_instances = []
    first_level_folders = [f.path for f in os.scandir(all_sets) if f.is_dir()]
    report_name = r"./workgroup_merge/models"

    time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    report_name = report_name.removesuffix('/') + '/' + f"model_statistics_{time_moment_now}.xlsx"
    with pandas.ExcelWriter(report_name) as excel_writer:

        for file_set in first_level_folders:
            model_data, ssh_data, sv_data = load_files_from_local_storage(file_set)
            save_and_get_statistics(initial_model_data=model_data,cgm_sv_data=sv_data,
                                    sheet_name=file_set,
                                    cgm_ssh_data=ssh_data,
                                    output_excel_writer=excel_writer)
    print("Done")

