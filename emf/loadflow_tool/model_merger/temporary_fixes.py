import triplets
import pandas as pd
import logging

from emf.common.integrations import elastic
from emf.loadflow_tool.helper import create_opdm_objects, get_model_outages
from emf.loadflow_tool.model_merger.merge_functions import (load_opdm_data, create_sv_and_updated_ssh, fix_sv_shunts,
                                                            fix_sv_tapsteps, remove_duplicate_sv_voltages,
                                                            remove_small_islands,check_and_fix_dependencies,
                                                            disconnect_equipment_if_flow_sum_not_zero,
                                                            export_to_cgmes_zip,
                                                            configure_paired_boundarypoint_injections_by_nodes,
                                                            get_opdm_data_from_models)


logger = logging.getLogger(__name__)


def run_pre_merge_processing(input_models, merging_area):

    # TODO warning logs for temp fix functions
    assembled_data = load_opdm_data(input_models)
    assembled_data = triplets.cgmes_tools.update_FullModel_from_filename(assembled_data)
    assembled_data = configure_paired_boundarypoint_injections_by_nodes(assembled_data)
    escape_upper_xml = assembled_data[assembled_data['VALUE'].astype(str).str.contains('.XML')]
    if not escape_upper_xml.empty:
        escape_upper_xml['VALUE'] = escape_upper_xml['VALUE'].str.replace('.XML', '.xml')
        assembled_data = triplets.rdf_parser.update_triplet_from_triplet(assembled_data, escape_upper_xml, update=True, add=False)

    input_models = create_opdm_objects([export_to_cgmes_zip([assembled_data])])

    return input_models


def check_net_interchanges(cgm_sv_data, cgm_ssh_data, original_models, fix_errors: bool = False,
                           threshold: float = None):
    """
    An attempt to calculate the net interchange 2 values and check them against those provided in ssh profiles
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param original_models: original profiles
    :param fix_errors: injects new calculated flows into merged ssh profiles
    :param threshold: specify threshold if needed
    :return (updated) ssh profiles
    """
    original_models = get_opdm_data_from_models(model_data=original_models)
    try:
        control_areas = (original_models.type_tableview('ControlArea')
                         .rename_axis('ControlArea')
                         .reset_index())[['ControlArea', 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                          'IdentifiedObject.energyIdentCodeEic', 'IdentifiedObject.name']]
    except KeyError:
        control_areas = original_models.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        ssh_areas = cgm_ssh_data.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        control_areas = control_areas.merge(ssh_areas, on='ControlArea')[['ControlArea', 'ControlArea.netInterchange',
                                                                          'ControlArea.pTolerance',
                                                                          'IdentifiedObject.energyIdentCodeEic',
                                                                          'IdentifiedObject.name']]
    tie_flows = (original_models.type_tableview('TieFlow')
                 .rename_axis('TieFlow').rename(columns={'TieFlow.ControlArea': 'ControlArea',
                                                         'TieFlow.Terminal': 'Terminal'})
                 .reset_index())[['ControlArea', 'Terminal', 'TieFlow.positiveFlowIn']]
    tie_flows = tie_flows.merge(control_areas[['ControlArea']], on='ControlArea')
    try:
        terminals = (original_models.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal', 'ACDCTerminal.connected']]
    except KeyError:
        terminals = (original_models.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal']]
    tie_flows = tie_flows.merge(terminals, on='Terminal')
    try:
        power_flows_pre = (original_models.type_tableview('SvPowerFlow')
                           .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                           .reset_index())[['Terminal', 'SvPowerFlow.p']]
        tie_flows = tie_flows.merge(power_flows_pre, on='Terminal', how='left')
    except Exception:
        logger.error(f"Was not able to get tie flows from original models")
    power_flows_post = (cgm_sv_data.type_tableview('SvPowerFlow')
                        .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                        .reset_index())[['Terminal', 'SvPowerFlow.p']]

    tie_flows = tie_flows.merge(power_flows_post, on='Terminal', how='left',
                                suffixes=('_pre', '_post'))
    try:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p_pre', 'SvPowerFlow.p_post']]
                              .agg(lambda x: pd.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
    except KeyError:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p']]
                              .agg(lambda x: pd.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
        tie_flows_grouped = tie_flows_grouped.rename(columns={'SvPowerFlow.p': 'SvPowerFlow.p_post'})
    tie_flows_grouped = control_areas.merge(tie_flows_grouped, on='ControlArea')
    if threshold and threshold > 0:
        tie_flows_grouped['Exceeded'] = (abs(tie_flows_grouped['ControlArea.netInterchange']
                                             - tie_flows_grouped['SvPowerFlow.p_post']) > threshold)
    else:
        tie_flows_grouped['Exceeded'] = (abs(tie_flows_grouped['ControlArea.netInterchange']
                                             - tie_flows_grouped['SvPowerFlow.p_post']) >
                                         tie_flows_grouped['ControlArea.pTolerance'])
    net_interchange_errors = tie_flows_grouped[tie_flows_grouped.eval('Exceeded')]
    if not net_interchange_errors.empty:
        if threshold > 0:
            logger.error(f"Found {len(net_interchange_errors.index)} possible net interchange_2 problems "
                         f"over {threshold}:")
        else:
            logger.error(f"Found {len(net_interchange_errors.index)} possible net interchange_2 problems:")
        print(net_interchange_errors.to_string())
        if fix_errors:
            logger.warning(f"Updating {len(net_interchange_errors.index)} interchanges to new values")
            new_areas = cgm_ssh_data.type_tableview('ControlArea').reset_index()[['ID',
                                                                                  'ControlArea.pTolerance', 'Type']]
            new_areas = new_areas.merge(net_interchange_errors[['ControlArea', 'SvPowerFlow.p_post']]
                                        .rename(columns={'ControlArea': 'ID',
                                                         'SvPowerFlow.p_post': 'ControlArea.netInterchange'}), on='ID')
            cgm_ssh_data = triplets.rdf_parser.update_triplet_from_tableview(cgm_ssh_data, new_areas)
    return cgm_ssh_data



def run_post_merge_processing(input_models, solved_model, task_properties, SMALL_ISLAND_SIZE, enable_temp_fixes,
                              time_horizon: str=None):

    time_horizon = time_horizon or task_properties["time_horizon"]
    scenario_datetime = task_properties["timestamp_utc"]
    merging_area = task_properties["merge_type"]
    merging_entity = task_properties["merging_entity"]
    mas = task_properties["mas"]
    version = task_properties["version"]

    models_as_triplets = load_opdm_data(input_models)
    sv_data, ssh_data = create_sv_and_updated_ssh(solved_model, input_models, models_as_triplets,
                                                  scenario_datetime, time_horizon,
                                                  version, merging_area,
                                                  merging_entity, mas)
    fix_net_interchange_errors = task_properties.get('fix_net_interchange2', False)
    net_interchange_threshold = int(task_properties.get('net_interchange2_threshold', 200))
    if enable_temp_fixes:
        sv_data = fix_sv_shunts(sv_data, models_as_triplets)
        sv_data = fix_sv_tapsteps(sv_data, ssh_data)
        sv_data = remove_small_islands(sv_data, int(SMALL_ISLAND_SIZE))
        sv_data = remove_duplicate_sv_voltages(cgm_sv_data=sv_data, original_data=models_as_triplets)
        sv_data = check_and_fix_dependencies(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data, original_data=models_as_triplets)
        #sv_data, ssh_data = disconnect_equipment_if_flow_sum_not_zero(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data, original_data=models_as_triplets) fix implemented in pypowsybl 1.8.1 
        try:
            ssh_data = check_net_interchanges(cgm_sv_data=sv_data,
                                              cgm_ssh_data=ssh_data,
                                              original_models=models_as_triplets,
                                              fix_errors=fix_net_interchange_errors,
                                              threshold=net_interchange_threshold)
        except KeyError:
            logger.error(f"No fields for netInterchange")
    return sv_data, ssh_data


def fix_model_outages(merged_model, model_list: list, merge_log, scenario_datetime, time_horizon, debug=False):

    area_map = {"LITGRID": "Lithuania", "AST": "Latvia", "ELERING": "Estonia"}
    outage_areas = [area_map.get(item, item) for item in model_list]

    elk_service = elastic.Elastic()

    # Get outage eic-mrid mapping
    mrid_map = elk_service.get_docs_by_query(index='config-network*', query={"match_all": {}}, size=10000)
    mrid_map['mrid'] = mrid_map['mrid'].str.lstrip('_')

    # Get latest UAP parse date
    if time_horizon == 'MO':
        merge_type = "Month"
    elif time_horizon == 'YR':
        merge_type = "Year"
    else:
        merge_type = "Week"

    body = {"size": 1, "query": {"bool": {"must": [{"match": {"Merge": merge_type}}]}},
            "sort": [{"reportParsedDate": {"order": "desc"}}], "fields": ["reportParsedDate"]}
    last_uap_version = elk_service.client.search(index='opc-outages-baltics*', body=body)['hits']['hits'][0]['fields']['reportParsedDate'][0]

    # Query for latest outage UAP
    uap_query = {"bool": {"must": [{"match": {"reportParsedDate": f"{last_uap_version}"}},
                                   {"match": {"Merge": merge_type}}]}}
    uap_outages = elk_service.get_docs_by_query(index='opc-outages-baltics*', query=uap_query, size=10000)
    uap_outages = uap_outages.merge(mrid_map[['eic', 'mrid']], how='left', on='eic').rename(columns={"mrid": 'grid_id'})

    # Filter outages according to model scenario date and replaced area
    filtered_outages = uap_outages[(uap_outages['start_date'] <= scenario_datetime) & (uap_outages['end_date'] >= scenario_datetime)]
    filtered_outages = filtered_outages[filtered_outages['Area'].isin(outage_areas)]

    mapped_outages = filtered_outages[~filtered_outages['grid_id'].isna()]
    missing_outages = filtered_outages[filtered_outages['grid_id'].isna()]

    if not missing_outages.empty:
        logger.warning(f"Missing outage mRID(s): {missing_outages['name'].values}")

    # Get outages already applied to the model
    model_outages = pd.DataFrame(get_model_outages(merged_model['network']))
    mapped_model_outages = pd.merge(model_outages, mrid_map, left_on='grid_id', right_on='mrid', how='inner')
    model_area_map = {"LITGRID": "LT", "AST": "LV", "ELERING": "EE"}
    model_outage_areas = [model_area_map.get(item, item) for item in model_list]
    filtered_model_outages = mapped_model_outages[mapped_model_outages['country'].isin(model_outage_areas)]

    logger.info("Fixing outages inside merged model")

    # Reconnecting outages from network-config list
    logger.info("Reconnecting outages from network-config list")
    for index, outage in filtered_model_outages.iterrows():
        try:
            if merged_model['network'].connect(outage['grid_id']):
                if debug:
                    logger.info(f" {outage['name']} {outage['grid_id']} successfully reconnected")
                merge_log.update({'outages_corrected': True})
                merge_log.get('outage_fixes').extend([{'name': outage['name'], 'grid_id': outage['grid_id'], "eic": outage['eic'], "outage_status": "connected"}])
            else:
                if uap_outages['grid_id'].str.contains(outage['grid_id']).any():
                    logger.info(f"{outage['name']} {outage['grid_id']} is already connected")
                else:
                    logger.error(f"Failed to connect outage: {outage['name']} {outage['grid_id']}")
        except Exception as e:
            logger.error((e, outage['name']))
            merge_log.get('outages_unmapped').extend([{'name': outage['name'], 'grid_id': outage['grid_id'], "eic": outage['eic']}])
            continue

    # Applying outages from UAP
    logger.info("Applying outages from UAP")
    for index, outage in mapped_outages.iterrows():
        try:
            if merged_model['network'].disconnect(outage['grid_id']):
                if debug:
                    logger.info(f"{outage['name']} {outage['grid_id']} successfully disconnected")
                merge_log.update({'outages_corrected': True})
                merge_log.get('outage_fixes').extend([{'name': outage['name'], 'grid_id': outage['grid_id'], "eic": outage['eic'], "outage_status": "disconnected"}])
            else:
                if uap_outages['grid_id'].str.contains(outage['grid_id']).any():
                    logger.info(f"{outage['name']} {outage['grid_id']} is already in outage")
                else:
                    logger.error(f"Failed to disconnect outage: {outage['name']} {outage['grid_id']}")
        except Exception as e:
            logger.error((e, outage['name']))
            merge_log.get('outages_unmapped').extend([{'name': outage['name'], 'grid_id': outage['grid_id'], "eic": outage['eic']}])
            continue

    return merged_model, merge_log
