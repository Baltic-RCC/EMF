import pypowsybl
import triplets
import pandas as pd
import logging

from emf.common.integrations import elastic
from emf.loadflow_tool.helper import create_opdm_objects
from emf.loadflow_tool.model_validator.model_statistics import get_model_outages
from emf.loadflow_tool.model_merger.merge_functions import (load_opdm_data, create_sv_and_updated_ssh, fix_sv_shunts,
                                                            fix_sv_tapsteps, remove_duplicate_sv_voltages,
                                                            remove_small_islands, check_and_fix_dependencies,
                                                            disconnect_equipment_if_flow_sum_not_zero,
                                                            export_to_cgmes_zip,
                                                            configure_paired_boundarypoint_injections_by_nodes,
                                                            get_opdm_data_from_models)


logger = logging.getLogger(__name__)


def check_switch_terminals(input_data: pd.DataFrame, column_name: str):
    """
    Checks if column of a dataframe contains only one value
    :param input_data: input data frame
    :param column_name: name of the column to check
    return True if different values are in column, false otherwise
    """
    data_slice = (input_data.reset_index())[column_name]
    return not pd.Series(data_slice[0] == data_slice).all()


def get_not_retained_switches_between_nodes(original_data):
    """
    For the loadflow open all the non-retained switches that connect different topological nodes
    Currently it is seen to help around 9 to 10 Kirchhoff 1st law errors from 2 TSOs
    :param original_data: original models in triplets format
    :return: updated original data
    """
    updated_switches = False
    original_models = get_opdm_data_from_models(original_data)
    not_retained_switches = original_models[(original_models['KEY'] == 'Switch.retained')
                                            & (original_models['VALUE'] == "false")][['ID']]
    closed_switches = original_models[(original_models['KEY'] == 'Switch.open')
                                      & (original_models['VALUE'] == 'false')]
    not_retained_closed = not_retained_switches.merge(closed_switches[['ID']], on='ID')
    terminals = original_models.type_tableview('Terminal').rename_axis('Terminal').reset_index()
    terminals = terminals[['Terminal',
                           # 'ACDCTerminal.connected',
                           'Terminal.ConductingEquipment',
                           'Terminal.TopologicalNode']]
    not_retained_terminals = (terminals.rename(columns={'Terminal.ConductingEquipment': 'ID'})
                              .merge(not_retained_closed, on='ID'))
    if not_retained_terminals.empty:
        return original_data, updated_switches
    between_tn = ((not_retained_terminals.groupby('ID')[['Terminal.TopologicalNode']]
                  .apply(lambda x: check_switch_terminals(x, 'Terminal.TopologicalNode')))
                  .reset_index(name='same_TN'))
    between_tn = between_tn[between_tn['same_TN']]
    return between_tn


def open_switches_in_network(network_pre_instance: pypowsybl.network.Network, switches_dataframe: pd.DataFrame):
    """
    Opens switches in loaded network given bu dataframe (uses ID for merging)
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :param switches_dataframe: dataframe
    """
    logger.info(f"Opening {len(switches_dataframe.index)} switches")
    switches = network_pre_instance.get_switches(all_attributes=True).reset_index()
    switches = switches.merge(switches_dataframe[['ID']].rename(columns={'ID': 'id'}), on='id')
    non_retained_closed = switches.merge(switches_dataframe.rename(columns={'ID': 'id'}),
                                         on='id')[['id', 'open']]
    non_retained_closed['open'] = True
    network_pre_instance.update_switches(non_retained_closed.set_index('id'))
    return network_pre_instance


def run_pre_merge_processing(input_models, merging_area):

    # TODO warning logs for temp fix functions
    assembled_data = load_opdm_data(input_models)
    assembled_data = triplets.cgmes_tools.update_FullModel_from_filename(assembled_data)
    assembled_data = configure_paired_boundarypoint_injections_by_nodes(assembled_data)
    escape_upper_xml = assembled_data[assembled_data['VALUE'].astype(str).str.contains('.XML')]
    between_tn = get_not_retained_switches_between_nodes(assembled_data)
    if not escape_upper_xml.empty:
        escape_upper_xml['VALUE'] = escape_upper_xml['VALUE'].str.replace('.XML', '.xml')
        assembled_data = triplets.rdf_parser.update_triplet_from_triplet(assembled_data, escape_upper_xml, update=True, add=False)

    input_models = create_opdm_objects([export_to_cgmes_zip([assembled_data])])

    return input_models, between_tn


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


def run_post_merge_processing(input_models: list,
                              merged_model: object,
                              task_properties: dict,
                              small_island_size: str,
                              enable_temp_fixes: bool,
                              time_horizon: str | None = None):

    time_horizon = time_horizon or task_properties["time_horizon"]
    scenario_datetime = task_properties["timestamp_utc"]
    merging_area = task_properties["merge_type"]
    merging_entity = task_properties["merging_entity"]
    mas = task_properties["mas"]
    version = task_properties["version"]

    models_as_triplets = load_opdm_data(input_models)
    sv_data, ssh_data = create_sv_and_updated_ssh(merged_model, input_models, models_as_triplets,
                                                  scenario_datetime, time_horizon,
                                                  version, merging_area,
                                                  merging_entity, mas)
    fix_net_interchange_errors = task_properties.get('fix_net_interchange2', False)
    net_interchange_threshold = int(task_properties.get('net_interchange2_threshold', 200))
    if enable_temp_fixes:
        sv_data = fix_sv_shunts(sv_data, models_as_triplets)
        sv_data = fix_sv_tapsteps(sv_data, ssh_data)
        sv_data = remove_small_islands(sv_data, int(small_island_size))
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


def fix_model_outages(merged_model: object, tso_list: list, scenario_datetime: str, time_horizon: str, debug: bool = False):

    area_map = {"LITGRID": "Lithuania", "AST": "Latvia", "ELERING": "Estonia"}
    outage_areas = [area_map.get(item, item) for item in tso_list]

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
    model_outages = pd.DataFrame(get_model_outages(merged_model.network))
    mapped_model_outages = pd.merge(model_outages, mrid_map, left_on='grid_id', right_on='mrid', how='inner')
    model_area_map = {"LITGRID": "LT", "AST": "LV", "ELERING": "EE"}
    model_outage_areas = [model_area_map.get(item, item) for item in tso_list]
    filtered_model_outages = mapped_model_outages[mapped_model_outages['country'].isin(model_outage_areas)]

    logger.info("Fixing outages inside merged model")

    # Reconnecting outages from network-config list
    for index, outage in filtered_model_outages.iterrows():
        try:
            if merged_model.network.connect(outage['grid_id']):
                logger.info(f" {outage['name']} {outage['grid_id']} successfully reconnected")
                merged_model.outages = True
                merged_model.outages_updated.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic'], "status": "connected"}])
            else:
                if uap_outages['grid_id'].str.contains(outage['grid_id']).any():
                    logger.info(f"{outage['name']} {outage['grid_id']} is already connected")
                else:
                    logger.error(f"Failed to connect outage: {outage['name']} {outage['grid_id']}")
                    merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic']}])

        except Exception as e:
            logger.error((e, outage['name']))
            merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic']}])
            continue

    # Applying outages from UAP
    for index, outage in mapped_outages.iterrows():
        try:
            if merged_model.network.disconnect(outage['grid_id']):
                logger.info(f"{outage['name']} {outage['grid_id']} successfully disconnected")
                merged_model.outages = True
                merged_model.outages_updated.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic'], "status": "disconnected"}])
            else:
                if uap_outages['grid_id'].str.contains(outage['grid_id']).any():
                    logger.info(f"{outage['name']} {outage['grid_id']} is already in outage")
                else:
                    logger.error(f"Failed to disconnect outage: {outage['name']} {outage['grid_id']}")
                    merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic']}])

        except Exception as e:
            logger.error((e, outage['name']))
            merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['grid_id'], "eic": outage['eic']}])
            merged_model.outages = False
            continue

    if merged_model.outages_unmapped:
        merged_model.outages = False

    return merged_model


def fix_igm_ssh_vs_cgm_ssh_error(network_pre_instance: pypowsybl.network.Network):
    """
    Implements various fixes to suppress igm ssh vs cgm ssh error
    1) Get all generators and remove them from slack distribution
    2) If generators have target_p outside the endpoints ('limits') of a curve then set it to be within
    3) Condensers p should not be modified so if it is not 0 then it sets the target_p to equal the existing p
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :return updated network_pre_instance
    """
    try:
        all_generators = network_pre_instance.get_elements(element_type=pypowsybl.network.ElementType.GENERATOR,
                                                           all_attributes=True).reset_index()
        generators_mask = (all_generators['CGMES.synchronousMachineOperatingMode'].str.contains('generator'))
        not_generators = all_generators[~generators_mask]
        generators = all_generators[generators_mask]
        curve_points = (network_pre_instance
                        .get_elements(element_type=pypowsybl.network.ElementType.REACTIVE_CAPABILITY_CURVE_POINT,
                                      all_attributes=True).reset_index())
        curve_limits = (curve_points.merge(generators[['id']], on='id')
                        .groupby('id').agg(curve_p_min=('p', 'min'), curve_p_max=('p', 'max'))).reset_index()
        curve_generators = generators.merge(curve_limits, on='id')
        # low end can be zero
        curve_generators = curve_generators[(curve_generators['target_p'] > curve_generators['curve_p_max']) |
                                            ((curve_generators['target_p'] > 0) &
                                             (curve_generators['target_p'] < curve_generators['curve_p_min']))]
        if not curve_generators.empty:
            logger.warning(f"Found {len(curve_generators.index)} generators for "
                           f"which p > max(reactive capacity curve(p)) or p < min(reactive capacity curve(p))")

            # Solution 1: set max_p from curve max, it should contain p on target-p
            upper_limit_violated = curve_generators[(curve_generators['max_p'] > curve_generators['curve_p_max'])]
            if not upper_limit_violated.empty:
                logger.warning(f"Updating max p from curve for {len(upper_limit_violated.index)} generators")
                upper_limit_violated['max_p'] = upper_limit_violated['curve_p_max']
                network_pre_instance.update_generators(upper_limit_violated[['id', 'max_p']].set_index('id'))

            lower_limit_violated = curve_generators[(curve_generators['min_p'] < curve_generators['curve_p_min'])]
            if not lower_limit_violated.empty:
                logger.warning(f"Updating min p from curve for {len(lower_limit_violated.index)} generators")
                lower_limit_violated['min_p'] = lower_limit_violated['curve_p_min']
                network_pre_instance.update_generators(lower_limit_violated[['id', 'min_p']].set_index('id'))

            # Solution 2: discard generator from participating
            extensions = network_pre_instance.get_extensions('activePowerControl')
            remove_curve_generators = extensions.merge(curve_generators[['id']],
                                                       left_index=True, right_on='id')
            if not remove_curve_generators.empty:
                remove_curve_generators['participate'] = False
                network_pre_instance.update_extensions('activePowerControl',
                                                       remove_curve_generators.set_index('id'))
        condensers = all_generators[(all_generators['CGMES.synchronousMachineType'].str.contains('condenser'))
                                    & (abs(all_generators['p']) > 0)
                                    & (abs(all_generators['target_p']) == 0)]
        # Fix condensers that have p not zero by setting their target_p to equal to p
        if not condensers.empty:
            logger.warning(f"Found {len(condensers.index)} condensers for which p ~= 0 & target_p = 0")
            condensers.loc[:, 'target_p'] = condensers['p'] * (-1)
            network_pre_instance.update_generators(condensers[['id', 'target_p']].set_index('id'))
        # Remove all not generators from active power distribution
        if not not_generators.empty:
            logger.warning(f"Removing {len(not_generators.index)} machines from power distribution")
            extensions = network_pre_instance.get_extensions('activePowerControl')
            remove_not_generators = extensions.merge(not_generators[['id']], left_index=True, right_on='id')
            remove_not_generators['participate'] = False
            remove_not_generators = remove_not_generators.set_index('id')
            network_pre_instance.update_extensions('activePowerControl', remove_not_generators)
    except Exception as ex:
        logger.error(f"Unable to pre-process for igm-cgm-ssh error: {ex}")
    return network_pre_instance
