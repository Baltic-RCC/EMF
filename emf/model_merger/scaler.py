"""
EMF REQUIREMENTS:
1. Compare the target values for AC net positions and DC links with the values recorded
after calculating the power flow on the pan-European model.
2. The recorded flow on DC links shall be equal to the target value of the scenario.
3. The recorded AC net position shall be equal to the reference value of the scenario.
4. If discrepancy exists for one or more scheduling areas, between the two values, then a
balance adjustment by adjusting the loads has to be done.
5. The discrepancy thresholds are defined as follows:
6. Sum of AC tieline flows - AC NET Position target < 2MW
7. If the discrepancy occurs as defined in the previous step, the conforming loads of each
scheduling area are modified proportionally in order to match the netted Area AC
position, while maintaining the power factor of the loads.
8. The Jacobian is built for the new power flow iteration and new values for the AC tie line
flows are calculated, in order to check if the conforming loads in the scheduling area have
to be adjusted again.
9. If the power injection in the global slack bus exceeds a configurable threshold, this power
injection shall be redistributed on all generation units in the synchronous area
proportional to the reserve margin.
10. This loop ends:
" When all the differences between the recorded and target values of net positions of
scheduling areas are below the discrepancy thresholds, as defined previously;
" In any case after the 15th iteration16 (adjustments take place within the iterations).

NOTES:
    - power factor sign defines whether P and Q values has opposite sign. This needs to be ensured because new Q values
are calculated from P values, then the power factors sign defines what sign should be for new Q value.
    - current algorithm is using subnetworks, therefore at network import parameters it should be set to True.
It is possible to use older solution but that causes problems with TTN IGM where some dangling lines does not have
substation assigned. Current algorithm defines element area from subnetworks identifiables instead of substations
dataframe.
"""

import pypowsybl as pp
import logging
import pandas as pd
import numpy as np
import json
import copy
from typing import Dict, List, Union
from collections import defaultdict
import config
from emf.common.config_parser import parse_app_properties
from emf.common.decorators import performance_counter
from emf.common.integrations.object_storage.schedules import query_acnp_schedules, query_hvdc_schedules
from emf.common.helpers.utils import attr_to_dict
from emf.common.helpers.loadflow import get_network_elements, get_slack_generators, get_connected_components_data
from emf.common.loadflow_tool.loadflow_settings import EU_RELAXED

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.scaler)

# Global variables
_country_col: str = 'CGMES.regionName'


def validate_loadflow_status(results: List, components: Dict):
    # Validate all network components convergence status in order to exclude diverged non main islands
    for result in [x for x in results if x.connected_component_num in components.keys()]:
        k = result.connected_component_num
        if result.status.name.lower() != 'converged':
            logger.warning(f"Network component {k} diverged during scaling, excluding: {components.pop(k)}")

    # Validate main island convergence
    if results[0].status.value == 0:
        return True
    else:
        return False


def get_areas_losses(network: pp.network.Network, buses: pd.DataFrame, components: Dict):
    # Calculate ACNP with losses (from cross-border lines)
    dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    dangling_lines = dangling_lines.merge(buses.connected_component, how='left', left_on='bus_id', right_index=True)
    dangling_lines = dangling_lines[dangling_lines.connected_component.isin(components.keys())]
    dangling_lines.connected_component = dangling_lines.connected_component.astype(int)
    dangling_lines['boundary_p'] = dangling_lines['boundary_p'] * -1  # invert boundary_p sign to match flow direction
    ac_dangling_lines = dangling_lines[dangling_lines.isHvdc == '']
    dc_dangling_lines = dangling_lines[dangling_lines.isHvdc == 'true']
    acnp_with_losses = _get_series_from_df(df=ac_dangling_lines, value_col='boundary_p').groupby(level=0).sum()

    # Calculate ACNP without losses (from generation and consumption)
    generation = get_areas_metrics(network=network, buses=buses, components=components, metric='GENERATOR')
    consumption = get_areas_metrics(network=network, buses=buses, components=components, metric='LOAD')
    ## Need to ensure that all series in substraction has same index values. For example when area does not have HVDC connections
    ## Otherwise we will get NaN values for areas without HVDC after regular substraction
    present_areas = generation.index.union(consumption.index)
    dcnp = _get_series_from_df(df=dc_dangling_lines, value_col='boundary_p').groupby(level=0).sum().reindex(present_areas, fill_value=0)
    acnp_without_losses = generation - consumption - dcnp

    # Calculate losses by regions
    losses = acnp_without_losses - acnp_with_losses

    return losses.round(1)


def get_areas_metrics(network: pp.network.Network, buses: pd.DataFrame, components: Dict, metric: str):
    df = get_network_elements(network, getattr(pp.network.ElementType, metric), all_attributes=True)
    df = df.merge(buses.connected_component, how='left', left_on='bus_id', right_index=True)
    df = df[df.connected_component.isin(components.keys())]
    df.connected_component = df.connected_component.astype(int)
    sign = -1 if metric == 'GENERATOR' else 1
    series = _get_series_from_df(df=df, value_col='p')
    return series.groupby(series.index).sum() * sign


def validate_converged_components(dangling_lines: pd.DataFrame, converged_components: Dict):
    logger.info(f"Validating converged islands")
    for k, v in list(converged_components.items()):
        v['state'] = 'valid'
        # In case of internal island it should contain only one area
        if len(v['countries']) == 1:
            component_dangling_lines = dangling_lines[dangling_lines['connected_component'] == k]
            # Check if there are any boundary lines which belongs to component
            if component_dangling_lines.empty:
                v['state'] = 'internal'
                logger.warning(f"Network component {k} considered as internal area island, excluding from scaling: {v}")

    return converged_components


def get_network_elements_map_to_areas(network: pp.network):
    _temp = []
    # Network import parameters have to be set to use subnetworks in order to use this function!
    sub_network_ids = network.get_sub_networks(all_attributes=True).index
    if sub_network_ids.empty:
        logger.error(f"Subnetworks does not exists in network model or disabled by import parameters")
        raise Exception("Scaling terminated due to missing subnetworks")
    for id in sub_network_ids:
        subnetwork = network.get_sub_network(id)
        country_id = subnetwork.get_substations(all_attributes=True)[_country_col].unique().tolist()[0]
        identifiables = subnetwork.get_identifiables()
        identifiables[_country_col] = country_id
        _temp.append(identifiables)

    return pd.concat(_temp)


def get_countries_to_components(components: Dict):
    country_to_keys = defaultdict(set)
    for key, entry in components.items():
        for country in entry.get('countries', []):
            country_to_keys[country].add(key)

    return country_to_keys


def get_fragmented_areas_participation(unpaired_dangling_lines: pd.DataFrame, areas_to_components: Dict):
    fragmented_areas = []
    for area, comps in areas_to_components.items():
        if len(comps) > 1:
            logger.warning(f"Fragmented area identified: {area} in components {list(comps)}")
            area_dangling_lines = unpaired_dangling_lines[unpaired_dangling_lines[_country_col] == area]
            fragments_acnp = {comp: area_dangling_lines[area_dangling_lines.connected_component == comp].boundary_p.sum() for comp in comps}
            total_fragments_acnp = abs(sum(fragments_acnp.values())) or 1  # removing zero division warning
            participation = {k: abs(v) / total_fragments_acnp for k, v in fragments_acnp.items()}
            fragmented_areas.append(pd.DataFrame({'connected_component': list(participation.keys()),
                                                  'participation': list(participation.values()),
                                                  'registered_resource': area}))

    if fragmented_areas:
        fragmented_areas = pd.concat(fragmented_areas)
    else:
        fragmented_areas = pd.DataFrame(columns=['connected_component', 'participation', 'registered_resource'])

    return fragmented_areas


def _get_series_from_df(df: pd.DataFrame, value_col: str, area_col: str = _country_col):
    return pd.Series(df[value_col].values,
                     index=df[area_col].astype(str) + "-" + df.connected_component.astype(str)).sort_index().round(1)


def _set_power_ratio_to_dangling_lines(df: pd.DataFrame):
    df['power_factor'] = df['boundary_q'] / df['boundary_p']  # estimate the power factor
    df['power_factor'] = df['power_factor'].fillna(0)  # handle zero division
    df['power_factor'] = df['power_factor'].clip(-float(POWER_FACTOR_THRESHOLD), float(POWER_FACTOR_THRESHOLD))
    return df


@performance_counter(units='seconds')
def scale_balance(model: object,
                  ac_schedules: List[Dict[str, Union[str, float, None]]],
                  dc_schedules: List[Dict[str, Union[str, float, None]]],
                  lf_settings: pp.loadflow.Parameters = EU_RELAXED,
                  debug=json.loads(DEBUG.lower()),
                  ):
    """
    Main method to scale each CGM area to target balance
    :param network: pypowsybl network object
    :param ac_schedules: target AC net positions in list of dict format
    :param dc_schedules: target DC net positions in list of dict format
    :param lf_settings: loadflow settings
    :param debug: debug flag
    :return: scaled pypowsybl network object
    """
    logger.info(f"Network scaling initialized")

    # Get pypowsybl network
    network = model.network

    # Define general variables to be used in scaling algorithm
    _CONSTANT_POWER_FACTOR = json.loads(CONSTANT_POWER_FACTOR.lower())
    _components = get_connected_components_data(network=network, bus_count_threshold=5, country_col_name=_country_col)
    _scaling_results = []
    _hvdc_results = []
    _iteration = 0

    # Defining logging level
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Get entire network elements mapping to areas
    _elements_to_areas_map = get_network_elements_map_to_areas(network=network)

    # Get buses
    buses = network.get_buses()

    # Get all dangling lines and define power factor
    # dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    dangling_lines = network.get_dangling_lines(all_attributes=True)
    dangling_lines[_country_col] = dangling_lines.index.map(_elements_to_areas_map[_country_col])
    dangling_lines = _set_power_ratio_to_dangling_lines(dangling_lines)
    dangling_lines['boundary_p'] = dangling_lines['boundary_p'] * -1  # invert boundary_p sign to match flow direction
    dangling_lines['boundary_q'] = dangling_lines['boundary_q'] * -1  # invert boundary_q sign to match flow direction (just used for printing)

    # Target HVDC setpoints
    target_hvdc_sp_df = pd.DataFrame(dc_schedules)

    # Target AC net positions mapping
    target_acnp_df = pd.DataFrame(ac_schedules)
    target_acnp_df['registered_resource'] = target_acnp_df['in_domain'].where(target_acnp_df['in_domain'].notna(), target_acnp_df['out_domain'])
    target_acnp_df = target_acnp_df.dropna(subset='registered_resource')
    target_acnp_df = target_acnp_df.sort_values('value', key=abs, ascending=False).drop_duplicates(subset='registered_resource')
    mask = (target_acnp_df['in_domain'].notna()) & (target_acnp_df['value'] > 0.0)  # value is not zero
    target_acnp_df['value'] = np.where(mask, target_acnp_df['value'] * -1, target_acnp_df['value'])

    # Validate presence of target AC net position by areas in network model
    present_areas = dangling_lines[_country_col].drop_duplicates()
    missing_ac_schedule = present_areas[~present_areas.isin(target_acnp_df.registered_resource)].to_list()
    if missing_ac_schedule:
        # TODO consider exit scaling here if some schedules are missing
        logger.error(f"Missing target AC schedule for areas present in network model: {missing_ac_schedule}")

    # Get pre-scale HVDC setpoints
    logger.info(f"Scaling HVDC network part")
    prescale_hvdc_sp = dangling_lines[dangling_lines.isHvdc == 'true'][['lineEnergyIdentificationCodeEIC', 'boundary_p', 'boundary_q']]
    prescale_hvdc_sp = prescale_hvdc_sp.rename(columns={'boundary_p': 'value', 'boundary_q': 'value_q'})
    _hvdc_results.append(pd.concat([prescale_hvdc_sp.set_index('lineEnergyIdentificationCodeEIC').value,
                                    pd.Series({'KEY': 'prescale-setpoint'})]).to_dict())
    for dclink in prescale_hvdc_sp.sort_values('lineEnergyIdentificationCodeEIC').to_dict('records'):
        logger.info(f"[INITIAL] PRE-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value'], 2)} MW")
        logger.debug(f"[INITIAL] PRE-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value_q'], 2)} MVar")

    # Mapping HVDC schedules to network
    _cols_to_keep = ['lineEnergyIdentificationCodeEIC', _country_col, 'ucte_xnode_code', 'power_factor']
    scalable_hvdc = dangling_lines[dangling_lines.isHvdc == 'true'][_cols_to_keep]
    scalable_hvdc.reset_index(inplace=True)
    scalable_hvdc = scalable_hvdc.merge(target_hvdc_sp_df, left_on='lineEnergyIdentificationCodeEIC', right_on='registered_resource')
    mask = (scalable_hvdc[_country_col] == scalable_hvdc['in_domain']) | (scalable_hvdc[_country_col] == scalable_hvdc['out_domain'])
    scalable_hvdc = scalable_hvdc[mask]
    mask = (scalable_hvdc[_country_col] == scalable_hvdc['in_domain']) & (scalable_hvdc['value'] > 0.0)
    scalable_hvdc['value'] = np.where(mask, scalable_hvdc['value'] * -1, scalable_hvdc['value'])
    # sorting values by abs() in descending order to be able to drop_duplicates() later
    scalable_hvdc = scalable_hvdc.loc[scalable_hvdc['value'].abs().sort_values(ascending=False).index]
    # drop duplicates by index and keep first rows (because df already sorted)
    scalable_hvdc = scalable_hvdc.drop_duplicates(subset='id', keep='first')
    scalable_hvdc = scalable_hvdc.set_index('id')

    # Updating HVDC network elements to scheduled values
    scalable_hvdc_target = scalable_hvdc[['value', 'lineEnergyIdentificationCodeEIC', 'power_factor']]
    if _CONSTANT_POWER_FACTOR:
        scalable_hvdc_target['value_q'] = scalable_hvdc_target.value * scalable_hvdc_target.power_factor  # ensure power factor is kept
    else:
        scalable_hvdc_target['value_q'] = dangling_lines.loc[scalable_hvdc_target.index].q0
    network.update_dangling_lines(id=scalable_hvdc_target.index, p0=scalable_hvdc_target.value, q0=scalable_hvdc_target.value_q)
    _hvdc_results.append(pd.concat([scalable_hvdc_target.set_index('lineEnergyIdentificationCodeEIC').value,
                                    pd.Series({'KEY': 'postscale-setpoint'})]).to_dict())
    logger.info(f"[INITIAL] HVDC elements updated to target values: {scalable_hvdc_target['lineEnergyIdentificationCodeEIC'].values}")
    for dclink in scalable_hvdc_target.sort_values('lineEnergyIdentificationCodeEIC').to_dict('records'):
        logger.info(f"[INITIAL] POST-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value'], 2)} MW")
        logger.debug(f"[INITIAL] POST-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value_q'], 2)} MVar")

    # Get AC net positions scaling perimeter -> non-negative ConformLoads
    loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True)
    loads = loads.merge(network.get_extensions('detail'), right_index=True, left_index=True)
    loads['power_factor'] = loads.q0 / loads.p0  # estimate the power factor of loads
    loads['power_factor'] = loads['power_factor'].clip(-float(POWER_FACTOR_THRESHOLD), float(POWER_FACTOR_THRESHOLD))
    conform_loads = loads[loads['variable_p0'] > 0]

    # Get network slack generators
    slack_generators = get_slack_generators(network)  # TODO
    logger.info(f"[INITIAL] Network slack generators: {slack_generators.name.to_list()}")

    # Solving initial loadflow
    converged_components = {}
    pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
    for result in [x for x in pf_results if x.connected_component_num in _components.keys()]:
        result_dict = attr_to_dict(result)
        logger.info(f"[INITIAL] Loadflow status: {result_dict.get('status').name}")
        logger.debug(f"[INITIAL] Loadflow results: {result_dict}")
        if not result.status.value:
            converged_components[result.connected_component_num] = _components[result.connected_component_num]
    else:
        if pf_results[0].status.value:
            logger.error(f"Terminating network scaling due to divergence in main island")
            return model

    # Get dangling lines after HVDC scaling and loadflow
    # dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    dangling_lines = network.get_dangling_lines(all_attributes=True)
    dangling_lines[_country_col] = dangling_lines.index.map(_elements_to_areas_map[_country_col])
    ## Merge buses to dangling lines in order to know dangling lines network component
    dangling_lines = dangling_lines.merge(buses.connected_component, how='left', left_on='bus_id', right_index=True)
    dangling_lines = _set_power_ratio_to_dangling_lines(dangling_lines)
    dangling_lines['boundary_p'] = dangling_lines['boundary_p'] * -1  # invert boundary_p sign to match flow direction

    # Validate existence of internal islands and exclude them
    converged_components = validate_converged_components(dangling_lines=dangling_lines, converged_components=converged_components)
    valid_components = {k: copy.deepcopy(v) for k, v in converged_components.items() if v['state'] == 'valid'}

    # Get pre-scale total network balance by each component -> AC+DC net position
    prescale_network_np = {k: round(dangling_lines[dangling_lines.connected_component == k].boundary_p.sum()) for k, v in valid_components.items()}
    _scaling_results.append({'KEY': 'prescale-network-np', 'GLOBAL': prescale_network_np, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] PRE-SCALE NETWORK NP by component: {prescale_network_np}")

    # Get pre-scale total network balance by each component -> AC net position
    unpaired_dangling_lines = (dangling_lines.isHvdc == '') & (dangling_lines.tie_line_id == '')
    prescale_network_acnp = {k: round(dangling_lines[unpaired_dangling_lines].query("connected_component == @k").boundary_p.sum()) for k, v in valid_components.items()}
    _scaling_results.append({'KEY': 'prescale-network-acnp', 'GLOBAL': prescale_network_acnp, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] PRE-SCALE NETWORK ACNP by component: {prescale_network_acnp}")

    # Identify fragmented IGMs - where some part of network model with boundary belongs other component
    areas_to_components = get_countries_to_components(components=valid_components)
    fragments_participation = get_fragmented_areas_participation(unpaired_dangling_lines=dangling_lines[unpaired_dangling_lines],
                                                                 areas_to_components=areas_to_components)

    # Map fragmented models to target ACNP schedules and recalculate values by participation
    target_acnp_df['connected_component'] = target_acnp_df['registered_resource'].map(areas_to_components)
    target_acnp_df = target_acnp_df.explode('connected_component')
    target_acnp_df = target_acnp_df.merge(fragments_participation, on=['connected_component', 'registered_resource'], how='left')
    target_acnp_df['participation'] = target_acnp_df['participation'].astype(float).fillna(1)  # non fragmented areas participation set to 1
    target_acnp_df['value'] = target_acnp_df['value'] * target_acnp_df['participation']
    target_acnp_df['value'] = target_acnp_df['value'].round(1)

    # Validate total network AC net position from schedules to network model and scale to meet scheduled (per each component)
    # Scaling is done through unpaired AC dangling lines
    # From target_acnp variable need to take only areas which are present in network model
    # TODO discuss whether to scale only converged islands or try on all. Currently scales converged higher than 5 buses
    logger.info(f"Scaling each existing island external injections to meet total island ACNP target schedule")
    target_network_acnp = {}
    for component_key, v in valid_components.items():
        scheduled_component_acnp = float(target_acnp_df[target_acnp_df.connected_component == component_key]['value'].sum().round(1))
        target_network_acnp[component_key] = round(scheduled_component_acnp)  # preserve for scaling report
        relevant_dangling_lines = dangling_lines[unpaired_dangling_lines].query("connected_component == @component_key")
        relevant_dangling_lines['participation'] = relevant_dangling_lines.boundary_p.abs() / relevant_dangling_lines.boundary_p.abs().sum()
        offset_network_acnp = prescale_network_acnp.get(component_key) - scheduled_component_acnp
        prescale_network_acnp_diff = offset_network_acnp * relevant_dangling_lines.participation
        prescale_network_acnp_target = relevant_dangling_lines.p0 - prescale_network_acnp_diff
        prescale_network_acnp_target.dropna(inplace=True)
        if _CONSTANT_POWER_FACTOR:
            _component_dl_q_values = prescale_network_acnp_target * relevant_dangling_lines.power_factor
        else:
            _component_dl_q_values = relevant_dangling_lines.q0
        logger.info(f"[ITER {_iteration}] Scaling network component {component_key} {v['countries']} ACNP to scheduled: {scheduled_component_acnp}")
        network.update_dangling_lines(id=prescale_network_acnp_target.index,
                                      p0=prescale_network_acnp_target.to_list(),
                                      q0=_component_dl_q_values.to_list())
    _scaling_results.append({'KEY': 'target-network-acnp', 'GLOBAL': target_network_acnp, 'ITER': _iteration})

    # Solving loadflow after aligning total network AC net position to scheduled
    pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
    for result in [x for x in pf_results if x.connected_component_num in _components.keys()]:
        result_dict = attr_to_dict(result)
        logger.info(f"[ITER {_iteration}] Loadflow status: {result_dict.get('status').name}")
        logger.debug(f"[ITER {_iteration}] Loadflow results: {result_dict}")

    # Check loadflow status
    # TODO need to consider how to evaluate it in case of multiple islands. For example if one of the island diverges but not the main
    if not validate_loadflow_status(results=pf_results, components=valid_components):
        model.scaled = False
        logger.warning(f"Terminating network scaling due to divergence in main island after island ACNP alignment")
        return model

    # Validate total network AC net position alignment
    # dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    dangling_lines = network.get_dangling_lines(all_attributes=True)
    dangling_lines[_country_col] = dangling_lines.index.map(_elements_to_areas_map[_country_col])
    dangling_lines = dangling_lines.merge(buses.connected_component, how='left', left_on='bus_id', right_index=True)
    dangling_lines['boundary_p'] = dangling_lines['boundary_p'] * -1  # invert boundary_p sign to match flow direction
    postscale_network_acnp = {k: round(dangling_lines[unpaired_dangling_lines].query("connected_component == @k").boundary_p.sum()) for k, v in valid_components.items()}
    _scaling_results.append({'KEY': 'postscale-network-acnp', 'GLOBAL': postscale_network_acnp, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] POST-SCALE NETWORK ACNP by component: {postscale_network_acnp}")

    # Get pre-scale generation and consumption
    if debug:
        prescale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR')
        prescale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD')
        _scaling_results.append(pd.concat([prescale_generation, pd.Series({'KEY': 'generation', 'ITER': _iteration})]).to_dict())
        _scaling_results.append(pd.concat([prescale_consumption, pd.Series({'KEY': 'consumption', 'ITER': _iteration})]).to_dict())

    # Get pre-scale AC net positions for each control area
    dangling_lines = dangling_lines[dangling_lines.connected_component.isin(valid_components.keys())]
    prescale_acnp = dangling_lines[dangling_lines.isHvdc == ''].groupby([_country_col, 'connected_component']).boundary_p.sum().reset_index()
    prescale_acnp.connected_component = prescale_acnp.connected_component.astype(int)
    _pre_scale_acnp_series = _get_series_from_df(df=prescale_acnp, value_col='boundary_p')
    _scaling_results.append(pd.concat([_pre_scale_acnp_series, pd.Series({'KEY': 'prescale-acnp', 'ITER': _iteration})]).to_dict())
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP: {_pre_scale_acnp_series.to_dict()}")

    # Filtering target AC net positions series by present regions in network
    combined_scaling_target_df = target_acnp_df.merge(prescale_acnp, how='inner',
                                                      left_on=['connected_component', 'registered_resource'],
                                                      right_on=['connected_component', _country_col]
                                                      )
    target_acnp = _get_series_from_df(df=combined_scaling_target_df, area_col='registered_resource', value_col='value')
    _scaling_results.append(pd.concat([target_acnp, pd.Series({'KEY': 'target-acnp', 'ITER': _iteration})]).to_dict())
    logger.info(f"[ITER {_iteration}] TARGET ACNP: {target_acnp.to_dict()}")

    # Get offsets between target and pre-scale AC net positions for each control area
    combined_scaling_target_df['offset_acnp'] = combined_scaling_target_df['boundary_p'] - combined_scaling_target_df['value']
    offset_acnp = _get_series_from_df(df=combined_scaling_target_df, area_col='registered_resource', value_col='offset_acnp')
    _scaling_results.append(pd.concat([offset_acnp, pd.Series({'KEY': 'offset-acnp', 'ITER': _iteration})]).to_dict())
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP offset: {offset_acnp.round(1).to_dict()}")

    # Perform scaling of AC part schedule of the network model with loop
    logger.info(f"Scaling AC network part")
    while _iteration < int(MAX_ITERATION):
        _iteration += 1

        # Get scaling area loads participation factors
        scalable_loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads.index)
        scalable_loads = scalable_loads.merge(buses.connected_component, left_on='bus_id', right_index=True, how='left')
        scalable_loads['p_participation'] = scalable_loads.p0 / scalable_loads.groupby([_country_col, 'connected_component']).p0.transform('sum')

        # Merge ACNP offsets to scalable loads
        scalable_loads = scalable_loads.reset_index().merge(
            combined_scaling_target_df[[_country_col, 'connected_component', 'offset_acnp']],
            how='left', on=[_country_col, 'connected_component']).set_index('id')

        # Scale loads by participation factor
        scalable_loads_diff = scalable_loads.offset_acnp * scalable_loads.p_participation
        scalable_loads_target = scalable_loads.p0 + scalable_loads_diff
        ## Removing loads which target value is NaN. It can be because missing target ACNP for this area
        scalable_loads_target.dropna(inplace=True)
        conform_loads_na = conform_loads.merge((scalable_loads_target.reset_index())[['id']],
                                               left_index=True, right_on='id').set_index('id')
        network.update_loads(id=scalable_loads_target.index,
                             p0=scalable_loads_target.to_list(),
                             q0=(scalable_loads_target * conform_loads_na.power_factor).to_list())  # maintain power factor

        # Solving post-scale loadflow
        pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
        for result in [x for x in pf_results if x.connected_component_num in _components.keys()]:
            result_dict = attr_to_dict(result)
            logger.info(f"[ITER {_iteration}] Loadflow status: {result_dict.get('status').name}")
            logger.debug(f"[ITER {_iteration}] Loadflow results: {result_dict}")

        # Check loadflow status
        if not validate_loadflow_status(results=pf_results, components=valid_components):
            model.scaled = False
            logger.warning(f"Terminating network scaling due to divergence in main island after iteration: {_iteration}")
            return model

        # Store distributed active power after AC part scaling
        distributed_power = round(pf_results[0].distributed_active_power, 2)
        _scaling_results.append({'KEY': 'distributed-power', 'GLOBAL': distributed_power, 'ITER': _iteration})

        # Get post-scale generation, consumption and losses
        if debug:
            postscale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR')
            postscale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD')
            _scaling_results.append(pd.concat([postscale_generation, pd.Series({'KEY': 'generation', 'ITER': _iteration})]).to_dict())
            _scaling_results.append(pd.concat([postscale_consumption, pd.Series({'KEY': 'consumption', 'ITER': _iteration})]).to_dict())

            # Get post-scale network losses by regions
            ## It is needed to estimate when loadflow engine balances entire network schedule with distributed slack enabled
            postscale_losses = get_areas_losses(network=network, buses=buses, components=valid_components)
            total_network_losses = postscale_losses.sum()
            _scaling_results.append(pd.concat([postscale_losses, pd.Series({'GLOBAL': total_network_losses, 'KEY': 'losses', 'ITER': _iteration})]).to_dict())
            logger.debug(f"[ITER {_iteration}] POST-SCALE LOSSES: {postscale_losses.to_dict()}")

        # Get post-scale AC net position
        # dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
        dangling_lines = network.get_dangling_lines(all_attributes=True)
        dangling_lines[_country_col] = dangling_lines.index.map(_elements_to_areas_map[_country_col])
        dangling_lines = dangling_lines.merge(buses.connected_component, how='left', left_on='bus_id', right_index=True)
        dangling_lines['boundary_p'] = dangling_lines['boundary_p'] * -1  # invert boundary_p sign to match flow direction
        dangling_lines = dangling_lines[dangling_lines.connected_component.isin(valid_components.keys())]
        postscale_acnp = dangling_lines[dangling_lines.isHvdc == ''].groupby([_country_col, 'connected_component']).boundary_p.sum().reset_index()
        postscale_acnp.connected_component = postscale_acnp.connected_component.astype(int)
        _post_scale_acnp_series = _get_series_from_df(df=postscale_acnp, value_col='boundary_p')
        _scaling_results.append(pd.concat([_post_scale_acnp_series, pd.Series({'KEY': 'postscale-acnp', 'ITER': _iteration})]).to_dict())
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP: {_post_scale_acnp_series.to_dict()}")

        # Get post-scale total network balance
        prescale_total_np = dangling_lines[dangling_lines['paired'] == False].boundary_p.sum()
        logger.info(f"[ITER {_iteration}] POST-SCALE TOTAL NP: {round(prescale_total_np, 2)}")

        # Get offset between target and post-scale AC net position
        ## Drop values of boundary_p and offset from first iteration
        combined_scaling_target_df = combined_scaling_target_df.drop(columns=['offset_acnp', 'boundary_p'])
        combined_scaling_target_df = combined_scaling_target_df.merge(postscale_acnp, how='left',
                                                                      on=[_country_col, 'connected_component'])
        ## Recalculate new offset AC net position
        combined_scaling_target_df['offset_acnp'] = combined_scaling_target_df['boundary_p'] - combined_scaling_target_df['value']
        offset_acnp = _get_series_from_df(df=combined_scaling_target_df, area_col='registered_resource', value_col='offset_acnp')
        _scaling_results.append(pd.concat([offset_acnp, pd.Series({'KEY': 'offset-acnp', 'ITER': _iteration})]).to_dict())
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP offsets: {offset_acnp.to_dict()}")

        # Breaking scaling loop if target ac net position for all areas is reached
        if all(abs(offset_acnp.values) <= int(BALANCE_THRESHOLD)):
            logger.info(f"[ITER {_iteration}] Scaling successful as ACNP offsets less than threshold: {int(BALANCE_THRESHOLD)} MW")
            break
    else:
        logger.warning(f"Max iteration limit reached")
        # TODO actions after scale break

    # Post-processing scaling results dataframe
    ac_scaling_results_df = pd.DataFrame(_scaling_results).set_index('ITER').sort_index().round(2)
    hvdc_results_df = pd.DataFrame(_hvdc_results).round(2)

    # Process data for merge report
    filtered_df = ac_scaling_results_df.query("KEY in ['prescale-acnp', 'postscale-acnp', 'offset-acnp']")
    filtered_df = filtered_df.loc[[0, filtered_df.index.max()]]
    filtered_df = filtered_df.drop(columns='GLOBAL')
    filtered_df.loc[(filtered_df.index == 0) & (filtered_df['KEY'] == 'offset-acnp'), 'KEY'] = 'initial-offset-acnp'
    filtered_df.loc[(filtered_df.index != 0) & (filtered_df['KEY'] == 'offset-acnp'), 'KEY'] = 'final-offset-acnp'
    filtered_df = filtered_df.dropna(axis=1)
    filtered_df['KEY'] = filtered_df['KEY'].str.replace('-', '_')
    ac_melted_df = filtered_df.melt(id_vars=['KEY'], var_name='area', value_name='value')
    ac_pivoted_df = ac_melted_df.pivot(index='area', columns='KEY', values='value').reset_index()
    ac_pivoted_df['success'] = abs(ac_pivoted_df['final_offset_acnp']) <= int(BALANCE_THRESHOLD)
    ac_scale_report_dict = ac_pivoted_df.astype(object).where(pd.notna(ac_pivoted_df), None).to_dict('records')

    hvdc_results_df['KEY'] = hvdc_results_df['KEY'].str.replace('-', '_')
    hvdc_melted_df = hvdc_results_df.melt(id_vars=['KEY'], var_name='name', value_name='value')
    hvdc_pivoted_df = hvdc_melted_df.pivot(index='name', columns='KEY', values='value').reset_index()
    hvdc_scale_report_dict = hvdc_pivoted_df.astype(object).where(pd.notna(hvdc_pivoted_df), None).to_dict('records')

    # Include data in merge report
    model.scaled_entity = ac_scale_report_dict
    model.scaled_hvdc = hvdc_scale_report_dict

    # Set the common scaling status flag
    model.scaled = all(ac_pivoted_df['success'])

    return model


################################################    BACKLOG    #########################################################

# # Balancing network to get distributed slack active power close to zero by scaling conform loads of entire network
# # Distributed active power will be scaled by each area sum load participation
# scalable_loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads.index)
# scalable_loads['p_participation'] = scalable_loads.p0 / scalable_loads.p0.sum()
#
# ## Scale loads by participation factor
# distributed_power = round(pf_results[0].distributed_active_power, 2)  # using only from main connected component
# _scaling_results.append({'KEY': 'distributed-power', 'GLOBAL': distributed_power, 'ITER': _iteration})
# scalable_loads_diff = (distributed_power * scalable_loads.p_participation) * correction_factor
# scalable_loads_target = scalable_loads.p0 - scalable_loads_diff
# scalable_loads_target.dropna(inplace=True)  # removing loads which target value is NaN. It can be because missing target ACNP for this area
# logger.info(f"[INITIAL] Balancing the network model to reduce to distributed active power: {distributed_power} MW")
# network.update_loads(id=scalable_loads_target.index,
#                      p0=scalable_loads_target.to_list(),
#                      q0=(scalable_loads_target * conform_loads.power_factor).to_list())  # maintain power factor
#
# # Solving loadflow after balancing the network
# pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
# for result in [x for x in pf_results if x.connected_component_num in _components.keys()]:
#     result_dict = attr_to_dict(result)
#     logger.info(f"[INITIAL] Loadflow status: {result_dict.get('status').name}")
#     logger.debug(f"[INITIAL] Loadflow results: {result_dict}")
#
# # Log distributed active power after network balancing
# distributed_power = round(pf_results[0].distributed_active_power, 2)
# _scaling_results.append({'KEY': 'distributed-power', 'GLOBAL': distributed_power, 'ITER': _iteration})
# logger.info(f"[INITIAL] Distributed active power after network balancing: {distributed_power} MW")


# # Distributed slack balancing in each iteration
# scalable_loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads.index)
# scalable_loads['p_participation'] = scalable_loads.p0 / scalable_loads.p0.sum()
# scalable_loads_diff = (distributed_power * scalable_loads.p_participation) * correction_factor
# scalable_loads_target = scalable_loads.p0 - scalable_loads_diff
# scalable_loads_target.dropna(inplace=True)  # removing loads which target value is NaN. It can be because missing target ACNP for this area
# logger.info(f"[ITER {_iteration}] Balancing the network model to reduce to distributed active power: {distributed_power} MW")
# network.update_loads(id=scalable_loads_target.index,
#                      p0=scalable_loads_target.to_list(),
#                      q0=(scalable_loads_target * conform_loads.power_factor).to_list())  # maintain power factor
#
# pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
# for result in [x for x in pf_results if x.connected_component_num in _components.keys()]:
#     result_dict = attr_to_dict(result)
#     logger.info(f"[ITER {_iteration}] Loadflow status: {result_dict.get('status').name}")
#     logger.debug(f"[ITER {_iteration}] Loadflow results: {result_dict}")

def hvdc_schedule_mapper(row, country_col_name: str = 'country'):
    """BACKLOG FUNCTION. CURRENTLY NOT USED"""
    schedules = pd.DataFrame(target_dcnp)
    eic_mask = schedules['TimeSeries.connectingLine_RegisteredResource.mRID'] == row['lineEnergyIdentificationCodeEIC']
    in_domain_mask = schedules["TimeSeries.in_Domain.regionName"] == row[country_col_name]
    out_domain_mask = schedules["TimeSeries.out_Domain.regionName"] == row[country_col_name]
    relevant_schedule = schedules[(eic_mask) & ((in_domain_mask) | (out_domain_mask))]

    if relevant_schedule.empty:
        logger.warning(f"No schedule available for resource: {row['lineEnergyIdentificationCodeEIC']}")
        return None

    if relevant_schedule["TimeSeries.in_Domain.regionName"].notnull().squeeze():
        return relevant_schedule["value"].squeeze() * -1
    elif relevant_schedule["TimeSeries.out_Domain.regionName"].notnull().squeeze():
        return relevant_schedule["value"].squeeze()
    else:
        logger.warning(f"Not able to define schedule direction for resource: {row['lineEnergyIdentificationCodeEIC']}")
        return None


if __name__ == "__main__":
    # Testing
    import sys

    class MergedModel:
        pass

    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    model_path = r"C:\Users\martynas.karobcikas\Documents\models\rmm\rmm_05_001_20250214T0330Z.zip"
    network = pp.network.load(model_path, parameters={"iidm.import.cgmes.source-for-iidm-id": "rdfID"})
    merged_model = MergedModel()
    merged_model.network = network

    # Query target schedules
    ac_schedules = query_acnp_schedules(time_horizon="ID", scenario_timestamp="2025-02-14T03:30:00Z")
    dc_schedules = query_hvdc_schedules(time_horizon="ID", scenario_timestamp="2025-02-14T03:30:00Z")

    # dc_schedules = [{'value': 350,
    #                  'in_domain': None,
    #                  'out_domain': 'LT',
    #                  'registered_resource': '10T-LT-SE-000013'},
    #                 {'value': 320,
    #                  'in_domain': 'LT',
    #                  'out_domain': None,
    #                  'registered_resource': '10T-LT-PL-000037'}
    #                 ]
    #
    # # ac_schedules.append({"value": 400, "in_domain": "LT", "out_domain": None})
    # ac_schedules = [
    #     {"value": 200, "in_domain": "LT", "out_domain": None},
    #     {"value": 100, "in_domain": None, "out_domain": "LV"},
    # ]

    network = scale_balance(model=merged_model, ac_schedules=ac_schedules, dc_schedules=dc_schedules, debug=True)
    # print(network.ac_scaling_results_df)

    # # Results analysis
    # print(network.ac_scaling_results_df.query("KEY == 'generation'"))
    # print(network.ac_scaling_results_df.query("KEY == 'consumption'"))
    # print(network.ac_scaling_results_df.query("KEY == 'offset-acnp'"))

    # Other examples
    # loads = network.get_loads(id=network.get_elements_ids(element_type=pp.network.ElementType.LOAD, countries=['LT'])