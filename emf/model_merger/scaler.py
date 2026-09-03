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
import polars as pl
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


def _pl_from_pypowsybl(df: pd.DataFrame, index_name: str = 'id') -> pl.DataFrame:
    """pypowsybl only returns pandas; keep its index (element id) as a real column since
    polars has no index concept."""
    name = df.index.name or index_name
    return pl.from_pandas(df.rename_axis(name).reset_index())


def _area_key_frame(df: pl.DataFrame, value_col: str, area_col: str = _country_col) -> pl.DataFrame:
    """Build an 'area_key' column (country + "-" + connected_component) and sum value_col
    per area_key -- equivalent to pandas' string-concatenated-index + groupby(level=0)."""
    return (
        df.with_columns(
            (pl.col(area_col).cast(pl.Utf8) + "-" + pl.col('connected_component').cast(pl.Utf8)).alias('area_key')
        )
        .group_by('area_key')
        .agg(pl.col(value_col).sum().alias('value'))
        .with_columns(pl.col('value').round(1))
        .sort('area_key')
    )


def _to_area_dict(frame: pl.DataFrame, key_col: str = 'area_key', val_col: str = 'value') -> dict:
    """Flatten a 2-column polars frame into a plain dict for report rows."""
    return dict(zip(frame[key_col].to_list(), frame[val_col].to_list()))


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


def get_areas_losses(network: pp.network.Network, buses: pl.DataFrame, components: Dict,
                     voltage_levels: pd.DataFrame = None, substations: pd.DataFrame = None) -> pl.DataFrame:
    # Calculate ACNP with losses (from cross-border lines)
    boundary_lines = _pl_from_pypowsybl(get_network_elements(network, pp.network.ElementType.BOUNDARY_LINE, all_attributes=True,
                                                              voltage_levels=voltage_levels, substations=substations))
    boundary_lines = boundary_lines.join(
        buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}),
        on='bus_id', how='left'
    )
    boundary_lines = boundary_lines.filter(pl.col('connected_component').is_in(list(components.keys())))
    boundary_lines = boundary_lines.with_columns(pl.col('connected_component').cast(pl.Int64))
    boundary_lines = boundary_lines.with_columns((pl.col('boundary_p') * -1).alias('boundary_p'))  # invert boundary_p sign to match flow direction
    ac_boundary_lines = boundary_lines.filter(pl.col('isHvdc') == '')
    dc_boundary_lines = boundary_lines.filter(pl.col('isHvdc') == 'true')
    acnp_with_losses = _area_key_frame(df=ac_boundary_lines, value_col='boundary_p')

    # Calculate ACNP without losses (from generation and consumption)
    generation = get_areas_metrics(network=network, buses=buses, components=components, metric='GENERATOR',
                                   voltage_levels=voltage_levels, substations=substations)
    consumption = get_areas_metrics(network=network, buses=buses, components=components, metric='LOAD',
                                    voltage_levels=voltage_levels, substations=substations)
    dcnp = _area_key_frame(df=dc_boundary_lines, value_col='boundary_p')

    # full join replaces pandas' index-alignment subtraction. Only dcnp gets fill_null(0)
    # (matches pandas' explicit reindex fill); the rest stay null on mismatch and propagate.
    losses = (
        generation.rename({'value': 'generation'})
        .join(consumption.rename({'value': 'consumption'}), on='area_key', how='full', coalesce=True)
        .join(dcnp.rename({'value': 'dcnp'}), on='area_key', how='full', coalesce=True)
        .join(acnp_with_losses.rename({'value': 'acnp_with_losses'}), on='area_key', how='full', coalesce=True)
        .with_columns(pl.col('dcnp').fill_null(0))
        .with_columns(
            (pl.col('generation') - pl.col('consumption') - pl.col('dcnp') - pl.col('acnp_with_losses'))
            .round(1)
            .alias('value')
        )
        .select(['area_key', 'value'])
        .sort('area_key')
    )

    return losses


def get_areas_metrics(network: pp.network.Network, buses: pl.DataFrame, components: Dict, metric: str,
                      voltage_levels: pd.DataFrame = None, substations: pd.DataFrame = None) -> pl.DataFrame:
    df = _pl_from_pypowsybl(get_network_elements(network, getattr(pp.network.ElementType, metric), all_attributes=True,
                                                 voltage_levels=voltage_levels, substations=substations))
    df = df.join(
        buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}),
        on='bus_id', how='left'
    )
    df = df.filter(pl.col('connected_component').is_in(list(components.keys())))
    df = df.with_columns(pl.col('connected_component').cast(pl.Int64))
    sign = -1 if metric == 'GENERATOR' else 1
    series = _area_key_frame(df=df, value_col='p')
    return series.with_columns((pl.col('value') * sign).alias('value'))


def validate_converged_components(boundary_lines: pl.DataFrame, converged_components: Dict):
    logger.info(f"Validating converged islands")
    for k, v in list(converged_components.items()):
        v['state'] = 'valid'
        # In case of internal island it should contain only one area
        if len(v['countries']) == 1:
            component_boundary_lines = boundary_lines.filter(pl.col('connected_component') == k)
            # Check if there are any boundary lines which belongs to component
            if component_boundary_lines.is_empty():
                v['state'] = 'internal'
                logger.warning(f"Network component {k} considered as internal area island, excluding from scaling: {v}")

    return converged_components


def get_network_elements_map_to_areas(network: pp.network) -> pl.DataFrame:
    _temp = []
    # Network import parameters have to be set to use subnetworks in order to use this function!
    sub_network_ids = network.get_sub_networks(all_attributes=True).index
    if sub_network_ids.empty:
        logger.error(f"Subnetworks does not exists in network model or disabled by import parameters")
        raise Exception("Scaling terminated due to missing subnetworks")
    for id in sub_network_ids:
        subnetwork = network.get_sub_network(id)
        country_id = subnetwork.get_substations(all_attributes=True)[_country_col].unique().tolist()[0]
        identifiables = _pl_from_pypowsybl(subnetwork.get_identifiables())
        identifiables = identifiables.with_columns(pl.lit(country_id).alias(_country_col))
        _temp.append(identifiables)

    return pl.concat(_temp, how='diagonal_relaxed')  # tolerates per-subnetwork column/dtype mismatches


def get_countries_to_components(components: Dict):
    country_to_keys = defaultdict(set)
    for key, entry in components.items():
        for country in entry.get('countries', []):
            country_to_keys[country].add(key)

    return country_to_keys


def get_fragmented_areas_participation(unpaired_boundary_lines: pl.DataFrame, areas_to_components: Dict) -> pl.DataFrame:
    fragmented_areas = []
    for area, comps in areas_to_components.items():
        if len(comps) > 1:
            logger.debug(f"Fragmented area identified: {area} in components {list(comps)}")
            area_boundary_lines = unpaired_boundary_lines.filter(pl.col(_country_col) == area)
            fragments_acnp = {
                comp: area_boundary_lines.filter(pl.col('connected_component') == comp)['boundary_p'].sum() or 0.0
                for comp in comps
            }
            total_fragments_acnp = abs(sum(fragments_acnp.values())) or 1  # removing zero division warning
            participation = {k: abs(v) / total_fragments_acnp for k, v in fragments_acnp.items()}
            fragmented_areas.append(pl.DataFrame({
                'connected_component': list(participation.keys()),
                'participation': list(participation.values()),
                'registered_resource': [area] * len(participation),
            }))

    if fragmented_areas:
        return pl.concat(fragmented_areas, how='diagonal_relaxed')
    else:
        return pl.DataFrame(schema={'connected_component': pl.Int64, 'participation': pl.Float64, 'registered_resource': pl.Utf8})


def _set_power_ratio_to_boundary_lines(df: pl.DataFrame) -> pl.DataFrame:
    # polars distinguishes NaN from null (pandas' .fillna(0) doesn't) -- fill both
    return df.with_columns(
        (pl.col('boundary_q') / pl.col('boundary_p')).fill_nan(0.0).alias('power_factor')
    ).with_columns(
        pl.col('power_factor').fill_null(0.0).clip(-float(POWER_FACTOR_THRESHOLD), float(POWER_FACTOR_THRESHOLD))
    )


@performance_counter(units='seconds')
def scale_balance(model: object,
                  ac_schedules: List[Dict[str, Union[str, float, None]]],
                  dc_schedules: List[Dict[str, Union[str, float, None]]],
                  *,
                  debug: bool,
                  lf_settings: pp.loadflow.Parameters = EU_RELAXED,
                  ):
    """
    Main method to scale each CGM area to target balance
    :param network: pypowsybl network object
    :param ac_schedules: target AC net positions in list of dict format
    :param dc_schedules: target DC net positions in list of dict format
    :param debug: when True, also computes extra generation/consumption/losses diagnostics
        included in the scaling report. Required (no default) -- the caller must source it
        from the task's task_properties.debug (see get_task_debug_flag), not from a local or
        properties-file default, so this can never silently diverge from the task config.
        Does not affect logging -- verbose scaling detail is always logged at DEBUG level and
        always reaches Elasticsearch regardless; this flag only controls whether the console
        shows it (see custom_logger.set_console_log_level)
    :param lf_settings: loadflow settings
    :return: scaled pypowsybl network object
    """
    logger.info(f"Network scaling initialized")

    # Get pypowsybl network
    network = model.network

    # Topology (voltage levels/substations) is static for the whole scaling run - only element
    # setpoints change below via update_boundary_lines/update_loads, never topology. Fetch both
    # once and reuse them everywhere get_network_elements()
    _voltage_levels_cache = network.get_voltage_levels(all_attributes=True)
    _substations_cache = network.get_substations(all_attributes=True)

    # Define general variables to be used in scaling algorithm
    _CONSTANT_POWER_FACTOR = json.loads(CONSTANT_POWER_FACTOR.lower())
    _components = get_connected_components_data(network=network, bus_count_threshold=5, country_col_name=_country_col,
                                                 voltage_levels=_voltage_levels_cache, substations=_substations_cache)
    _scaling_results = []
    _hvdc_results = []
    _iteration = 0

    # Get entire network elements mapping to areas
    _elements_to_areas_map = get_network_elements_map_to_areas(network=network)  # polars: columns include 'id', _country_col

    # Get buses
    buses = _pl_from_pypowsybl(network.get_buses())

    # Get all dangling lines and define power factor
    boundary_lines = _pl_from_pypowsybl(network.get_boundary_lines(all_attributes=True))
    boundary_lines = boundary_lines.join(
        _elements_to_areas_map.select(['id', _country_col]), on='id', how='left'
    )
    boundary_lines = _set_power_ratio_to_boundary_lines(boundary_lines)
    boundary_lines = boundary_lines.with_columns([
        (pl.col('boundary_p') * -1).alias('boundary_p'),  # invert boundary_p sign to match flow direction
        (pl.col('boundary_q') * -1).alias('boundary_q'),  # invert boundary_q sign to match flow direction (just used for printing)
    ])

    # Target HVDC setpoints
    target_hvdc_sp_df = pl.DataFrame(dc_schedules)

    # Target AC net positions mapping
    target_acnp_df = pl.DataFrame(ac_schedules)
    # ac_schedules can carry missing in_domain/out_domain as the literal string "NaN", not a
    # real null -- pl.coalesce() would pick that string as a real value. Null it out first.
    target_acnp_df = target_acnp_df.with_columns([
        pl.when(pl.col('in_domain').is_null() | pl.col('in_domain').is_in(['NaN', 'nan', 'NAN', '']))
        .then(None).otherwise(pl.col('in_domain')).alias('in_domain'),
        pl.when(pl.col('out_domain').is_null() | pl.col('out_domain').is_in(['NaN', 'nan', 'NAN', '']))
        .then(None).otherwise(pl.col('out_domain')).alias('out_domain'),
    ])
    target_acnp_df = target_acnp_df.with_columns(
        pl.coalesce(['in_domain', 'out_domain']).alias('registered_resource')
    )
    target_acnp_df = target_acnp_df.filter(pl.col('registered_resource').is_not_null())

    # .unique(keep='first', maintain_order=True) does NOT mean "first after my sort" -- it
    # preserves output row order, which let a duplicate zero-value row win over the real
    # one. Rank by abs(value) per group and keep rank 1 instead.
    target_acnp_df = (
        target_acnp_df
        .with_columns(pl.col('value').abs().alias('_abs_value'))
        .with_columns(
            pl.col('_abs_value').rank(method='ordinal', descending=True).over('registered_resource').alias('_rank')
        )
        .filter(pl.col('_rank') == 1)
        .drop(['_abs_value', '_rank'])
    )

    target_acnp_df = target_acnp_df.with_columns(
        pl.when((pl.col('in_domain').is_not_null()) & (pl.col('value') > 0.0))
        .then(pl.col('value') * -1)
        .otherwise(pl.col('value'))
        .alias('value')
    )

    # Validate presence of target AC net position by areas in network model
    present_areas = boundary_lines.select(_country_col).unique()[_country_col]
    target_registered = set(target_acnp_df['registered_resource'].to_list())
    missing_ac_schedule = [a for a in present_areas.to_list() if a not in target_registered]
    if missing_ac_schedule:
        # TODO consider exit scaling here if some schedules are missing
        logger.error(f"Missing target AC schedule for areas present in network model: {missing_ac_schedule}")

    # Get pre-scale HVDC setpoints
    logger.info(f"Scaling HVDC network part")
    prescale_hvdc_sp = (
        boundary_lines.filter(pl.col('isHvdc') == 'true')
        .select(['lineEnergyIdentificationCodeEIC', 'boundary_p', 'boundary_q'])
        .rename({'boundary_p': 'value', 'boundary_q': 'value_q'})
    )
    _hvdc_results.append({
        **dict(zip(prescale_hvdc_sp['lineEnergyIdentificationCodeEIC'].to_list(), prescale_hvdc_sp['value'].to_list())),
        'KEY': 'prescale-setpoint',
    })
    for dclink in prescale_hvdc_sp.sort('lineEnergyIdentificationCodeEIC').to_dicts():
        logger.debug(f"[INITIAL] PRE-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {dclink['value']} MW")
        logger.debug(f"[INITIAL] PRE-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {dclink['value_q']} MVar")

    # Mapping HVDC schedules to network
    _cols_to_keep = ['id', 'lineEnergyIdentificationCodeEIC', _country_col, 'ucte_xnode_code', 'power_factor']
    scalable_hvdc = boundary_lines.filter(pl.col('isHvdc') == 'true')
    # Ignore HVDC elements in update of setpoint which are disconnected by network model
    scalable_hvdc = scalable_hvdc.filter(pl.col('connected'))[_cols_to_keep]
    scalable_hvdc = scalable_hvdc.join(target_hvdc_sp_df, left_on='lineEnergyIdentificationCodeEIC', right_on='registered_resource', how='inner')
    missing_hvdc_target = scalable_hvdc.filter(pl.col('value').is_null())['lineEnergyIdentificationCodeEIC'].to_list()
    if missing_hvdc_target:
        raise ValueError(f"Missing target DC schedule value for HVDC links present in network model: {missing_hvdc_target}")
    scalable_hvdc = scalable_hvdc.filter(
        (pl.col(_country_col) == pl.col('in_domain')) | (pl.col(_country_col) == pl.col('out_domain'))
    )
    scalable_hvdc = scalable_hvdc.with_columns(
        pl.when((pl.col(_country_col) == pl.col('in_domain')) & (pl.col('value') > 0.0))
        .then(pl.col('value') * -1)
        .otherwise(pl.col('value'))
        .alias('value')
    )
    # keep the highest-magnitude row per id -- same .unique(keep='first') bug as above, same rank() fix
    scalable_hvdc = scalable_hvdc.with_columns(pl.col('value').abs().alias('_abs_value'))
    scalable_hvdc = scalable_hvdc.with_columns(
        pl.col('_abs_value').rank(method='ordinal', descending=True).over('id').alias('_rank')
    )
    scalable_hvdc = scalable_hvdc.filter(pl.col('_rank') == 1).drop(['_abs_value', '_rank'])

    # Updating HVDC network elements to scheduled values
    scalable_hvdc_target = scalable_hvdc.select(['id', 'value', 'lineEnergyIdentificationCodeEIC', 'power_factor'])
    if _CONSTANT_POWER_FACTOR:
        scalable_hvdc_target = scalable_hvdc_target.with_columns(
            (pl.col('value') * pl.col('power_factor')).alias('value_q')  # ensure power factor is kept
        )
    else:
        # maintain_order='left' keeps row order matching the positional id/p0/q0 lists below
        scalable_hvdc_target = scalable_hvdc_target.join(
            boundary_lines.select(['id', 'q0']), on='id', how='left', maintain_order='left'
        ).rename({'q0': 'value_q'})
    network.update_boundary_lines(
        id=scalable_hvdc_target['id'].to_list(),
        p0=scalable_hvdc_target['value'].to_list(),
        q0=scalable_hvdc_target['value_q'].to_list(),
    )
    _hvdc_results.append({
        **dict(zip(scalable_hvdc_target['lineEnergyIdentificationCodeEIC'].to_list(), scalable_hvdc_target['value'].to_list())),
        'KEY': 'postscale-setpoint',
    })
    logger.info(f"[INITIAL] HVDC elements updated to target values: {scalable_hvdc_target['lineEnergyIdentificationCodeEIC'].to_list()}")
    for dclink in scalable_hvdc_target.sort('lineEnergyIdentificationCodeEIC').to_dicts():
        logger.debug(f"[INITIAL] POST-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {dclink['value']} MW")
        logger.debug(f"[INITIAL] POST-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {dclink['value_q']} MVar")

    # Get AC net positions scaling perimeter -> non-negative ConformLoads
    loads = _pl_from_pypowsybl(get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True,
                                                     voltage_levels=_voltage_levels_cache, substations=_substations_cache))
    loads = loads.join(_pl_from_pypowsybl(network.get_extensions('detail')), on='id', how='inner')
    loads = loads.with_columns((pl.col('q0') / pl.col('p0')).alias('power_factor'))  # estimate the power factor of loads
    loads = loads.with_columns(pl.col('power_factor').clip(-float(POWER_FACTOR_THRESHOLD), float(POWER_FACTOR_THRESHOLD)))
    conform_loads = loads.filter(pl.col('variable_p0') > 0)

    # Get network slack generators
    # slack_generators = get_slack_generators(network)  # TODO
    # logger.info(f"[INITIAL] Network slack generators: {slack_generators.name.to_list()}")

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
            logger.debug(f"Terminating network scaling due to divergence in main island")
            return model

    # Get dangling lines after HVDC scaling and loadflow
    boundary_lines = _pl_from_pypowsybl(network.get_boundary_lines(all_attributes=True))
    boundary_lines = boundary_lines.join(_elements_to_areas_map.select(['id', _country_col]), on='id', how='left')
    ## Join buses to dangling lines in order to know dangling lines network component
    boundary_lines = boundary_lines.join(
        buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
    )
    boundary_lines = _set_power_ratio_to_boundary_lines(boundary_lines)
    boundary_lines = boundary_lines.with_columns((pl.col('boundary_p') * -1).alias('boundary_p'))  # invert boundary_p sign to match flow direction

    # Validate existence of internal islands and exclude them
    converged_components = validate_converged_components(boundary_lines=boundary_lines, converged_components=converged_components)
    valid_components = {k: copy.deepcopy(v) for k, v in converged_components.items() if v['state'] == 'valid'}

    # Get pre-scale total network balance by each component -> AC+DC net position
    # polars .sum() on an empty selection returns None, not 0 -- hence the `or 0.0` guards below
    prescale_network_np = {
        str(k): round(boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'prescale-network-np', 'GLOBAL': prescale_network_np, 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] PRE-SCALE NETWORK NP by component: {prescale_network_np}")

    # Get pre-scale total network balance by each component -> AC net position
    unpaired_boundary_lines_mask = (pl.col('isHvdc') == '') & (pl.col('tie_line_id') == '')
    unpaired_boundary_lines = boundary_lines.filter(unpaired_boundary_lines_mask)
    prescale_network_acnp = {
        str(k): round(unpaired_boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'prescale-network-acnp', 'GLOBAL': prescale_network_acnp, 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] PRE-SCALE NETWORK ACNP by component: {prescale_network_acnp}")

    # Identify fragmented IGMs - where some part of network model with boundary belongs other component
    areas_to_components = get_countries_to_components(components=valid_components)
    fragments_participation = get_fragmented_areas_participation(
        unpaired_boundary_lines=boundary_lines.filter(pl.col('isHvdc') == ''),
        areas_to_components=areas_to_components,
    )

    # Map fragmented models to target ACNP schedules and recalculate values by participation.
    # join()+explode('connected_component') produced wrong per-component assignments here, so
    # the (registered_resource, connected_component) pairs are built directly in Python instead.
    target_acnp_df = target_acnp_df.drop([c for c in ['connected_component'] if c in target_acnp_df.columns])
    _country_component_pairs = pl.DataFrame(
        [
            {'registered_resource': country, 'connected_component': comp}
            for country, comps in areas_to_components.items()
            for comp in comps
        ],
        schema={'registered_resource': pl.Utf8, 'connected_component': pl.Int64},
    ) if areas_to_components else pl.DataFrame(schema={'registered_resource': pl.Utf8, 'connected_component': pl.Int64})
    target_acnp_df = target_acnp_df.join(_country_component_pairs, on='registered_resource', how='left')
    target_acnp_df = target_acnp_df.join(fragments_participation, on=['connected_component', 'registered_resource'], how='left')
    target_acnp_df = target_acnp_df.with_columns(pl.col('participation').cast(pl.Float64).fill_null(1.0))  # non fragmented areas participation set to 1
    target_acnp_df = target_acnp_df.with_columns((pl.col('value') * pl.col('participation')).alias('value'))
    target_acnp_df = target_acnp_df.with_columns(pl.col('value').round(1))

    # Validate total network AC net position from schedules to network model and scale to meet scheduled (per each component)
    # Scaling is done through unpaired AC dangling lines
    # From target_acnp variable need to take only areas which are present in network model
    # TODO discuss whether to scale only converged islands or try on all. Currently scales converged higher than 5 buses
    logger.info(f"Scaling each existing island external injections to meet total island ACNP target schedule")
    target_network_acnp = {}
    for component_key, v in valid_components.items():
        scheduled_component_acnp = float(round(
            target_acnp_df.filter(pl.col('connected_component') == component_key)['value'].sum() or 0.0, 1
        ))
        target_network_acnp[str(component_key)] = round(scheduled_component_acnp)  # preserve for scaling report
        relevant_boundary_lines = unpaired_boundary_lines.filter(pl.col('connected_component') == component_key)
        total_abs_boundary_p = relevant_boundary_lines['boundary_p'].abs().sum() or 0.0
        relevant_boundary_lines = relevant_boundary_lines.with_columns(
            (pl.col('boundary_p').abs() / total_abs_boundary_p).alias('participation')
        )
        offset_network_acnp = prescale_network_acnp.get(str(component_key)) - scheduled_component_acnp
        relevant_boundary_lines = relevant_boundary_lines.with_columns(
            (pl.col('p0') - offset_network_acnp * pl.col('participation')).alias('prescale_network_acnp_target')
        )
        relevant_boundary_lines = relevant_boundary_lines.filter(pl.col('prescale_network_acnp_target').is_not_null())
        if _CONSTANT_POWER_FACTOR:
            relevant_boundary_lines = relevant_boundary_lines.with_columns(
                (pl.col('prescale_network_acnp_target') * pl.col('power_factor')).alias('_component_dl_q_values')
            )
        else:
            relevant_boundary_lines = relevant_boundary_lines.with_columns(pl.col('q0').alias('_component_dl_q_values'))
        logger.info(f"[ITER {_iteration}] Scaling network component {component_key} {v['countries']} ACNP to scheduled: {scheduled_component_acnp}")
        network.update_boundary_lines(
            id=relevant_boundary_lines['id'].to_list(),
            p0=relevant_boundary_lines['prescale_network_acnp_target'].to_list(),
            q0=relevant_boundary_lines['_component_dl_q_values'].to_list(),
        )
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
        logger.debug(f"Terminating network scaling due to divergence in main island after island ACNP alignment")
        return model

    # Validate total network AC net position alignment
    boundary_lines = _pl_from_pypowsybl(network.get_boundary_lines(all_attributes=True))
    boundary_lines = boundary_lines.join(_elements_to_areas_map.select(['id', _country_col]), on='id', how='left')
    boundary_lines = boundary_lines.join(
        buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
    )
    boundary_lines = boundary_lines.with_columns((pl.col('boundary_p') * -1).alias('boundary_p'))  # invert boundary_p sign to match flow direction
    unpaired_boundary_lines = boundary_lines.filter(unpaired_boundary_lines_mask)
    postscale_network_acnp = {
        str(k): round(unpaired_boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'postscale-network-acnp', 'GLOBAL': postscale_network_acnp, 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] POST-SCALE NETWORK ACNP by component: {postscale_network_acnp}")

    # Get pre-scale generation and consumption
    if debug:
        prescale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR',
                                                 voltage_levels=_voltage_levels_cache, substations=_substations_cache)
        prescale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD',
                                                 voltage_levels=_voltage_levels_cache, substations=_substations_cache)
        _scaling_results.append({**_to_area_dict(prescale_generation), 'KEY': 'generation', 'ITER': _iteration})
        _scaling_results.append({**_to_area_dict(prescale_consumption), 'KEY': 'consumption', 'ITER': _iteration})

    # Get pre-scale AC net positions for each control area
    boundary_lines = boundary_lines.filter(pl.col('connected_component').is_in(list(valid_components.keys())))
    prescale_acnp = (
        boundary_lines.filter(pl.col('isHvdc') == '')
        .group_by([_country_col, 'connected_component'])
        .agg(pl.col('boundary_p').sum())
    )
    prescale_acnp = prescale_acnp.with_columns(pl.col('connected_component').cast(pl.Int64))
    _pre_scale_acnp_frame = _area_key_frame(df=prescale_acnp, value_col='boundary_p')
    _scaling_results.append({**_to_area_dict(_pre_scale_acnp_frame), 'KEY': 'prescale-acnp', 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] PRE-SCALE ACNP: {_to_area_dict(_pre_scale_acnp_frame)}")

    # Filtering target AC net positions series by present regions in network
    combined_scaling_target_df = target_acnp_df.join(
        prescale_acnp, how='inner',
        left_on=['connected_component', 'registered_resource'],
        right_on=['connected_component', _country_col],
        coalesce=False,  # keeps CGMES.regionName as its own column instead of merging into registered_resource
    )
    target_acnp = _area_key_frame(df=combined_scaling_target_df, area_col='registered_resource', value_col='value')
    _scaling_results.append({**_to_area_dict(target_acnp), 'KEY': 'target-acnp', 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] TARGET ACNP: {_to_area_dict(target_acnp)}")

    # Get offsets between target and pre-scale AC net positions for each control area
    combined_scaling_target_df = combined_scaling_target_df.with_columns(
        (pl.col('boundary_p') - pl.col('value')).alias('offset_acnp')
    )
    offset_acnp = _area_key_frame(df=combined_scaling_target_df, area_col='registered_resource', value_col='offset_acnp')
    _scaling_results.append({**_to_area_dict(offset_acnp), 'KEY': 'offset-acnp', 'ITER': _iteration})
    logger.debug(f"[ITER {_iteration}] PRE-SCALE ACNP offset: {_to_area_dict(offset_acnp)}")

    # Perform scaling of AC part schedule of the network model with loop
    logger.info(f"Scaling AC network part")
    while _iteration < int(MAX_ITERATION):
        _iteration += 1

        # Get scaling area loads participation factors
        scalable_loads = _pl_from_pypowsybl(
            get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads['id'].to_list(),
                                 voltage_levels=_voltage_levels_cache, substations=_substations_cache)
        )
        scalable_loads = scalable_loads.join(
            buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
        )
        scalable_loads = scalable_loads.with_columns(
            (pl.col('p0') / pl.col('p0').sum().over([_country_col, 'connected_component'])).alias('p_participation')
        )

        # Join ACNP offsets to scalable loads
        scalable_loads = scalable_loads.join(
            combined_scaling_target_df.select([_country_col, 'connected_component', 'offset_acnp']),
            how='left', on=[_country_col, 'connected_component'],
        )

        # Scale loads by participation factor
        scalable_loads = scalable_loads.with_columns(
            (pl.col('offset_acnp') * pl.col('p_participation')).alias('scalable_loads_diff')
        ).with_columns(
            (pl.col('p0') + pl.col('scalable_loads_diff')).alias('scalable_loads_target')
        )
        ## Removing loads which target value is NaN. It can be because missing target ACNP for this area
        scalable_loads_target = scalable_loads.filter(pl.col('scalable_loads_target').is_not_null() & pl.col('scalable_loads_target').is_not_nan())
        # maintain_order='right' keeps conform_loads_na in scalable_loads_target's row order --
        # required for the positional q0 zip into network.update_loads() below.
        conform_loads_na = conform_loads.join(
            scalable_loads_target.select('id'), on='id', how='inner', maintain_order='right'
        )
        network.update_loads(
            id=scalable_loads_target['id'].to_list(),
            p0=scalable_loads_target['scalable_loads_target'].to_list(),
            q0=(scalable_loads_target['scalable_loads_target'] * conform_loads_na['power_factor']).to_list(),  # maintain power factor
        )

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
            postscale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR',
                                                      voltage_levels=_voltage_levels_cache, substations=_substations_cache)
            postscale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD',
                                                       voltage_levels=_voltage_levels_cache, substations=_substations_cache)
            _scaling_results.append({**_to_area_dict(postscale_generation), 'KEY': 'generation', 'ITER': _iteration})
            _scaling_results.append({**_to_area_dict(postscale_consumption), 'KEY': 'consumption', 'ITER': _iteration})

            # Get post-scale network losses by regions
            ## It is needed to estimate when loadflow engine balances entire network schedule with distributed slack enabled
            postscale_losses = get_areas_losses(network=network, buses=buses, components=valid_components,
                                                voltage_levels=_voltage_levels_cache, substations=_substations_cache)
            total_network_losses = postscale_losses['value'].sum() or 0.0
            _scaling_results.append({
                **_to_area_dict(postscale_losses), 'GLOBAL': total_network_losses, 'KEY': 'losses', 'ITER': _iteration
            })
            logger.debug(f"[ITER {_iteration}] POST-SCALE LOSSES: {_to_area_dict(postscale_losses)}")

        # Get post-scale AC net position
        boundary_lines = _pl_from_pypowsybl(network.get_boundary_lines(all_attributes=True))
        boundary_lines = boundary_lines.join(_elements_to_areas_map.select(['id', _country_col]), on='id', how='left')
        boundary_lines = boundary_lines.join(
            buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
        )
        boundary_lines = boundary_lines.with_columns((pl.col('boundary_p') * -1).alias('boundary_p'))  # invert boundary_p sign to match flow direction
        boundary_lines = boundary_lines.filter(pl.col('connected_component').is_in(list(valid_components.keys())))
        postscale_acnp = (
            boundary_lines.filter(pl.col('isHvdc') == '')
            .group_by([_country_col, 'connected_component'])
            .agg(pl.col('boundary_p').sum())
        )
        postscale_acnp = postscale_acnp.with_columns(pl.col('connected_component').cast(pl.Int64))
        _post_scale_acnp_frame = _area_key_frame(df=postscale_acnp, value_col='boundary_p')
        _scaling_results.append({**_to_area_dict(_post_scale_acnp_frame), 'KEY': 'postscale-acnp', 'ITER': _iteration})
        logger.debug(f"[ITER {_iteration}] POST-SCALE ACNP: {_to_area_dict(_post_scale_acnp_frame)}")

        # Get post-scale total network balance
        prescale_total_np = boundary_lines.filter(pl.col('paired') == False)['boundary_p'].sum() or 0.0
        logger.debug(f"[ITER {_iteration}] POST-SCALE TOTAL NP: {round(prescale_total_np, 2)}")

        # Get offset between target and post-scale AC net position
        ## Drop values of boundary_p and offset from first iteration
        combined_scaling_target_df = combined_scaling_target_df.drop(['offset_acnp', 'boundary_p'])
        combined_scaling_target_df = combined_scaling_target_df.join(
            postscale_acnp, how='left', on=[_country_col, 'connected_component']
        )
        ## Recalculate new offset AC net position
        combined_scaling_target_df = combined_scaling_target_df.with_columns(
            (pl.col('boundary_p') - pl.col('value')).alias('offset_acnp')
        )
        offset_acnp = _area_key_frame(df=combined_scaling_target_df, area_col='registered_resource', value_col='offset_acnp')
        _scaling_results.append({**_to_area_dict(offset_acnp), 'KEY': 'offset-acnp', 'ITER': _iteration})
        logger.debug(f"[ITER {_iteration}] POST-SCALE ACNP offsets: {_to_area_dict(offset_acnp)}")

        # Breaking scaling loop if target ac net position for all areas is reached
        if all(abs(v) <= int(BALANCE_THRESHOLD) for v in offset_acnp['value'].to_list()):
            logger.info(f"[ITER {_iteration}] Scaling successful as ACNP offsets less than threshold: {int(BALANCE_THRESHOLD)} MW")
            break
    else:
        logger.warning(f"Max iteration limit reached")
        # TODO actions after scale break

    # Post-processing scaling results dataframe. polars .round() isn't frame-wide like
    # pandas', so float columns are picked out explicitly below.
    ac_scaling_results_df = pl.DataFrame(_scaling_results, infer_schema_length=None).sort('ITER')
    ac_scaling_results_df = ac_scaling_results_df.with_columns(
        [pl.col(c).round(2) for c, dt in zip(ac_scaling_results_df.columns, ac_scaling_results_df.dtypes) if dt in (pl.Float64, pl.Float32)]
    )
    hvdc_results_df = pl.DataFrame(_hvdc_results, infer_schema_length=None)
    hvdc_results_df = hvdc_results_df.with_columns(
        [pl.col(c).round(2) for c, dt in zip(hvdc_results_df.columns, hvdc_results_df.dtypes) if dt in (pl.Float64, pl.Float32)]
    )

    # Process data for merge report
    filtered_df = ac_scaling_results_df.filter(pl.col('KEY').is_in(['prescale-acnp', 'postscale-acnp', 'offset-acnp']))
    # Select first + last row -- polars has no index, so a temp row-number column stands in
    filtered_df = filtered_df.with_row_index('_row_idx')
    filtered_df = filtered_df.filter((pl.col('_row_idx') == 0) | (pl.col('_row_idx') == filtered_df['_row_idx'].max()))
    is_first_row = pl.col('_row_idx') == 0
    filtered_df = filtered_df.with_columns(
        pl.when(is_first_row & (pl.col('KEY') == 'offset-acnp')).then(pl.lit('initial-offset-acnp'))
        .when((~is_first_row) & (pl.col('KEY') == 'offset-acnp')).then(pl.lit('final-offset-acnp'))
        .otherwise(pl.col('KEY')).alias('KEY')
    )

    filtered_df = filtered_df.drop(['_row_idx', 'GLOBAL'])
    # drop any column containing a null -- polars has no .dropna(axis=1) equivalent
    non_null_cols = [c for c in filtered_df.columns if filtered_df[c].null_count() < filtered_df.height]
    filtered_df = filtered_df.select(non_null_cols)
    filtered_df = filtered_df.with_columns(pl.col('KEY').str.replace_all('-', '_'))
    ac_melted_df = filtered_df.unpivot(index=['KEY'], variable_name='area', value_name='value')
    ac_pivoted_df = ac_melted_df.pivot(index='area', on='KEY', values='value')
    ac_pivoted_df = ac_pivoted_df.with_columns(
        (pl.col('final_offset_acnp').abs() <= int(BALANCE_THRESHOLD)).alias('success')
    )
    ac_scale_report_dict = ac_pivoted_df.to_dicts()  # polars nulls already serialize as None

    hvdc_results_df = hvdc_results_df.with_columns(pl.col('KEY').str.replace_all('-', '_'))
    hvdc_melted_df = hvdc_results_df.unpivot(index=['KEY'], variable_name='name', value_name='value')
    hvdc_pivoted_df = hvdc_melted_df.pivot(index='name', on='KEY', values='value')
    hvdc_scale_report_dict = hvdc_pivoted_df.to_dicts()

    # Include data in merge report
    model.scaled_entity = ac_scale_report_dict
    model.scaled_hvdc = hvdc_scale_report_dict

    # Set the common scaling status flag
    model.scaled = all(ac_pivoted_df['success'].to_list())

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
    schedules = pl.DataFrame(target_dcnp)
    relevant_schedule = schedules.filter(
        (pl.col('TimeSeries.connectingLine_RegisteredResource.mRID') == row['lineEnergyIdentificationCodeEIC'])
        & (
            (pl.col('TimeSeries.in_Domain.regionName') == row[country_col_name])
            | (pl.col('TimeSeries.out_Domain.regionName') == row[country_col_name])
        )
    )

    if relevant_schedule.is_empty():
        logger.warning(f"No schedule available for resource: {row['lineEnergyIdentificationCodeEIC']}")
        return None

    if relevant_schedule['TimeSeries.in_Domain.regionName'][0] is not None:
        return relevant_schedule['value'][0] * -1
    elif relevant_schedule['TimeSeries.out_Domain.regionName'][0] is not None:
        return relevant_schedule['value'][0]
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

    network = scale_balance(model=merged_model, ac_schedules=ac_schedules, dc_schedules=dc_schedules, debug=True)
