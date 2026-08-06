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

POLARS CONVERSION NOTES (read before trusting this in production):
    - pypowsybl only speaks pandas at its API boundary (network.get_*() returns pandas.DataFrame,
      network.update_*() expects pandas-shaped/list-like input keyed by element id). Every function
      below converts to polars immediately after a pypowsybl read, and converts back to plain
      lists right before a pypowsybl write. Search for `_pl_from_pypowsybl` to find every boundary.
    - pandas' automatic index-alignment (Series - Series, .reindex(), .loc[...]) has NO polars
      equivalent, since polars has no index concept. Every such spot has been rewritten as an
      explicit join on 'id' (element id) or 'area_key' (country + "-" + connected_component).
      These are the highest-risk rewrites in this file -- validate them against real data.
    - Order-sensitive pandas .loc[] lookups (where row order of the result had to match another
      already-built list before calling network.update_*()) use `.join(..., maintain_order="left")`
      to preserve left-frame row order, since network.update_*() calls are positional (id list must
      line up 1:1 with p0/q0 lists).
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


# CHANGED (#1): new helper -- pypowsybl only returns/accepts pandas, so every network.get_*()
# result gets wrapped into polars here, keeping the pandas index (element id) as a real column
# since polars has no index concept.
def _pl_from_pypowsybl(df: pd.DataFrame, index_name: str = 'id') -> pl.DataFrame:
    """Convert a pandas DataFrame coming out of pypowsybl into polars, keeping the
    pypowsybl element index (element id) as an explicit column so it survives outside pandas."""
    name = df.index.name or index_name
    return pl.from_pandas(df.rename_axis(name).reset_index())


# CHANGED (#3): new helper replacing `_get_series_from_df(df, value_col).groupby(level=0).sum()`.
# The original built a pandas Series indexed by a string "country-component" key; here that key
# becomes an explicit 'area_key' column and the groupby+sum is done directly on it.
def _area_key_frame(df: pl.DataFrame, value_col: str, area_col: str = _country_col) -> pl.DataFrame:
    """Replacement for the old `_get_series_from_df(...).groupby(level=0).sum()` pattern.
    Builds an explicit 'area_key' column (country + "-" + connected_component) and sums
    value_col per area_key -- equivalent to pandas' string-concatenated-index + groupby(level=0).
    Safe to call even when rows are already unique per area_key (e.g. already grouped upstream)."""
    return (
        df.with_columns(
            (pl.col(area_col).cast(pl.Utf8) + "-" + pl.col('connected_component').cast(pl.Utf8)).alias('area_key')
        )
        .group_by('area_key')
        .agg(pl.col(value_col).sum().alias('value'))
        .with_columns(pl.col('value').round(1))
        .sort('area_key')
    )


# CHANGED (#5): new helper replacing the `pd.concat([series, pd.Series({'KEY': ...})]).to_dict()`
# idiom used everywhere to flatten a Series into a report-row dict with extra metadata keys.
def _to_area_dict(frame: pl.DataFrame, key_col: str = 'area_key', val_col: str = 'value') -> dict:
    """Flatten a 2-column polars frame into a plain dict, replacing the old
    `pd.concat([series, pd.Series({'KEY': ...})]).to_dict()` idiom used for report rows."""
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


def get_areas_losses(network: pp.network.Network, buses: pl.DataFrame, components: Dict) -> pl.DataFrame:
    # Calculate ACNP with losses (from cross-border lines)
    boundary_lines = _pl_from_pypowsybl(get_network_elements(network, pp.network.ElementType.BOUNDARY_LINE, all_attributes=True))
    # CHANGED (#2): pandas `.merge(buses.connected_component, left_on='bus_id', right_index=True)`
    # -> explicit join on a real 'id' column (renamed to 'bus_id' to match the join key).
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
    generation = get_areas_metrics(network=network, buses=buses, components=components, metric='GENERATOR')
    consumption = get_areas_metrics(network=network, buses=buses, components=components, metric='LOAD')
    dcnp = _area_key_frame(df=dc_boundary_lines, value_col='boundary_p')

    # CHANGED (#4): pandas did `generation - consumption - dcnp` relying on automatic index
    # alignment (NaN where an area_key is missing on one side), plus an explicit
    # `.reindex(present_areas, fill_value=0)` just for dcnp. Polars has no index alignment, so
    # this is now an explicit `full` join across all four value frames. Only `dcnp` gets
    # `fill_null(0)` (matching pandas' explicit reindex fill); generation / consumption /
    # acnp_with_losses stay null on mismatch, and the subtraction below propagates that null --
    # same NaN-on-mismatch behaviour as the original pandas code.
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


def get_areas_metrics(network: pp.network.Network, buses: pl.DataFrame, components: Dict, metric: str) -> pl.DataFrame:
    df = _pl_from_pypowsybl(get_network_elements(network, getattr(pp.network.ElementType, metric), all_attributes=True))
    # CHANGED (#2): same bus-index merge -> explicit join, as in get_areas_losses above.
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
            # CHANGED (#6): pandas boolean-mask filter `boundary_lines[boundary_lines['connected_component'] == k]`
            # -> polars `.filter(pl.col(...) == k)`.
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

    # CHANGED: pd.concat(_temp) -> pl.concat(_temp, how='diagonal_relaxed') (tolerates any
    # per-subnetwork column/dtype mismatches the same way pandas' concat would).
    return pl.concat(_temp, how='diagonal_relaxed')


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
            logger.warning(f"Fragmented area identified: {area} in components {list(comps)}")
            # CHANGED (#6): boolean-mask filters -> .filter(pl.col(...) == ...).
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


# CHANGED: `.fillna(0)` -> `.fill_nan(0.0)` / `.fill_null(0.0)` (polars distinguishes NaN from
# null, unlike pandas, so both are handled) and `.clip(lo, hi)` stays a direct equivalent.
def _set_power_ratio_to_boundary_lines(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col('boundary_q') / pl.col('boundary_p')).fill_nan(0.0).alias('power_factor')
    ).with_columns(
        pl.col('power_factor').fill_null(0.0).clip(-float(POWER_FACTOR_THRESHOLD), float(POWER_FACTOR_THRESHOLD))
    )


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
    # FIXED: ac_schedules apparently carries missing in_domain/out_domain as the literal string
    # "NaN" (confirmed via debug: the raw table printed "NaN" as text, not `null` -- polars only
    # prints an actual missing value as `null`). `pl.coalesce()` only falls through to the next
    # column on a true null -- a non-null string that happens to say "NaN" gets picked as if it
    # were a real value, silently corrupting registered_resource for every row where in_domain
    # was one of these string sentinels instead of a real null. Explicitly convert "NaN"/"nan"
    # string sentinels (and real nulls) to true null on both columns *before* coalescing, so the
    # fall-through to out_domain actually happens where it should.
    target_acnp_df = target_acnp_df.with_columns([
        pl.when(pl.col('in_domain').is_null() | pl.col('in_domain').is_in(['NaN', 'nan', 'NAN', '']))
        .then(None).otherwise(pl.col('in_domain')).alias('in_domain'),
        pl.when(pl.col('out_domain').is_null() | pl.col('out_domain').is_in(['NaN', 'nan', 'NAN', '']))
        .then(None).otherwise(pl.col('out_domain')).alias('out_domain'),
    ])
    # CHANGED: `.where(cond, other)` on a Series -> `pl.coalesce([...])`, since "take in_domain,
    # else out_domain" is exactly what coalesce does.
    target_acnp_df = target_acnp_df.with_columns(
        pl.coalesce(['in_domain', 'out_domain']).alias('registered_resource')
    )
    # CHANGED: `.dropna(subset=...)` -> `.filter(pl.col(...).is_not_null())`.
    target_acnp_df = target_acnp_df.filter(pl.col('registered_resource').is_not_null())

    target_acnp_df = target_acnp_df.filter(pl.col('registered_resource').is_not_null())

    # FIXED (was #10, then re-fixed): `.unique(subset=..., keep='first', maintain_order=True)` does
    # NOT reliably mean "keep the first row after my sort" -- it preserves output row order, which
    # isn't the same guarantee. This silently kept the wrong (zero-value) row for areas where the
    # real-value row needed the sort to move it to the front (confirmed via debug: ES, PL,
    # DE-TENNET_DE all had a duplicate zero-value row that won over the real one).
    # A `.group_by(...).agg(pl.all().first())` alternative was tried next, but errored on some
    # polars versions ("cannot create expression literal for value of type method"). Using a
    # `.rank().over()` window function instead -- broadly supported and side-steps the group_by/agg
    # issue entirely: rank rows by abs(value) descending within each registered_resource group,
    # then keep only the rank-1 (highest-magnitude) row per group.
    target_acnp_df = (
        target_acnp_df
        .with_columns(pl.col('value').abs().alias('_abs_value'))
        .with_columns(
            pl.col('_abs_value').rank(method='ordinal', descending=True).over('registered_resource').alias('_rank')
        )
        .filter(pl.col('_rank') == 1)
        .drop(['_abs_value', '_rank'])
    )

    # CHANGED (#7): `np.where(mask, a, b)` -> `pl.when(mask).then(a).otherwise(b)`.
    target_acnp_df = target_acnp_df.with_columns(
        pl.when((pl.col('in_domain').is_not_null()) & (pl.col('value') > 0.0))
        .then(pl.col('value') * -1)
        .otherwise(pl.col('value'))
        .alias('value')
    )

    # Validate presence of target AC net position by areas in network model
    # CHANGED: `series[~series.isin(other)].to_list()` -> plain-Python set membership check,
    # since polars Series don't chain as naturally here and this is simple/cheap either way.
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
    # CHANGED (#5): `pd.concat([series.set_index(...).value, pd.Series({'KEY': ...})]).to_dict()`
    # -> plain dict unpacking with `**`.
    _hvdc_results.append({
        **dict(zip(prescale_hvdc_sp['lineEnergyIdentificationCodeEIC'].to_list(), prescale_hvdc_sp['value'].to_list())),
        'KEY': 'prescale-setpoint',
    })
    # CHANGED: `.to_dict('records')` -> `.to_dicts()`.
    for dclink in prescale_hvdc_sp.sort('lineEnergyIdentificationCodeEIC').to_dicts():
        logger.info(f"[INITIAL] PRE-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value'], 2)} MW")
        logger.debug(f"[INITIAL] PRE-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value_q'], 2)} MVar")

    # Mapping HVDC schedules to network
    _cols_to_keep = ['id', 'lineEnergyIdentificationCodeEIC', _country_col, 'ucte_xnode_code', 'power_factor']
    scalable_hvdc = boundary_lines.filter(pl.col('isHvdc') == 'true')
    # Ignore HVDC elements in update of setpoint which are disconnected by network model
    # CHANGED: `.reset_index()` no longer needed -- 'id' is already a real column via _pl_from_pypowsybl.
    scalable_hvdc = scalable_hvdc.filter(pl.col('connected'))[_cols_to_keep]
    # CHANGED: `.merge(...)` -> `.join(..., how='inner')`.
    scalable_hvdc = scalable_hvdc.join(target_hvdc_sp_df, left_on='lineEnergyIdentificationCodeEIC', right_on='registered_resource', how='inner')
    # CHANGED (#6): boolean-mask filter -> `.filter(...)`.
    scalable_hvdc = scalable_hvdc.filter(
        (pl.col(_country_col) == pl.col('in_domain')) | (pl.col(_country_col) == pl.col('out_domain'))
    )
    # CHANGED (#7): `np.where(...)` -> `pl.when(...).then(...).otherwise(...)`.
    scalable_hvdc = scalable_hvdc.with_columns(
        pl.when((pl.col(_country_col) == pl.col('in_domain')) & (pl.col('value') > 0.0))
        .then(pl.col('value') * -1)
        .otherwise(pl.col('value'))
        .alias('value')
    )
    # keep the highest-magnitude row per id
    # FIXED (was #10, then re-fixed): same bug as the AC schedule dedup above -- `.unique(keep=
    # 'first', maintain_order=True)` doesn't reliably respect the preceding sort, and the
    # `.group_by(...).agg(pl.all().first())` alternative errored on some polars versions. Using
    # the same `.rank().over()` window function approach as the AC schedule fix above.
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
        # CHANGED (#2): `boundary_lines.loc[scalable_hvdc_target.index].q0` (pandas .loc reindexes
        # to match the caller's row order exactly) -> explicit join with `maintain_order='left'`
        # so scalable_hvdc_target's row order is preserved -- required because the id/p0/q0 lists
        # passed to update_boundary_lines() below are positional.
        scalable_hvdc_target = scalable_hvdc_target.join(
            boundary_lines.select(['id', 'q0']), on='id', how='left', maintain_order='left'
        ).rename({'q0': 'value_q'})
    network.update_boundary_lines(
        id=scalable_hvdc_target['id'].to_list(),
        p0=scalable_hvdc_target['value'].to_list(),
        q0=scalable_hvdc_target['value_q'].to_list(),
    )
    # CHANGED (#5): same dict-flatten pattern as the prescale block above.
    _hvdc_results.append({
        **dict(zip(scalable_hvdc_target['lineEnergyIdentificationCodeEIC'].to_list(), scalable_hvdc_target['value'].to_list())),
        'KEY': 'postscale-setpoint',
    })
    logger.info(f"[INITIAL] HVDC elements updated to target values: {scalable_hvdc_target['lineEnergyIdentificationCodeEIC'].to_list()}")
    for dclink in scalable_hvdc_target.sort('lineEnergyIdentificationCodeEIC').to_dicts():
        logger.info(f"[INITIAL] POST-SCALE HVDC active power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value'], 2)} MW")
        logger.debug(f"[INITIAL] POST-SCALE HVDC reactive power setpoint of {dclink['lineEnergyIdentificationCodeEIC']}: {round(dclink['value_q'], 2)} MVar")

    # Get AC net positions scaling perimeter -> non-negative ConformLoads
    loads = _pl_from_pypowsybl(get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True))
    # CHANGED (#2): `.merge(network.get_extensions('detail'), right_index=True, left_index=True)`
    # (implicit index-to-index alignment) -> explicit join on the 'id' column both sides now carry.
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
            logger.error(f"Terminating network scaling due to divergence in main island")
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
    # CHANGED (#6): `df[df.connected_component == k].boundary_p.sum()` -> `.filter(...)['boundary_p'].sum()`.
    # Also note: polars `.sum()` on an empty selection returns None, not 0, hence `or 0.0` guards below.
    #changed k into str as polars is not that leniant as pandas was </3
    prescale_network_np = {
        str(k): round(boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'prescale-network-np', 'GLOBAL': prescale_network_np, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] PRE-SCALE NETWORK NP by component: {prescale_network_np}")

    # Get pre-scale total network balance by each component -> AC net position
    # CHANGED (#6): `.query("connected_component == @k")` (pandas @-variable syntax) has no polars
    # equivalent -- the Python variable is just used directly inside `.filter(pl.col(...) == k)`.
    # The boolean mask itself is now a reusable polars expression instead of a precomputed pandas
    # boolean Series, so it can be reapplied to fresh boundary_lines frames later in the function.
    unpaired_boundary_lines_mask = (pl.col('isHvdc') == '') & (pl.col('tie_line_id') == '')
    unpaired_boundary_lines = boundary_lines.filter(unpaired_boundary_lines_mask)
    # k into str for polars
    prescale_network_acnp = {
        str(k): round(unpaired_boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'prescale-network-acnp', 'GLOBAL': prescale_network_acnp, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] PRE-SCALE NETWORK ACNP by component: {prescale_network_acnp}")

    # Identify fragmented IGMs - where some part of network model with boundary belongs other component
    areas_to_components = get_countries_to_components(components=valid_components)
    fragments_participation = get_fragmented_areas_participation(
        unpaired_boundary_lines=boundary_lines.filter(pl.col('isHvdc') == ''),
        areas_to_components=areas_to_components,
    )

    # Map fragmented models to target ACNP schedules and recalculate values by participation
    # FIXED: the previous join()+explode('connected_component') approach was producing wrong
    # per-component assignments (some components getting 0 rows, others absorbing rows that
    # belonged elsewhere) -- most likely because target_acnp_df already carried some column that
    # collided with the join, or because explode() on a freshly-joined List column behaved
    # unexpectedly. Rebuilt to build the (registered_resource, connected_component) pairs
    # directly in Python -- this is a byte-for-byte equivalent of pandas'
    # `series.map(dict_of_sets).explode()`, but skips polars .explode() and any join-name-
    # collision risk entirely, since each row is constructed explicitly rather than fanned out.
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
    # CHANGED: `.merge(fragments_participation, on=[...], how='left')` -> `.join(..., how='left')`.
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
        #changed component_key to str for polars
        target_network_acnp[str(component_key)] = round(scheduled_component_acnp)  # preserve for scaling report
        # CHANGED (#6): `.query("connected_component == @component_key")` -> `.filter(...)`.
        relevant_boundary_lines = unpaired_boundary_lines.filter(pl.col('connected_component') == component_key)
        total_abs_boundary_p = relevant_boundary_lines['boundary_p'].abs().sum() or 0.0
        # CHANGED: `series.abs() / series.abs().sum()` (a pandas Series column assignment) ->
        # `.with_columns(...)` building 'participation' as a new column.
        relevant_boundary_lines = relevant_boundary_lines.with_columns(
            (pl.col('boundary_p').abs() / total_abs_boundary_p).alias('participation')
        )
        #converted to str for polars
        offset_network_acnp = prescale_network_acnp.get(str(component_key)) - scheduled_component_acnp
        relevant_boundary_lines = relevant_boundary_lines.with_columns(
            (pl.col('p0') - offset_network_acnp * pl.col('participation')).alias('prescale_network_acnp_target')
        )
        # CHANGED: `.dropna(inplace=True)` on a Series -> `.filter(pl.col(...).is_not_null())`.
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
        logger.warning(f"Terminating network scaling due to divergence in main island after island ACNP alignment")
        return model

    # Validate total network AC net position alignment
    boundary_lines = _pl_from_pypowsybl(network.get_boundary_lines(all_attributes=True))
    boundary_lines = boundary_lines.join(_elements_to_areas_map.select(['id', _country_col]), on='id', how='left')
    boundary_lines = boundary_lines.join(
        buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
    )
    boundary_lines = boundary_lines.with_columns((pl.col('boundary_p') * -1).alias('boundary_p'))  # invert boundary_p sign to match flow direction
    unpaired_boundary_lines = boundary_lines.filter(unpaired_boundary_lines_mask)
    #k into str
    postscale_network_acnp = {
        str(k): round(unpaired_boundary_lines.filter(pl.col('connected_component') == k)['boundary_p'].sum() or 0.0)
        for k, v in valid_components.items()
    }
    _scaling_results.append({'KEY': 'postscale-network-acnp', 'GLOBAL': postscale_network_acnp, 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] POST-SCALE NETWORK ACNP by component: {postscale_network_acnp}")

    # Get pre-scale generation and consumption
    if debug:
        prescale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR')
        prescale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD')
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
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP: {_to_area_dict(_pre_scale_acnp_frame)}")

    # Filtering target AC net positions series by present regions in network
    combined_scaling_target_df = target_acnp_df.join(
        prescale_acnp, how='inner',
        left_on=['connected_component', 'registered_resource'],
        right_on=['connected_component', _country_col],
        coalesce=False, #keeps CGMES.regionName allegedly
    )
    target_acnp = _area_key_frame(df=combined_scaling_target_df, area_col='registered_resource', value_col='value')
    _scaling_results.append({**_to_area_dict(target_acnp), 'KEY': 'target-acnp', 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] TARGET ACNP: {_to_area_dict(target_acnp)}")

    # Get offsets between target and pre-scale AC net positions for each control area
    combined_scaling_target_df = combined_scaling_target_df.with_columns(
        (pl.col('boundary_p') - pl.col('value')).alias('offset_acnp')
    )
    offset_acnp = _area_key_frame(df=combined_scaling_target_df, area_col='registered_resource', value_col='offset_acnp')
    _scaling_results.append({**_to_area_dict(offset_acnp), 'KEY': 'offset-acnp', 'ITER': _iteration})
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP offset: {_to_area_dict(offset_acnp)}")

    # Perform scaling of AC part schedule of the network model with loop
    logger.info(f"Scaling AC network part")
    while _iteration < int(MAX_ITERATION):
        _iteration += 1

        # Get scaling area loads participation factors
        scalable_loads = _pl_from_pypowsybl(
            get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads['id'].to_list())
        )
        scalable_loads = scalable_loads.join(
            buses.select(['id', 'connected_component']).rename({'id': 'bus_id'}), on='bus_id', how='left'
        )
        # CHANGED (#8): `series / series.groupby([...]).transform('sum')` -> polars' window
        # expression `series.sum().over([...])` -- a direct 1:1 replacement for `.transform()`.
        scalable_loads = scalable_loads.with_columns(
            (pl.col('p0') / pl.col('p0').sum().over([_country_col, 'connected_component'])).alias('p_participation')
        )

        # Join ACNP offsets to scalable loads
        # CHANGED: `.reset_index().merge(..., on=[...]).set_index('id')` -> plain `.join(...)`,
        # since 'id' is already a normal column, no reset/set-index dance needed.
        scalable_loads = scalable_loads.join(
            combined_scaling_target_df.select([_country_col, 'connected_component', 'offset_acnp']),
            how='left', on=[_country_col, 'connected_component'],
        )

        # Scale loads by participation factor
        # CHANGED: `series.offset_acnp * series.p_participation` / `series.p0 + diff` (pandas
        # Series arithmetic) -> `.with_columns(...)` building new columns on the same frame.
        scalable_loads = scalable_loads.with_columns(
            (pl.col('offset_acnp') * pl.col('p_participation')).alias('scalable_loads_diff')
        ).with_columns(
            (pl.col('p0') + pl.col('scalable_loads_diff')).alias('scalable_loads_target')
        )
        ## Removing loads which target value is NaN. It can be because missing target ACNP for this area
        # CHANGED: `.dropna(inplace=True)` -> `.filter(pl.col(...).is_not_null())`.
        scalable_loads_target = scalable_loads.filter(pl.col('scalable_loads_target').is_not_null() & pl.col('scalable_loads_target').is_not_nan())
        # CHANGED (#2): `conform_loads.merge(scalable_loads_target.reset_index()[['id']], left_index=True, right_on='id').set_index('id')`
        # -> explicit join with `maintain_order='right'` so conform_loads_na ends up in the same
        # row order as scalable_loads_target -- required since the q0 list built below (line just
        # after this) is zipped positionally against scalable_loads_target's own id/p0 lists when
        # passed into network.update_loads().
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
            postscale_generation = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='GENERATOR')
            postscale_consumption = get_areas_metrics(network=network, buses=buses, components=valid_components, metric='LOAD')
            _scaling_results.append({**_to_area_dict(postscale_generation), 'KEY': 'generation', 'ITER': _iteration})
            _scaling_results.append({**_to_area_dict(postscale_consumption), 'KEY': 'consumption', 'ITER': _iteration})

            # Get post-scale network losses by regions
            ## It is needed to estimate when loadflow engine balances entire network schedule with distributed slack enabled
            postscale_losses = get_areas_losses(network=network, buses=buses, components=valid_components)
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
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP: {_to_area_dict(_post_scale_acnp_frame)}")

        # Get post-scale total network balance
        prescale_total_np = boundary_lines.filter(pl.col('paired') == False)['boundary_p'].sum() or 0.0
        logger.info(f"[ITER {_iteration}] POST-SCALE TOTAL NP: {round(prescale_total_np, 2)}")

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
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP offsets: {_to_area_dict(offset_acnp)}")

        # Breaking scaling loop if target ac net position for all areas is reached
        if all(abs(v) <= int(BALANCE_THRESHOLD) for v in offset_acnp['value'].to_list()):
            logger.info(f"[ITER {_iteration}] Scaling successful as ACNP offsets less than threshold: {int(BALANCE_THRESHOLD)} MW")
            break
    else:
        logger.warning(f"Max iteration limit reached")
        # TODO actions after scale break

    # Post-processing scaling results dataframe
    # CHANGED: `pd.DataFrame(_scaling_results).set_index('ITER').sort_index().round(2)` -- polars
    # has no index, so 'ITER' stays a normal column and sorting is `.sort('ITER')`. `.round(2)` in
    # pandas rounds every numeric column at once; polars requires picking float columns explicitly
    # (the list comprehension below), since `.round()` isn't frame-wide.
    ac_scaling_results_df = pl.DataFrame(_scaling_results, infer_schema_length=None).sort('ITER')
    ac_scaling_results_df = ac_scaling_results_df.with_columns(
        [pl.col(c).round(2) for c, dt in zip(ac_scaling_results_df.columns, ac_scaling_results_df.dtypes) if dt in (pl.Float64, pl.Float32)]
    )
    hvdc_results_df = pl.DataFrame(_hvdc_results, infer_schema_length=None)
    hvdc_results_df = hvdc_results_df.with_columns(
        [pl.col(c).round(2) for c, dt in zip(hvdc_results_df.columns, hvdc_results_df.dtypes) if dt in (pl.Float64, pl.Float32)]
    )

    # Attach the raw per-iteration tables to the model so callers (including code that imports
    # and calls scale_balance() directly, not just the __main__ test harness below) can access
    # and save them -- these were only local variables before, so nothing outside this function
    # could reach them without this.
    model.ac_scaling_results_df = ac_scaling_results_df
    model.hvdc_results_df = hvdc_results_df

    # Process data for merge report
    # CHANGED (#6): `.query("KEY in [...]")` -> `.filter(pl.col('KEY').is_in([...]))`.
    filtered_df = ac_scaling_results_df.filter(pl.col('KEY').is_in(['prescale-acnp', 'postscale-acnp', 'offset-acnp']))
    # CHANGED: `.loc[[0, filtered_df.index.max()]]` (select first + last row by index label) ->
    # since polars has no index, a temporary row-number column stands in for the old 'ITER' index
    # values 0 and its max, then gets dropped again.
    filtered_df = filtered_df.with_row_index('_row_idx')
    filtered_df = filtered_df.filter((pl.col('_row_idx') == 0) | (pl.col('_row_idx') == filtered_df['_row_idx'].max()))
    is_first_row = pl.col('_row_idx') == 0
    # CHANGED: two chained `.loc[boolean_mask, 'KEY'] = ...` assignments -> a single
    # `pl.when().then().when().then().otherwise()` chain (polars has no in-place label-based assignment).
    filtered_df = filtered_df.with_columns(
        pl.when(is_first_row & (pl.col('KEY') == 'offset-acnp')).then(pl.lit('initial-offset-acnp'))
        .when((~is_first_row) & (pl.col('KEY') == 'offset-acnp')).then(pl.lit('final-offset-acnp'))
        .otherwise(pl.col('KEY')).alias('KEY')
    )

    # CHANGED: `.drop(columns='GLOBAL')` -> `.drop([...])` (also drops the temp row-index column).
    filtered_df = filtered_df.drop(['_row_idx', 'GLOBAL'])
    # CHANGED: `.dropna(axis=1)` (drop any column containing a null) has no polars equivalent --
    # built manually via null_count() per column.
    non_null_cols = [c for c in filtered_df.columns if filtered_df[c].null_count() < filtered_df.height]
    filtered_df = filtered_df.select(non_null_cols)
    # CHANGED: `.str.replace('-', '_')` (pandas Series.str) -> polars `.str.replace_all(...)`
    # (note: pandas `.replace()` without `regex=True`/count already replaces all occurrences here,
    # so `replace_all` is the correct match, not `replace` which only replaces the first hit in polars).
    filtered_df = filtered_df.with_columns(pl.col('KEY').str.replace_all('-', '_'))
    # CHANGED (#9): `.melt(id_vars=['KEY'], var_name='area', value_name='value')` -> `.unpivot(...)`
    # (polars renamed melt to unpivot); `.pivot(index='area', columns='KEY', values='value')` ->
    # `.pivot(index='area', on='KEY', values='value')` (polars renamed columns= to on=).
    ac_melted_df = filtered_df.unpivot(index=['KEY'], variable_name='area', value_name='value')
    ac_pivoted_df = ac_melted_df.pivot(index='area', on='KEY', values='value')
    ac_pivoted_df = ac_pivoted_df.with_columns(
        (pl.col('final_offset_acnp').abs() <= int(BALANCE_THRESHOLD)).alias('success')
    )
    # CHANGED: `.astype(object).where(pd.notna(df), None).to_dict('records')` (pandas' NaN->None
    # dance for JSON-friendly output) -> plain `.to_dicts()`, since polars nulls already serialize
    # as Python None with no extra conversion needed.
    ac_scale_report_dict = ac_pivoted_df.to_dicts()

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
    # CHANGED (#11): dead/unused function, converted for consistency only.
    # Boolean-mask filters -> `.filter(...)`; `.squeeze()` on a 1-row/1-col frame -> `[0]` indexing.
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
