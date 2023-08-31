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
• When all the differences between the recorded and target values of net positions of
scheduling areas are below the discrepancy thresholds, as defined previously;
• In any case after the 15th iteration16 (adjustments take place within the iterations).
"""
import pypowsybl as pp
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Union
import config
from emf.common.config_parser import parse_app_properties
from emf.common.decorators import performance_counter
from emf.common.integrations import elastic
from emf.loadflow_tool.helper import attr_to_dict, get_network_elements, get_slack_generators, get_connected_component_counts
from emf.loadflow_tool.loadflow_settings import CGM_DEFAULT, CGM_RELAXED_1, CGM_RELAXED_2

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.scaler)


def query_hvdc_schedules(process_type: str,
                         utc_start: str,
                         utc_end: str,
                         area_eic_map: Dict[str, str] | None = None) -> dict | None:
    """
    Method to get HVDC schedules (business type - B63)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param utc_start: start time in utc. Example: '2023-08-08T23:00:00'
    :param utc_end: end time in utc. Example: '2023-08-09T00:00:00'
    :param area_eic_map: dictionary of geographical region names and control area eic code
    :return: schedules in dict format
    """
    # Define area name to eic mapping table
    if not area_eic_map:
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())

    # Define metadata dictionary
    metadata = {
        "process.processType": process_type,
        "TimeSeries.businessType": "B63",
    }

    # Get HVDC schedules
    service = elastic.Elastic()
    schedules_df = service.query_schedules_from_elk(
        index=ELK_INDEX_PATTERN,
        utc_start=utc_start,
        utc_end=utc_end,
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # TODO filter out data by reason code that take only verified tada

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.connectingLine_RegisteredResource.mRID"]
    schedules_df = schedules_df[_cols]
    schedules_df.rename(columns={"TimeSeries.connectingLine_RegisteredResource.mRID": "registered_resource"}, inplace=True)
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


def query_acnp_schedules(process_type: str,
                         utc_start: str,
                         utc_end: str,
                         area_eic_map: Dict[str, str] | None = None) -> dict | None:
    """
    Method to get ACNP schedules (business type - B64)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param utc_start: start time in utc. Example: '2023-08-08T23:00:00'
    :param utc_end: end time in utc. Example: '2023-08-09T00:00:00'
    :return:
    """
    # Define area name to eic mapping table
    if not area_eic_map:
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())

    metadata = {
        "process.processType": process_type,
        "TimeSeries.businessType": "B64",
    }

    # Get AC area schedules
    service = elastic.Elastic()
    schedules_df = service.query_schedules_from_elk(
        index=ELK_INDEX_PATTERN,
        utc_start=utc_start,
        utc_end=utc_end,
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain"]
    schedules_df = schedules_df[_cols]
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


# TODO arguments validation with pydantic
@performance_counter(units='seconds')
def scale_balance(network: pp.network.Network,
                  ac_schedules: List[Dict[str, Union[str, float, None]]],
                  dc_schedules: List[Dict[str, Union[str, float, None]]],
                  lf_settings: pp.loadflow.Parameters = CGM_RELAXED_1,
                  debug=False
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
    _island_bus_count = get_connected_component_counts(network=network, bus_count_threshold=5)
    _scaling_results = []
    _iteration = 0

    # Defining logging level
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Target HVDC setpoints
    target_hvdc_sp_df = pd.DataFrame(dc_schedules)
    # logger.info(f"[INITIAL] Target DC NP: {target_dcnp.to_dict()}")

    # Target AC net position
    target_acnp_df = pd.DataFrame(ac_schedules)
    target_acnp_df['registered_resource'] = target_acnp_df['in_domain'].where(target_acnp_df['in_domain'].notna(), target_acnp_df['out_domain'])
    target_acnp_df = target_acnp_df.dropna(subset='registered_resource')
    target_acnp_df = target_acnp_df.sort_values('value', key=abs, ascending=False).drop_duplicates(subset='registered_resource')
    mask = (target_acnp_df['in_domain'].notna()) & (target_acnp_df['value'] > 0.0)  # in_domain not None and value is not zero
    target_acnp_df['value'] = np.where(mask, target_acnp_df['value'] * -1, target_acnp_df['value'])
    target_acnp = target_acnp_df.set_index('registered_resource')['value']
    # logger.info(f"[INITIAL] Target AC NP: {target_acnp.to_dict()}")

    # Get pre-scale HVDC setpoints
    dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    prescale_hvdc_sp = dangling_lines[dangling_lines.isHvdc == 'true'][['ucte-x-node-code', 'p']]
    for dclink in prescale_hvdc_sp.to_dict('records'):
        logger.info(f"[INITIAL] PRE-SCALE HVDC setpoint of {dclink['ucte-x-node-code']}: {round(dclink['p'], 2)} MW")

    # Mapping HVDC schedules to network
    scalable_hvdc = dangling_lines[dangling_lines.isHvdc == 'true'][['lineEnergyIdentificationCodeEIC', 'CGMES.regionName', 'ucte-x-node-code']]
    scalable_hvdc.reset_index(inplace=True)
    scalable_hvdc = scalable_hvdc.merge(target_hvdc_sp_df, left_on='lineEnergyIdentificationCodeEIC', right_on='registered_resource')
    mask = (scalable_hvdc['CGMES.regionName'] == scalable_hvdc['in_domain']) | (scalable_hvdc['CGMES.regionName'] == scalable_hvdc['out_domain'])
    scalable_hvdc = scalable_hvdc[mask]
    mask = (scalable_hvdc['CGMES.regionName'] == scalable_hvdc['in_domain']) & (scalable_hvdc['value'] > 0.0)
    scalable_hvdc['value'] = np.where(mask, scalable_hvdc['value'] * -1, scalable_hvdc['value'])
    scalable_hvdc = scalable_hvdc.set_index('id')

    # Scaling HVDC network elements
    scalable_hvdc_target = scalable_hvdc[['value', 'ucte-x-node-code']]
    network.update_dangling_lines(id=scalable_hvdc_target.index, p0=scalable_hvdc_target.value)
    logger.info(f"[INITIAL] HVDC elements updated to target values: {scalable_hvdc_target['ucte-x-node-code'].values}")
    for dclink in scalable_hvdc_target.to_dict('records'):
        logger.info(f"[INITIAL] POST-SCALE HVDC setpoint of {dclink['ucte-x-node-code']}: {round(dclink['value'], 2)} MW")

    # Get AC scaling area -> non-negative ConformLoads
    loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True)
    loads = loads.merge(network.get_extensions('detail'), right_index=True, left_index=True)
    conform_loads = loads[loads['variable_p0'] > 0]

    # Get network slack generators
    slack_generators = get_slack_generators(network)
    logger.info(f"[INITIAL] Network slack generators: {slack_generators.name.to_list()}")

    # Solving pre-scale loadflow
    # TODO exit scaling if pre-scale LF diverged
    pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
    for result in [x for x in pf_results if x.connected_component_num in _island_bus_count.keys()]:
        result_dict = attr_to_dict(result)
        logger.info(f"[INITIAL] Loadflow status: {result_dict.get('status').name}")
        logger.debug(f"[INITIAL] Loadflow results: {result_dict}")

    # Get pre-scale AC net position
    dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
    prescale_acnp = dangling_lines[dangling_lines.isHvdc == ''].groupby('CGMES.regionName').p.sum()
    _scaling_results.append(pd.concat([prescale_acnp, pd.Series({'STEP': 'prescale-acnp', 'ITER': f"iter-{_iteration}"})]).to_dict())
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP: {prescale_acnp.to_dict()}")

    # Filtering target AC net position series by present regions in network
    target_acnp = target_acnp[target_acnp.index.isin(prescale_acnp.index)]
    _scaling_results.append(pd.concat([target_acnp, pd.Series({'STEP': 'target-acnp', 'ITER': f"iter-{_iteration}"})]).to_dict())
    logger.info(f"[ITER {_iteration}] TARGET ACNP: {target_acnp.to_dict()}")

    # Get offset between target and pre-scale AC net position
    offset_acnp = prescale_acnp - target_acnp[target_acnp.index.isin(prescale_acnp.index)]
    offset_acnp.dropna(inplace=True)
    _scaling_results.append(pd.concat([offset_acnp, pd.Series({'STEP': 'offset-acnp', 'ITER': f"iter-{_iteration}"})]).to_dict())
    logger.info(f"[ITER {_iteration}] PRE-SCALE ACNP offset: {offset_acnp.to_dict()}")

    # Perform scaling of AC part of the network with loop
    while _iteration < int(MAX_ITERATION):
        _iteration += 1

        # Get scaling area loads participation factors
        # TODO have to maintain power factor
        scalable_loads = get_network_elements(network, pp.network.ElementType.LOAD, all_attributes=True, id=conform_loads.index)
        scalable_loads['p_participation'] = scalable_loads.p0 / scalable_loads.groupby('CGMES.regionName').p0.transform('sum')

        # Scale loads by participation factor
        # TODO Parallel processing with multiple scenarios +10%/+20% and etc
        correction_factor = (100 + int(SCALING_CORR_FACTOR)) / 100
        scalable_loads_diff = (scalable_loads['CGMES.regionName'].map(offset_acnp) * scalable_loads.p_participation) * correction_factor
        scalable_loads_target = scalable_loads.p0 + scalable_loads_diff
        scalable_loads_target.dropna(inplace=True)  # removing loads which target value is NaN. It can be because missing target ACNP for this area
        network.update_loads(id=scalable_loads_target.index, p0=scalable_loads_target.to_list())

        # Solving post-scale loadflow
        pf_results = pp.loadflow.run_ac(network=network, parameters=lf_settings)
        for result in [x for x in pf_results if x.connected_component_num in _island_bus_count.keys()]:
            result_dict = attr_to_dict(result)
            logger.info(f"[ITER {_iteration}] Loadflow status: {result_dict.get('status').name}")
            logger.debug(f"[ITER {_iteration}] Loadflow results: {result_dict}")

        # Get post-scale AC net position
        dangling_lines = get_network_elements(network, pp.network.ElementType.DANGLING_LINE, all_attributes=True)
        postscale_acnp = dangling_lines[dangling_lines.isHvdc == ''].groupby('CGMES.regionName').p.sum()
        _scaling_results.append(pd.concat([postscale_acnp, pd.Series({'STEP': 'postscale-acnp', 'ITER': f"iter-{_iteration}"})]).to_dict())
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP: {postscale_acnp.to_dict()}")

        # Get offset between target and post-scale AC net position
        offset_acnp = postscale_acnp - target_acnp[target_acnp.index.isin(postscale_acnp.index)]
        offset_acnp.dropna(inplace=True)
        _scaling_results.append(pd.concat([offset_acnp, pd.Series({'STEP': 'offset-acnp', 'ITER': f"iter-{_iteration}"})]).to_dict())
        logger.info(f"[ITER {_iteration}] POST-SCALE ACNP offsets: {offset_acnp.to_dict()}")

        # Breaking scaling loop if target ac net position for all areas is reached
        if all(abs(offset_acnp.values) <= int(BALANCE_THRESHOLD)):
            logger.info(f"[ITER {_iteration}] Scaling successful as ACNP offsets less than threshold: {int(BALANCE_THRESHOLD)} MW")
            break
    else:
        logger.warning(f"Max iteration limit reached")
        # TODO actions after scale break

    network.ac_scaling_results_df = pd.DataFrame(_scaling_results)

    return network


def hvdc_schedule_mapper(row):
    """BACKLOG FUNCTION. CURRENTLY NOT USED"""
    schedules = pd.DataFrame(target_dcnp)
    eic_mask = schedules['TimeSeries.connectingLine_RegisteredResource.mRID'] == row['lineEnergyIdentificationCodeEIC']
    in_domain_mask = schedules["TimeSeries.in_Domain.regionName"] == row['CGMES.regionName']
    out_domain_mask = schedules["TimeSeries.out_Domain.regionName"] == row['CGMES.regionName']
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
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    model_path = r"input\4b816231-bf06-4cbe-bba1-bb6fa7280af1.zip"
    network = pp.network.load(model_path)

    # Query target schedules
    # ac_schedules = query_acnp_schedules(process_type="A01", utc_start="2023-08-24T07:00:00", utc_end="2023-08-24T08:00:00")
    dc_schedules = query_hvdc_schedules(process_type="A18", utc_start="2023-08-30T04:00:00", utc_end="2023-08-30T06:00:00")

    # ac_schedules.append({"value": 400, "in_domain": "LT", "out_domain": None})
    ac_schedules = [{"value": 400, "in_domain": "LT", "out_domain": None}]

    network = scale_balance(network=network, ac_schedules=ac_schedules, dc_schedules=dc_schedules, debug=True)
    print(network.ac_scaling_results_df)

    # Other examples
    # loads = network.get_loads(id=network.get_elements_ids(element_type=pp.network.ElementType.LOAD, countries=['LT']))