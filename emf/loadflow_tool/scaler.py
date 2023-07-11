import pypowsybl as pp
import logging
import pandas as pd
import numpy as np
from settings import PROVIDER_PF_PARAMETERS

logger = logging.getLogger(__name__)


def series_scaler(series: pd.Series) -> pd.Series:
    scaled_series = (series - np.mean(series))
    return scaled_series


def scale_balance(network: object, target_acnp: dict):

    """
    Need to have:
        1. Merged countries. EIC code is not available in pp, only country identification like 'LT'

    What apparoach to use:
        1. Firstly get available areas in CGM from substations lists and then get loads for each area
        2. Get all loads in CGM and group by country (whats if some ares does not have loads - but I think is not the case)


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


    :param network:
    :return:
    """
    MAX_ITERATION = 10
    BALANCE_THRESHOLD = 2

    target_acnp = pd.Series(target_acnp)
    logger.info(f"Target AC NP: {target_acnp.to_dict()}")

    # Get voltage levels and substations for area definition
    _voltage_levels = network.get_voltage_levels().rename(columns={"name": "voltage_level_name"})
    _substations = network.get_substations().rename(columns={"name": "substation_name"})

    # STEP 1. Solve loadflow
    pf_parameters = pp.loadflow.Parameters(voltage_init_mode=pp.loadflow.VoltageInitMode.UNIFORM_VALUES,
                                           transformer_voltage_control_on=None,
                                           no_generator_reactive_limits=False,
                                           phase_shifter_regulation_on=None,
                                           twt_split_shunt_admittance=None,
                                           simul_shunt=False,
                                           read_slack_bus=True,
                                           write_slack_bus=None,
                                           distributed_slack=True,
                                           balance_type=None,
                                           dc_use_transformer_ratio=None,
                                           countries_to_balance=None,
                                           connected_component_mode=None,
                                           provider_parameters=PROVIDER_PF_PARAMETERS,
                                           )
    pf_result = pp.loadflow.run_ac(network=network, parameters=pf_parameters)
    for island_res in pf_result:
        logger.info(f"Initial load flow status -> {island_res.status.name}")
        logger.info(f"Initial load flow results -> ITERATION: {island_res.iteration_count}, MISMATCH: {round(island_res.slack_bus_active_power_mismatch, 2)}, SLACK_BUS: {island_res.slack_bus_id}")

    iteration = 0
    while iteration < MAX_ITERATION:
        iteration += 1
        logger.info(f"Balance scale iteration: {iteration}")
        # STEP X. Get current AC NP
        # tie_lines = network.get_tie_lines()
        dangling_lines = network.get_dangling_lines()
        dangling_lines = dangling_lines.merge(_voltage_levels, left_on='voltage_level_id', right_index=True, suffixes=(None, '_voltage_level'))
        dangling_lines = dangling_lines.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))

        # TODO need to eliminate HVDC
        current_acnp = dangling_lines.groupby('country').p.sum()
        logger.info(f"Current AC NP: {current_acnp.to_dict()}")

        # for area_id, area_data in dangling_lines.groupby('country'):  # You can apply sum directly in groupby
        #     ac_np = area_data.query("connected == True").p.sum()  # TODO this return NP, need to eliminate HVDCs
        #     logger.info(f"AC NET position of {area_id}: {ac_np}")

        # STEP X. Calculate offset between target and current AC NP
        offset_acnp = current_acnp - target_acnp
        logger.info(f"Balance offset: {offset_acnp.to_dict()}")

        # STEP X. Get scalable area
        # TODO have to maintain power factor
        # TODO Check whether to use p or p0 values
        loads = network.get_loads(all_attributes=True)
        loads = loads.merge(_voltage_levels, left_on='voltage_level_id', right_index=True, suffixes=(None, '_voltage_level'))
        loads = loads.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))
        loads['p_participation'] = loads.p0 / loads.groupby('country').p0.transform('sum')

        # TODO Another option to get area loads is as follows
        # loads = network.get_loads(id=network.get_elements_ids(element_type=pp.network.ElementType.LOAD, countries=['LT']))
        # loads['p_percent'] = loads.p0 / loads.p0.sum()

        # STEP X. Scale loads
        # TODO check what to do with negative loads, maybe to remove from scaling (case for Litgrid)
        target_loads = loads.p0 + (loads.country.map(offset_acnp) * loads.p_participation)
        target_loads.dropna(inplace=True)  # removing loads which target calue is NaN. It can be because missing target AC NP for this area
        network.update_loads(id=target_loads.index, p0=target_loads.to_list())

        # STEP X. Solve load flow and check AC net again
        pf_result = pp.loadflow.run_ac(network=network, parameters=pf_parameters)
        for island_res in pf_result:
            logger.info(f"Load flow status -> {island_res.status.name}")
            logger.info(f"Load flow results -> ITERATION: {island_res.iteration_count}, MISMATCH: {round(island_res.slack_bus_active_power_mismatch, 2)}, SLACK_BUS: {island_res.slack_bus_id}")


        dangling_lines = network.get_dangling_lines()
        dangling_lines = dangling_lines.merge(_voltage_levels, left_on='voltage_level_id', right_index=True, suffixes=(None, '_voltage_level'))
        dangling_lines = dangling_lines.merge(_substations, left_on='substation_id', right_index=True, suffixes=(None, '_substation'))

        # TODO need to eliminate HVDC
        current_acnp = dangling_lines.groupby('country').p.sum()
        logger.info(f"New AC NP: {current_acnp.to_dict()}")

        # Breaking loop if target for all areas is reached
        offset_acnp = current_acnp - target_acnp
        offset_acnp.dropna(inplace=True)
        if all(abs(offset_acnp.values) < BALANCE_THRESHOLD):
            logger.info("Breaking loop as all balance offsets less than threshold")
            logger.info(f"Final balances offsets: {offset_acnp.to_dict()}")
            break
    else:
        logger.warning(f"Max iteration limit reached")
        # TODO actions after scale break


    logger.info("END")







if __name__ == "__main__":
    # Testing
    import sys
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    model_path = r"input\4b816231-bf06-4cbe-bba1-bb6fa7280af1.zip"
    network = pp.network.load(model_path)

    target_acnp = {"LT": -600, "LV": 200}

    scale_balance(network=network, target_acnp=target_acnp)