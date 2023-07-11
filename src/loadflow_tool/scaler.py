import pypowsybl as pp
import logging

logger = logging.getLogger(__name__)


def scale(network):

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

    # STEP 1. Solve loadflow
    network.run_ac()

    # STEP 2. Get current AC NET
    # tie_lines = network.get_tie_lines()

    dangling_lines = network.get_dangling_lines()
    dangling_lines = dangling_lines.merge(voltage_levels, left_on='voltage_level_id', right_index=True).merge(substations, left_on='substation_id', right_index=True)

    # Iterate over each area
    # df['%'] = 100 * df['Fee'] / df.groupby('country')['p'].transform('sum')

    for area_id, area_data in dangling_lines.groupby('country'):  # You can apply sum directly in groupby
        ac_np = area_data.query("connected == True").p.sum()  # TODO this return NP, need to eliminate HVDCs
        logger.info(f"AC NET position of {area_id}: {ac_np}")


    # STEP 3. Calculate ammount to scale
    target_acnp = 1500
    amount_to_scale = ac_np - target_acnp


    # STEP 4. scale loads  #TODO have to maintain power factor
    loads = network.get_loads(all_attributes=True)
    voltage_levels = network.get_voltage_levels()
    substations = network.get_substations()

    result = loads.merge(voltage_levels, left_on='voltage_level_id', right_index=True).merge(substations, left_on='substation_id', right_index=True)

    # another option to get area loads is as follows
    area_loads = network.get_loads(id=network.get_elements_ids(element_type=pp.network.ElementType.LOAD, countries=['LT']))
    area_loads['participation_factor'] = area_loads.p0 / area_loads.p0.sum()

    new_load = area_loads.p0 + (amount_to_scale * area_loads.participation_factor)

    network.update_loads(id=area_loads.index, p0=new_load.to_list())


    # STEP 5. Solve load flow and check AC net again










if __name__ == "__main__":

    import sys
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

