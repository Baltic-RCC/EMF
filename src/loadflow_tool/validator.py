import pypowsybl
from helper import package_for_pypowsybl
import logging

logger = logging.getLogger(__name__)


# https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
igm_validation_parameters = pypowsybl.loadflow.Parameters(voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.PREVIOUS_VALUES,
                                                          transformer_voltage_control_on=True,
                                                          no_generator_reactive_limits=False,
                                                          phase_shifter_regulation_on=True,
                                                          #twt_split_shunt_admittance=None,
                                                          simul_shunt=True,
                                                          read_slack_bus=True,
                                                          write_slack_bus=False,
                                                          distributed_slack=False,
                                                          balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_P_MAX,
                                                          dc_use_transformer_ratio=None,
                                                          countries_to_balance=None,
                                                          connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN, # ALL / MAIN
                                                          #provider_parameters=None
                                                          )


def validate_model(model, latest_boundary):

    instance_files = model['opdm:OPDMObject']['opde:Component'] + latest_boundary['opdm:OPDMObject']['opde:Component']

    global_zip_filename = package_for_pypowsybl(instance_files)

    # TODO - get powsybl to use BytesIO object instead of filesystem paths for import
    network = pypowsybl.network.load(global_zip_filename)
    print(network)

    # TODO - use PF Parameters as defined in EMF requirements
    result = pypowsybl.loadflow.run_ac(network)
    print(result)

    # TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
    # note - multiple islands wo load or generation can be an issue

    return {"network": network, "powerflow": result}
