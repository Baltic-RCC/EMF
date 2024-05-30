import pypowsybl
from helper import package_for_pypowsybl
import logging
import sys
import settings

logger = logging.getLogger(__name__)


class ModelValidator:

    # https://pypowsybl.readthedocs.io/en/stable/reference/loadflow/parameters.html#pypowsybl.loadflow.Parameters
    lf_parameters = pypowsybl.loadflow.Parameters(
        voltage_init_mode=pypowsybl._pypowsybl.VoltageInitMode.PREVIOUS_VALUES,
        transformer_voltage_control_on=True,
        no_generator_reactive_limits=False,
        phase_shifter_regulation_on=True,
        twt_split_shunt_admittance=None,
        simul_shunt=True,
        read_slack_bus=True,
        write_slack_bus=False,
        distributed_slack=False,
        balance_type=pypowsybl._pypowsybl.BalanceType.PROPORTIONAL_TO_GENERATION_P_MAX,
        dc_use_transformer_ratio=None,
        countries_to_balance=None,
        connected_component_mode=pypowsybl._pypowsybl.ConnectedComponentMode.MAIN,  # ALL / MAIN
        provider_parameters=settings.PROVIDER_PF_PARAMETERS,
        )

    def __init__(self, debug):

        self.debug = debug

        # Debug mode
        if self.debug:
            logger.info(f"Application running in DEBUG mode")
            logger.setLevel(logging.DEBUG)
            # logging.getLogger('powsybl').setLevel(logging.INFO)
            # logging.getLogger('powsybl').setLevel(1)

    def get_models_from_opde(self, time_horizon, scenario_date):

        service = OPDM(server=settings.OPDM_SERVER, username=settings.OPDM_USERNAME, password=settings.OPDM_PASSWORD)

        bds_opdm_object = service.get_latest_boundary()

        models_container = []
        for tso in settings.INCLUDED_TSOS:
            model_opdm_object_list = service.get_latest_models_and_download(tso=tso,
                                                                            time_horizon=time_horizon,
                                                                            scenario_date=scenario_date)

            if not model_opdm_object_list:  # skipping if there is no model in OPDE
                logger.info(f"No model found on OPDE")
                continue
            else:
                models_container.append(*model_opdm_object_list)

        # Adding the latest bds to have full model
        models_container.append(bds_opdm_object)

        return models_container


    def validate_model(self, model, latest_boundary):

        instance_files = model['opdm:OPDMObject']['opde:Component'] + latest_boundary['opdm:OPDMObject']['opde:Component']

        global_zip_filename = package_for_pypowsybl(instance_files)

        # TODO - get powsybl to use BytesIO object instead of filesystem paths for import
        logger.info(f"Loading model from -> {global_zip_filename}")
        self.network = pypowsybl.network.load(global_zip_filename)
        logger.info(f"{self.network}")

        # TODO - use PF Parameters as defined in EMF requirements
        self.lf_result = pypowsybl.loadflow.run_ac(network=self.network, parameters=self.lf_parameters)
        for island_res in self.lf_result:
            logger.info(f"Load flow status -> {island_res.status.name}")
            logger.info(f"Load flow results -> ITERATION: {island_res.iteration_count}, MISMATCH: {round(island_res.slack_bus_active_power_mismatch, 2)}, SLACK_BUS: {island_res.slack_bus_id}")

        # TODO - record AC NP and DC Flows to metadata storage (and more), this is useful for replacement logic and scaling
        # note - multiple islands wo load or generation can be an issue

        return {"network": self.network, "loadflow": self.lf_result}
