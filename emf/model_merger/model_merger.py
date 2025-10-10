import logging
import pypowsybl
import config
import json
import time
from uuid import uuid4
import datetime
from dataclasses import dataclass, field
from typing import List
from emf.common.helpers.time import parse_datetime
from io import BytesIO
from zipfile import ZipFile
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import opdm, minio_api, elastic, edx
from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
from emf.common.integrations.object_storage.schedules import query_acnp_schedules, query_hvdc_schedules, calculate_ac_net_position
from emf.common.loadflow_tool import loadflow_settings, settings_manager
from emf.common.helpers.utils import attr_to_dict, convert_dict_str_to_bool
from emf.common.helpers.cgmes import export_to_cgmes_zip
from emf.common.helpers.opdm_objects import get_opdm_component_data_bytes
from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.tasks import update_task_status
from emf.model_merger import merge_functions
from emf.model_merger import scaler
from emf.model_merger.merge_functions import filter_models_by_acnp
from emf.model_merger.replacement import run_replacement, get_tsos_available_in_storage
from emf.model_merger.temporary import handle_igm_ssh_vs_cgm_ssh_error
from emf.common.logging.custom_logger import get_elk_logging_handler
from concurrent.futures import ThreadPoolExecutor
from lxml import etree

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.merger)
executor = ThreadPoolExecutor(max_workers=20)


def async_call(function, callback=None, *args, **kwargs):
    future = executor.submit(function, *args, **kwargs)
    if callback:
        future.add_done_callback(lambda f: callback(f.result()))
    return future


def log_opdm_response(response):
    logger.debug(etree.tostring(response, pretty_print=True).decode())


@dataclass
class MergedModel:
    network: pypowsybl.network = None
    time_horizon: str = None
    time_horizon_id: str =field(default_factory=str)
    name = None
    loadflow_status: str | None = None

    # Status flags
    scaled: bool = None
    replaced: bool = None
    outages: bool = None
    uploaded_to_opde: bool = False
    uploaded_to_minio: bool = False

    # Extended data
    loadflow: List = field(default_factory=list)
    excluded: List = field(default_factory=list)
    scaled_entity: List = field(default_factory=list)
    scaled_hvdc: List = field(default_factory=list)
    replaced_entity: List = field(default_factory=list)
    replacement_reason: List = field(default_factory=list)
    outages_updated: List = field(default_factory=list)
    outages_unmapped: List = field(default_factory=list)
    merge_included_entity: List = field(default_factory=list)


@dataclass(init=False)
class ModelEntity:
    data_source: str = "OPDM"
    quality_indicator: str = "Valid"
    tso: str = None
    time_horizon: str = None
    scenario_timestamp: str = None
    model_sv_id: str = None
    version: int = None
    quality_indicator: str = "Valid"
    creation_timestamp: str = None
    file_name: str = None

    def __init__(self, data_source: str, quality_indicator: str, **kwargs):
        self.data_source = data_source
        self.quality_indicator = quality_indicator
        self.tso = kwargs.get('pmd:TSO', 'unknown')
        self.time_horizon = kwargs.get('pmd:timeHorizon', 'unknown')
        self.scenario_timestamp = kwargs.get('pmd:scenarioDate', 'unknown')
        self.model_sv_id = kwargs.get('pmd:fullModel_ID', 'unknown')
        self.version = int(kwargs.get('pmd:version', 999))
        self.creation_timestamp = kwargs.get('pmd:creationDate', 'unknown')
        self.file_name = kwargs.get('pmd:fileName', 'unknown')


class HandlerMergeModels:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()
        self.elk_logging_handler = get_elk_logging_handler()
        self.opdm_service = None

    @staticmethod
    def run_loadflow(merged_model):
        # Set starting point of lf settings priority list
        if json.loads(ENABLE_DYNAMIC_MERGE_SETTINGS.lower()):
            settings_list = [param.strip() for param in MERGE_LOAD_FLOW_SETTINGS_PRIORITY.split(",")]
            settings_priority = next((i for i, value in enumerate(settings_list) if value == MERGE_LOAD_FLOW_SETTINGS),
                                     None)
            settings_list = settings_list[settings_priority:]
        else:
            settings_list = [MERGE_LOAD_FLOW_SETTINGS]

        for lf_settings in settings_list:
            logger.info(f"Solving loadflow with settings: {lf_settings}")
            # report = pypowsybl.report.Reporter()
            manager = settings_manager.LoadflowSettingsManager(settings_keyword=lf_settings)
            result = pypowsybl.loadflow.run_ac(network=merged_model.network,
                                               parameters=manager.build_pypowsybl_parameters(),
                                               # reporter=loadflow_report,
                                               )
            if result[0].status_text == 'Converged':
                break
            else:
                logger.warning(f"Failed to solve loadflow with settings: {lf_settings}")

        result_dict = [attr_to_dict(island) for island in result]
        # Modify all nested objects to native data types
        for island in result_dict:
            island['status'] = island['status'].name
            # Extract only first slack bus from internal pypowsybl object
            slack_bus_results = island.pop('slack_bus_results')
            if slack_bus_results:
                island['slack_bus_id'] = getattr(slack_bus_results[0], 'id', 'undefined')
                island['active_power_mismatch'] = getattr(slack_bus_results[0], 'active_power_mismatch', float())
            else:
                island['slack_bus_id'] = 'undefined'
                island['active_power_mismatch'] = float()

        # merged_model.loadflow = json.loads(loadflow_report.to_json())
        # merged_model.loadflow = str(loadflow_report)
        merged_model.loadflow = [island for island in result_dict if island['reference_bus_id']]
        merged_model.loadflow_status = result[0].status.name  # store main island loadflow status
        merged_model.loadflow_settings = lf_settings

        return merged_model

    def handle(self, task_object: dict, properties: dict, **kwargs):

        start_time = datetime.datetime.now(datetime.UTC)

        # Create instance of merged model
        merged_model = MergedModel()

        # Parse relevant data from Task
        task = task_object
        if not isinstance(task, dict):
            task = json.loads(task_object)

        # Convert task fields to bool where necessary
        task = convert_dict_str_to_bool(task)

        # TODO - make it to a wrapper once it is settled/standardized how this info is exchanged
        # Initialize trace
        self.elk_logging_handler.start_trace(task)
        logger.debug(task)

        # Set task to started
        update_task_status(task, "started")

        # Task configuration
        task_creation_time = task.get('task_creation_time')
        task_properties = task.get('task_properties', {})
        included_models = task_properties.get('included', [])
        excluded_models = task_properties.get('excluded', [])
        local_import_models = task_properties.get('local_import', [])
        time_horizon = task_properties["time_horizon"]
        scenario_datetime = task_properties["timestamp_utc"]
        schedule_start = task_properties.get("reference_schedule_start_utc")
        schedule_end = task_properties.get("reference_schedule_end_utc")
        schedule_time_horizon = task_properties.get("reference_schedule_time_horizon")
        merging_area = task_properties["merge_type"]
        merging_entity = task_properties["merging_entity"]
        mas = task_properties["mas"]
        version = task_properties["version"]
        model_replacement = task_properties["replacement"]
        model_scaling = task_properties["scaling"]
        model_upload_to_opdm = task_properties["upload_to_opdm"]
        model_upload_to_minio = task_properties["upload_to_minio"]
        model_merge_report_send_to_elk = task_properties["send_merge_report"]
        post_temp_fixes = task_properties['post_temp_fixes']
        force_outage_fix = task_properties['force_outage_fix']
        lvl8_reporting = task_properties['lvl8_reporting']

        # Get aligned schedules
        # Set default time horizon and scenario timestamp if not provided
        if not schedule_time_horizon or schedule_time_horizon == "AUTO":
            schedule_time_horizon = time_horizon

        if not schedule_start:
            schedule_start = scenario_datetime

        ac_schedules = query_acnp_schedules(time_horizon=schedule_time_horizon, scenario_timestamp=schedule_start)
        dc_schedules = query_hvdc_schedules(time_horizon=schedule_time_horizon, scenario_timestamp=schedule_start)
        acnp_dict = calculate_ac_net_position(ac_schedules)

        # Collect valid models from ObjectStorage
        downloaded_models = get_latest_models_and_download(time_horizon=time_horizon,
                                                           scenario_date=scenario_datetime,
                                                           valid=True,
                                                           data_source='OPDM')
        latest_boundary = get_latest_boundary()

        # Filter out models that are not to be used in merge
        models = merge_functions.filter_models(models=downloaded_models,
                                               included_models=included_models,
                                               excluded_models=excluded_models,
                                               filter_on='pmd:TSO')

        merged_model.merge_included_entity = [ModelEntity(data_source='OPDM', quality_indicator='Valid', **model).__dict__ for model in models]

        # Get additional models from ObjectStorage if local import is configured
        if local_import_models:
            additional_models = get_latest_models_and_download(time_horizon=time_horizon,
                                                               scenario_date=scenario_datetime,
                                                               valid=True,
                                                               data_source='PDN')
            additional_models = merge_functions.filter_models(models=additional_models,
                                                              included_models=local_import_models,
                                                              filter_on='pmd:TSO')
            merged_model.merge_included_entity.extend(
                [ModelEntity(data_source='PDN', quality_indicator='Valid', **model).__dict__ for model in additional_models])

            missing_local_import = [tso for tso in local_import_models if
                                    tso not in [model['pmd:TSO'] for model in additional_models]]
            merged_model.excluded.extend([{'tso': tso, 'reason': 'missing-pdn'} for tso in missing_local_import])

            # Exclude models that are outside scheduled AC net position deadband of 200MW
            if acnp_dict:
                additional_models = filter_models_by_acnp(additional_models, acnp_dict, ACNP_THRESHOLD, CONFORM_LOAD_FACTOR)
                excluded_incorrect = [tso for tso in local_import_models
                                      if tso not in [model['pmd:TSO'] for model in additional_models] if tso not in missing_local_import]
                if excluded_incorrect:
                    logger.warning(f"Exluded TSO due to incorrect Schedules: {excluded_incorrect}")
                    merged_model.excluded.extend([{'tso': tso, 'reason': 'incorrect-schedule'} for tso in excluded_incorrect])
                    missing_local_import = [tso for tso in local_import_models if tso not in [model['pmd:TSO'] for model in additional_models]]

            # Perform local replacement if configured
            if model_replacement and missing_local_import:
                try:
                    logger.info(f"Running replacement for local storage missing models: {missing_local_import}")
                    replacement_models_local = run_replacement(tso_list=missing_local_import,
                                                               time_horizon=time_horizon,
                                                               scenario_date=scenario_datetime,
                                                               data_source='PDN',
                                                               acnp_dict=acnp_dict,
                                                               acnp_threshold=ACNP_THRESHOLD,
                                                               conform_load_factor=CONFORM_LOAD_FACTOR)

                    logger.info(
                        f"Local storage replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models_local]}")
                    replaced_entities_local = [ModelEntity(data_source='PDN', quality_indicator='Substituted', **model).__dict__ for model in
                                               replacement_models_local]
                    merged_model.replaced_entity.extend(replaced_entities_local)
                    additional_models.extend(replacement_models_local)
                except Exception as error:
                    logger.error(f"Failed to run replacement: {error} {error.with_traceback()}")
        else:
            additional_models = []

        # Check missing models for replacement
        if included_models:
            missing_models = [model for model in included_models if model not in [model['pmd:TSO'] for model in models]]
            if missing_models:
                merged_model.excluded.extend([{'tso': tso, 'reason': 'missing-opdm'} for tso in missing_models])
        else:
            if model_replacement:
                # Get TSOs who models are available in storage for replacement period
                available_tsos = get_tsos_available_in_storage(time_horizon=time_horizon)
                valid_model_tsos = [model['pmd:TSO'] for model in models]
                # Need to ensure that excluded models by task configuration would not be taken in replacement context
                missing_models = [tso for tso in available_tsos if tso not in valid_model_tsos + excluded_models]
            else:
                missing_models = []

        # Exclude models that are outside scheduled AC net position deadband of 200MW
        if included_models:
            if acnp_dict:
                models = filter_models_by_acnp(models, acnp_dict, ACNP_THRESHOLD, CONFORM_LOAD_FACTOR)
                excluded_incorrect = [model for model in included_models if model not in [model['pmd:TSO'] for model in models] if model not in missing_models]
                if excluded_incorrect:
                    logger.warning(f"Exluded TSO due to incorrect Schedules: {excluded_incorrect}")
                    merged_model.excluded.extend([{'tso': tso, 'reason': 'incorrect-schedule'} for tso in missing_models])

        # Run replacement on missing models
        if model_replacement and missing_models:
            try:
                logger.info(f"Running replacement for missing models: {missing_models}")
                replacement_models = run_replacement(missing_models,
                                                     time_horizon,
                                                     scenario_datetime,
                                                     acnp_dict=acnp_dict,
                                                     acnp_threshold=ACNP_THRESHOLD,
                                                     conform_load_factor=CONFORM_LOAD_FACTOR)
                if replacement_models:
                    logger.info(
                        f"Replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models]}")
                    replaced_entities = [ModelEntity(data_source='OPDM', quality_indicator='Substituted', **model).__dict__ for model in
                                         replacement_models]
                    merged_model.replaced_entity.extend(replaced_entities)
                    models.extend(replacement_models)
                    merged_model.replaced = True
                else:
                    merged_model.replaced = False
            except Exception as error:
                logger.error(f"Failed to run replacement: {error}")
                merged_model.replaced = False

        # Store models together with boundary set and check whether there are enough models to merge
        input_models = models + additional_models + [latest_boundary]
        if len(input_models) < 2:
            logger.warning("No valid models found for merging, exiting merge process")
            properties.headers['success'] = False
            return task_object, properties

        # Load network model and merge
        merge_start = datetime.datetime.now(datetime.UTC)
        merged_model.network = load_network_model(opdm_objects=input_models)
        merged_model.network_meta = attr_to_dict(instance=merged_model.network, sanitize_to_strings=True)
        merged_model.included = [model['pmd:TSO'] for model in input_models if model.get('pmd:TSO', None)]

        # Crosscheck replaced model outages with latest UAP if at least one Baltic model was replaced
        replaced_tso_list = [entity['tso'] for entity in merged_model.replaced_entity]

        # Update model outages
        tso_list = []
        if force_outage_fix:  # force outage fix on all models if set
            tso_list = merged_model.included
        elif merging_area == 'BA' and any(tso in ['LITGRID', 'AST', 'ELERING'] for tso in
                                          replaced_tso_list):  # by default do it on Baltic merge replaced models
            tso_list = replaced_tso_list
        if tso_list:  # if not set force and not replaced BA then nothing to fix
            merged_model = merge_functions.update_model_outages(merged_model=merged_model,
                                                                tso_list=tso_list,
                                                                scenario_datetime=scenario_datetime,
                                                                time_horizon=time_horizon)

        # Various corrections from igmsshvscgmssh error
        if json.loads(REMOVE_GENERATORS_FROM_SLACK_DISTRIBUTION.lower()):
            merged_model.network = handle_igm_ssh_vs_cgm_ssh_error(network_pre_instance=merged_model.network)

        # Ensure boundary point EquivalentInjection are set to zero for paired tie lines
        merged_model.network = merge_functions.ensure_paired_equivalent_injection_compatibility(
            network=merged_model.network)

        # Ensure boundary line connectivity consistency for paired boundary lines
        merged_model.network = merge_functions.ensure_paired_boundary_line_connectivity(network=merged_model.network)

        # TODO - run other LF if default fails
        # Run loadflow on merged model
        merged_model = self.run_loadflow(merged_model=merged_model)
        logger.info(
            f"Loadflow status of main island: {merged_model.loadflow_status} [settings: {merged_model.loadflow_settings}]")

        # Perform scaling
        if model_scaling:

            # Scale balance if all schedules were received
            if all([ac_schedules, dc_schedules]):
                try:
                    merged_model = scaler.scale_balance(model=merged_model,
                                                        ac_schedules=ac_schedules,
                                                        dc_schedules=dc_schedules,
                                                        lf_settings=getattr(loadflow_settings,
                                                                            merged_model.loadflow_settings))
                except Exception as e:
                    logger.error(e)
                    merged_model.scaled = False
            else:
                logger.warning(f"Schedule reference data not available: {schedule_time_horizon} for {schedule_start}")
                logger.warning(f"Network model schedule scaling not performed")
                merged_model.scaled = False

        # Record main merging process end
        merge_end = datetime.datetime.now(datetime.UTC)

        # Update time_horizon in case of generic ID process type
        if time_horizon.upper() == "ID":
            time_horizon = merge_functions.set_intraday_time_horizon(scenario_datetime, task_creation_time)
            logger.info(f"Setting intraday time horizon to: {time_horizon}")
            merged_model.time_horizon_id = time_horizon

        # Set merged model name
        model_type = "RMM" if merging_area == "BA" else "CGM"
        merged_model.name = f"{model_type}_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"

        # Crate OPDM object for merged model
        opdm_object_meta = merge_functions.create_merged_model_opdm_object(
            object_id=merged_model.network_meta['id'].split("uuid:")[-1],
            time_horizon=time_horizon,
            merging_entity=merging_entity,
            merging_area=merging_area,
            scenario_date=scenario_datetime,
            mas=mas,
            version=version,
        )
        # Export merged model
        # TODO change here to export SSH profiles as well
        exported_model = merge_functions.export_merged_model(network=merged_model.network,
                                                             opdm_object_meta=opdm_object_meta,
                                                             profiles=["SV"],
                                                             cgm_convention=False)

        # Run post-processing
        post_p_start = datetime.datetime.now(datetime.UTC)
        logger.info(f"Starting merged model post-processing")
        # TODO here should be one existing network structure. IIDM model can be exported and removed to release memory
        sv_data, ssh_data, opdm_object_meta = merge_functions.run_post_merge_processing(input_models=input_models,
                                                                                        exported_model=exported_model,
                                                                                        opdm_object_meta=opdm_object_meta,
                                                                                        enable_temp_fixes=post_temp_fixes,
                                                                                        task_properties=task_properties
                                                                                        )

        # for merge report need to get the final uuid.
        merged_model.network_meta['fullModel_ID'] = opdm_object_meta['pmd:fullModel_ID']
        # Package both input models and exported CGM profiles to in memory zip files
        serialized_data = export_to_cgmes_zip([ssh_data, sv_data])
        post_p_end = datetime.datetime.now(datetime.UTC)
        logger.debug(f"Post processing took: {(post_p_end - post_p_start).total_seconds()} seconds")
        logger.debug(f"Merging took: {(merge_end - merge_start).total_seconds()} seconds")

        # Upload to OPDM 
        if model_upload_to_opdm:
            if merged_model.loadflow[0]['status'] == 'CONVERGED':  # Only upload if the model LF is solved
                try:
                    self.opdm_service = opdm.OPDM()

                    if SEND_TYPE == 'SOAP':
                        for item in serialized_data:
                            logger.info(f"Uploading to OPDM: {item.name}")
                            #time.sleep(1) tested before with 2 and 4. Currently removing as FS gonna be default. 
                            async_call(function=self.opdm_service.publication_request,
                                    callback=log_opdm_response,
                                    file_path_or_file_object=item)
                    elif SEND_TYPE == 'FS':
                        for item in serialized_data:
                            logger.info(f"Uploading to OPDM: {item.name}")
                            self.opdm_service.put_file(file_id=item.name,
                                          file_content=item)

                    merged_model.uploaded_to_opde = True
                except Exception as error:
                    logging.error(f"Unexpected error on uploading to OPDM: {error}", exc_info=True)
            else:
                logger.info(f"Model not uploaded to OPDM due to convergance issue: {merged_model.loadflow[0]['status']}")

        # Create zipped model data
        merged_model_object = BytesIO()
        with ZipFile(merged_model_object, "w") as merged_model_zip:
            # Include CGM model files
            for item in serialized_data:
                merged_model_zip.writestr(item.name, item.getvalue())

            # Include original IGM files
            for input_model in input_models:
                for instance in input_model['opde:Component']:
                    if instance['opdm:Profile']['pmd:cgmesProfile'] in ['EQ', 'TP', 'EQBD', 'TPBD', 'EQ_BD', 'TP_BD']:
                        file_object = get_opdm_component_data_bytes(opdm_component=instance)
                        logging.info(f"Adding file: {file_object.name}")
                        merged_model_zip.writestr(file_object.name, file_object.getvalue())
        merged_model_object.name = f"{OUTPUT_MINIO_FOLDER}/{merged_model.name}.zip"

        # Upload to Minio storage
        if model_upload_to_minio:
            logger.info(f"Uploading merged model to MINIO: {merged_model_object.name}")
            minio_metadata = merge_functions.evaluate_trustability(merged_model.__dict__, task['task_properties'])
            try:
                response = self.minio_service.upload_object(file_path_or_file_object=merged_model_object,
                                                            bucket_name=OUTPUT_MINIO_BUCKET,
                                                            metadata=minio_metadata,
                                                            )
                if response:
                    merged_model.uploaded_to_minio = True
            except Exception as error:
                logging.error(f"Unexpected error on uploading to Object Storage: {error}", exc_info=True)

        logger.info(f"Merged model creation done for: {merged_model.name}")

        end_time = datetime.datetime.now(datetime.UTC)
        task_duration = end_time - start_time
        logger.info(f"Task ended at {end_time}, total run time {task_duration}",
                    extra={"task_duration": task_duration.total_seconds(),
                           "task_start_time": start_time.isoformat(),
                           "task_end_time": end_time.isoformat()})

        # Set task to finished
        update_task_status(task, "finished")
        logger.debug(task)

        # Update merged model attributes
        merged_model.duration_s = (end_time - merge_start).total_seconds()
        merged_model.content_reference = merged_model_object.name

        # Update OPDM object data with CGM relevant data and send to Elastic
        opdm_object_meta['pmd:content-reference'] = merged_model.content_reference
        response = elastic.Elastic.send_to_elastic(index=OPDE_MODELS_ELK_INDEX, json_message=opdm_object_meta)

        # Send merge report and OPDM object metadata to Elastic
        merge_report = None
        if model_merge_report_send_to_elk:
            logger.info(f"Sending merge report to Elastic")
            try:
                merge_report = merge_functions.generate_merge_report(merged_model=merged_model, task=task)
                try:
                    response = elastic.Elastic.send_to_elastic(index=MERGE_REPORT_ELK_INDEX, json_message=merge_report)
                except Exception as error:
                    logger.error(f"Merge report sending to Elastic failed: {error}")
            except Exception as error:
                logger.error(f"Failed to create merge report: {error}")

        # Send QAS level 8 report if configured
        if lvl8_reporting and merge_report:
            try:
                lvl8_report = merge_functions.lvl8_report_cgm(merge_report=merge_report)
                service_edx = edx.EDX()
                message_id = service_edx.send_message(receiver_EIC=QAS_EIC,
                                                      business_type=QAS_MSG_TYPE,
                                                      content=lvl8_report)
                logger.info(f"QAS-Level-8 report generated and sent with ID: {message_id}")
            except Exception as error:
                logger.error(f"Failed to send QAS-Level-8 report with error: {error}")
        else:
            logger.warning(f"QAS-Level-8 not generated because merge report unavailable or disabled by configuration")

        # Append message headers with OPDM root metadata
        extracted_meta = {key: value for key, value in opdm_object_meta.items() if isinstance(value, str)}
        properties.headers = extracted_meta

        # Stop Trace
        self.elk_logging_handler.stop_trace()

        logger.info(f"Merge task finished for model: {merged_model.name}")

        return json.dumps(opdm_object_meta), properties


if __name__ == "__main__":
    sample_task = {
        "@context": "https://example.com/task_context.jsonld",
        "@type": "Task",
        "@id": "urn:uuid:ee3c57bf-fa4e-402c-82ac-7352c0d8e118",
        "process_id": "https://example.com/processes/CGM_CREATION",
        "run_id": "https://example.com/runs/IntraDayCGM/1",
        "job_id": "urn:uuid:d9343f48-23cd-4d8a-ae69-1940a0ab1837",
        "task_type": "automatic",
        "task_initiator": "cgm_handler_unit_test",
        "task_priority": "normal",
        "task_creation_time": "2024-11-28T20:39:42.448064",
        "task_update_time": "",
        "task_status": "created",
        "task_status_trace": [
            {
                "status": "created",
                "timestamp": "2024-11-28T20:39:42.448064"
            }
        ],
        "task_dependencies": [],
        "task_tags": [],
        "task_retry_count": 0,
        "task_timeout": "PT1H",
        "task_gate_open": "2024-05-24T21:00:00+00:00",
        "task_gate_close": "2024-05-24T21:15:00+00:00",
        "job_period_start": "2024-05-24T22:00:00+00:00",
        "job_period_end": "2024-05-25T06:00:00+00:00",
        "task_properties": {
            "timestamp_utc": "2025-10-07T12:30:00+00:00",
            "merge_type": "BA",
            "merging_entity": "BALTICRCC",
            "included": ["PSE", "AST"],
            "excluded": [],
            "local_import": ["LITGRID", "ELERING"],
            "time_horizon": "2D",
            "version": "000",
            "mas": "http://www.baltic-rsc.eu/OperationalPlanning",
            "post_temp_fixes": "True",
            "fix_net_interchange2": "True",
            "replacement": "True",
            "scaling": "True",
            "upload_to_opdm": "False",
            "upload_to_minio": "False",
            "send_merge_report": "False",
            "force_outage_fix": "False",
            "lvl8_reporting": "False"
        }
    }

    worker = HandlerMergeModels()
    finished_task = worker.handle(sample_task, {})
