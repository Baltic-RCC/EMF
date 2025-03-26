import logging
import pandas
import pypowsybl
import config
import json
import time
from uuid import uuid4
import datetime
from dataclasses import dataclass, field
from typing import List

from emf.loadflow_tool.model_merger.merge_functions import calculate_intraday_time_horizon
from emf.common.time_helper import parse_datetime
from io import BytesIO
from zipfile import ZipFile
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import opdm, minio_api, elastic
from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
from emf.common.integrations.object_storage.schedules import query_acnp_schedules, query_hvdc_schedules
from emf.loadflow_tool import loadflow_settings
from emf.loadflow_tool.helper import opdmprofile_to_bytes, attr_to_dict
from emf.loadflow_tool.model_merger import merge_functions
from emf.task_generator.task_generator import update_task_status
from emf.common.logging.custom_logger import get_elk_logging_handler
from emf.loadflow_tool.replacement import run_replacement, get_available_tsos, run_replacement_local
from emf.loadflow_tool import scaler
# TODO - move this async solution to some common module
from concurrent.futures import ThreadPoolExecutor
from lxml import etree
from emf.loadflow_tool.model_merger.temporary_fixes import run_post_merge_processing, run_pre_merge_processing, \
    fix_model_outages, open_switches_in_network, fix_igm_ssh_vs_cgm_ssh_error

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


def convert_dict_str_to_bool(data_dict: dict):
    for key, value in data_dict.items():
        if isinstance(value, str):
            if value in ['True', 'true', 'TRUE']:
                data_dict[key] = True
            elif value in ['False', 'false', 'FALSE']:
                data_dict[key] = False
        elif isinstance(value, dict):
            # Recursively converter nested dictionaries
            data_dict[key] = convert_dict_str_to_bool(value)

    return data_dict


@dataclass
class MergedModel:
    network: pypowsybl.network = None
    time_horizon = None
    name = None
    loadflow_status: str | None = None

    # Status flags
    scaled: bool = False
    replaced: bool = False
    outages: bool = False
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


class HandlerMergeModels:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio_api.ObjectStorage()
        self.elk_logging_handler = get_elk_logging_handler()

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
        merging_area = task_properties["merge_type"]
        merging_entity = task_properties["merging_entity"]
        mas = task_properties["mas"]
        version = task_properties["version"]
        model_replacement = task_properties["replacement"]
        model_replacement_local = task_properties["replacement_local"]
        model_scaling = task_properties["scaling"]
        model_upload_to_opdm = task_properties["upload_to_opdm"]
        model_upload_to_minio = task_properties["upload_to_minio"]
        model_merge_report_send_to_elk = task_properties["send_merge_report"]
        pre_temp_fixes = task_properties['pre_temp_fixes']
        post_temp_fixes = task_properties['post_temp_fixes']
        force_outage_fix = task_properties['force_outage_fix']
        open_non_retained_switches_between_tn = json.loads(OPEN_NON_RETAINED_SWITCHES_BETWEEN_TN.lower())
        try:
            net_interchange_threshold = int(NET_INTERCHANGE_THRESHOLD)
        except Exception:
            net_interchange_threshold = 200

        task_properties['net_interchange2_threshold'] = net_interchange_threshold

        remove_non_generators_from_slack_participation = True

        # Collect valid models from ObjectStorage
        downloaded_models = get_latest_models_and_download(time_horizon, scenario_datetime, valid=False)
        latest_boundary = get_latest_boundary()

        # Filter out models that are not to be used in merge
        filtered_models = merge_functions.filter_models(downloaded_models, included_models, excluded_models, filter_on='pmd:TSO')

        # Get additional models directly from Minio
        if local_import_models:
            additional_models_data = self.minio_service.get_latest_models_and_download(time_horizon=time_horizon,
                                                                                       scenario_datetime=scenario_datetime,
                                                                                       model_entity=local_import_models,
                                                                                       bucket_name=INPUT_MINIO_BUCKET,
                                                                                       prefix=INPUT_MINIO_FOLDER)

            missing_local_import = [tso for tso in local_import_models if tso not in [model['pmd:TSO'] for model in additional_models_data]]
            merged_model.excluded.extend([{'tso': tso, 'reason': 'Missing in PDN'} for tso in missing_local_import])

            # Perform local replacement if configured
            if model_replacement_local and missing_local_import:
                try:
                    logger.info(f"Running replacement for local storage missing models: {missing_local_import}")
                    replacement_models_local = run_replacement_local(tso_list=missing_local_import,
                                                                     time_horizon=time_horizon,
                                                                     scenario_date=scenario_datetime)
                    if replacement_models_local:
                        for model in replacement_models_local:
                            additional_models_data_replace = self.minio_service.get_latest_models_and_download(
                                time_horizon=model['pmd:timeHorizon'],
                                scenario_datetime=model['pmd:scenarioDate'],
                                model_entity=[model['pmd:TSO']],
                                bucket_name=INPUT_MINIO_BUCKET,
                                prefix=INPUT_MINIO_FOLDER)[0]
                            additional_models_data_replace["@time_horizon"] = model["pmd:timeHorizon"]
                            additional_models_data_replace["@timestamp"] = model["pmd:scenarioDate"]
                            additional_models_data_replace["pmd:versionNumber"] = model["pmd:versionNumber"]
                            additional_models_data.append(additional_models_data_replace)

                        logger.info(f"Local storage replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models_local]}")
                        replaced_entities_local = [{'tso': model['pmd:TSO'], 'time_horizon': model[
                            'pmd:timeHorizon'], 'scenario_timestamp': model[
                            'pmd:scenarioDate']} for model in replacement_models_local]
                        merged_model.replaced_entity.extend(replaced_entities_local)
                except Exception as error:
                    logger.error(f"Failed to run replacement: {error} {error.with_traceback()}")
        else:
            additional_models_data = []

        # Check model validity and availability
        valid_models = [model for model in filtered_models if model['valid']]
        invalid_models = [model['pmd:TSO'] for model in filtered_models if model not in valid_models]
        if invalid_models:
            merged_model.excluded.extend([{'tso': tso, 'reason': 'Invalid'} for tso in invalid_models])

        # Check missing models for replacement
        if included_models:
            missing_models = [model for model in included_models if model not in [model['pmd:TSO'] for model in valid_models]]
            if missing_models:
                merged_model.excluded.extend([{'tso': tso, 'reason': 'Missing in OPDM'} for tso in missing_models])
        else:
            if model_replacement:
                available_tsos = get_available_tsos()
                missing_models = [model for model in available_tsos if model not in [model['pmd:TSO'] for model in valid_models] + excluded_models]
            else:
                missing_models = []

        # Run replacement on missing/invalid models
        if model_replacement and missing_models:
            try:
                logger.info(f"Running replacement for missing models: {missing_models}")
                replacement_models = run_replacement(missing_models, time_horizon, scenario_datetime)
                if replacement_models:
                    logger.info(f"Replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models]}")
                    replaced_entities = [{'tso': model['pmd:TSO'],
                                          'time_horizon': model['pmd:timeHorizon'],
                                          'scenario_timestamp': model['pmd:scenarioDate']}
                                         for model in replacement_models]
                    merged_model.replaced_entity.extend(replaced_entities)
                    valid_models = valid_models + replacement_models
                    merged_model.replaced = True
                    # TODO put exclusion_reason logging under replacement
            except Exception as error:
                logger.error(f"Failed to run replacement: {error}")

        # Store all relevant models for loading
        valid_models = valid_models + additional_models_data

        # Return None if there are no models to be merged
        if not valid_models:
            logger.warning("Found no valid models to merge, returning None")
            return None

        # Store models together with boundary
        input_models = valid_models + [latest_boundary]
        if len(input_models) < 2:
            logger.warning("Found no models to merge, returning None")
            return None

        # Run pre-processing
        pre_p_start = datetime.datetime.now(datetime.UTC)
        between_tn = pandas.DataFrame()
        if pre_temp_fixes:
            input_models, between_tn = run_pre_merge_processing(input_models, merging_area)
        pre_p_end = datetime.datetime.now(datetime.UTC)
        logger.debug(f"Pre-processing took: {(pre_p_end - pre_p_start).total_seconds()} seconds")

        # Load network model and merge
        merge_start = datetime.datetime.now(datetime.UTC)
        merged_model.network = merge_functions.load_model(opdm_objects=input_models)
        merged_model.network_meta = attr_to_dict(instance=merged_model.network, sanitize_to_strings=True)
        merged_model.included = [model['pmd:TSO'] for model in valid_models if model['pmd:TSO']]

        # Crosscheck replaced model outages with latest UAP if at least one Baltic model was replaced
        replaced_tso_list = [entity['tso'] for entity in merged_model.replaced_entity]
        valid_tso_list = [tso['pmd:TSO'] for tso in valid_models]

        # Update model outages
        tso_list = []
        if force_outage_fix: #force outage fix on all models if set
            tso_list = valid_tso_list
        elif merging_area == 'BA' and any(tso in ['LITGRID', 'AST', 'ELERING'] for tso in replaced_tso_list): #by default do it on Baltic merge replaced models
            tso_list = replaced_tso_list
        if tso_list: #if not set force and not replaced BA then nothing to fix
            merged_model = fix_model_outages(merged_model=merged_model,
                                             tso_list=tso_list,
                                             scenario_datetime=scenario_datetime,
                                             time_horizon=time_horizon)

        # Various fixes from igmsshvscgmssh error
        if remove_non_generators_from_slack_participation:
            merged_model.network = fix_igm_ssh_vs_cgm_ssh_error(merged_model.network)

        if open_non_retained_switches_between_tn and not between_tn.empty:
            merged_model.network = open_switches_in_network(network_pre_instance=merged_model.network,
                                                            switches_dataframe=between_tn)

        # TODO - run other LF if default fails
        # Run loadflow on merged model
        merged_model = merge_functions.run_lf(merged_model=merged_model,
                                              loadflow_settings=getattr(loadflow_settings, MERGE_LOAD_FLOW_SETTINGS))

        # Perform scaling
        if model_scaling:
            # Get aligned schedules
            ac_schedules = query_acnp_schedules(time_horizon=time_horizon, scenario_timestamp=scenario_datetime)
            dc_schedules = query_hvdc_schedules(time_horizon=time_horizon, scenario_timestamp=scenario_datetime)
            # Scale balance if all schedules were received
            if all([ac_schedules, dc_schedules]):
                merged_model = scaler.scale_balance(model=merged_model,
                                                    ac_schedules=ac_schedules,
                                                    dc_schedules=dc_schedules,
                                                    lf_settings=getattr(loadflow_settings, MERGE_LOAD_FLOW_SETTINGS))
            else:
                logger.warning(f"Schedule reference data not available, skipping model scaling")

        merge_end = datetime.datetime.now(datetime.UTC)
        logger.info(f"Loadflow status of main island: {merged_model.loadflow[0]['status_text']}")

        # Update time_horizon in case of generic ID process type
        new_time_horizon = None
        if time_horizon.upper() == "ID":
            time_horizon = calculate_intraday_time_horizon(scenario_datetime, task_creation_time)
            new_time_horizon = time_horizon
            logger.info(f"Setting intraday time horizon to: {time_horizon}")

        # Run post-processing
        post_p_start = datetime.datetime.now(datetime.UTC)
        sv_data, ssh_data = run_post_merge_processing(input_models=input_models,
                                                      merged_model=merged_model,
                                                      task_properties=task_properties,
                                                      small_island_size=SMALL_ISLAND_SIZE,
                                                      enable_temp_fixes=post_temp_fixes,
                                                      time_horizon=new_time_horizon)

        # Package both input models and exported CGM profiles to in memory zip files
        serialized_data = merge_functions.export_to_cgmes_zip([ssh_data, sv_data])
        post_p_end = datetime.datetime.now(datetime.UTC)
        logger.debug(f"Post processing took: {(post_p_end - post_p_start).total_seconds()} seconds")
        logger.debug(f"Merging took: {(merge_end - merge_start).total_seconds()} seconds")

        # Upload to OPDM
        if model_upload_to_opdm:
            try:
                for item in serialized_data:
                    logger.info(f"Uploading to OPDM: {item.name}")
                    time.sleep(2)
                    async_call(function=self.opdm_service.publication_request,
                               callback=log_opdm_response,
                               file_path_or_file_object=item)
                merged_model.uploaded_to_opde = True
            except:
                logging.error(f"""Unexpected error on uploading to OPDM:""", exc_info=True)

        # Create zipped model data
        if merging_area == 'BA':
            merged_model.name = f"RMM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"
        else:
            merged_model.name = f"CGM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"
        merged_model_data = BytesIO()
        with ZipFile(merged_model_data, "w") as merged_model_zip:
            # Include CGM model files
            for item in serialized_data:
                merged_model_zip.writestr(item.name, item.getvalue())

            # Include original IGM files
            for input_model in input_models:
                for instance in input_model['opde:Component']:
                    if instance['opdm:Profile']['pmd:cgmesProfile'] in ['EQ', 'TP', 'EQBD', 'TPBD']:
                        file_object = opdmprofile_to_bytes(instance)
                        logging.info(f"Adding file: {file_object.name}")
                        merged_model_zip.writestr(file_object.name, file_object.getvalue())

        merged_model_object = merged_model_data
        merged_model_object.name = f"{OUTPUT_MINIO_FOLDER}/{merged_model.name}.zip"

        # Upload to Minio storage
        if model_upload_to_minio:
            logger.info(f"Uploading merged model to MINIO: {merged_model_object.name}")
            try:
                response = self.minio_service.upload_object(merged_model_object, bucket_name=OUTPUT_MINIO_BUCKET)
                if response:
                    merged_model.uploaded_to_minio = True
            except:
                logging.error(f"""Unexpected error on uploading to Object Storage:""", exc_info=True)

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
        merged_model.loadflow_settings = MERGE_LOAD_FLOW_SETTINGS
        merged_model.duration_s = (end_time - merge_start).total_seconds()
        merged_model.content_reference = merged_model_object.name

        # Send merge report to Elastic
        if model_merge_report_send_to_elk:
            logger.info(f"Sending merge report to Elastic")
            try:
                merge_report = merge_functions.generate_merge_report(merged_model=merged_model, task=task)
                try:
                    response = elastic.Elastic.send_to_elastic(index=MERGE_REPORT_ELK_INDEX,
                                                               json_message=merge_report)
                except Exception as error:
                    logger.error(f"Merge report sending to Elastic failed: {error}")
            except Exception as error:
                logger.error(f"Failed to create merge report: {error}")

        # Stop Trace
        self.elk_logging_handler.stop_trace()

        logger.info(f"Merge task finished for model: {merged_model.name}")

        return task, properties


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
            "timestamp_utc": "2025-02-15T08:30:00+00:00",
            "merge_type": "BA",
            "merging_entity": "BALTICRCC",
            "included": ['PSE', 'AST', 'LITGRID'],
            "excluded": [],
            "local_import": ['ELERING'],
            "time_horizon": "ID",
            "version": "99",
            "mas": "http://www.baltic-rsc.eu/OperationalPlanning",
            "pre_temp_fixes": "True",
            "post_temp_fixes": "True",
            "fix_net_interchange2": "True",
            "replacement": "True",
            "replacement_local": "True",
            "scaling": "True",
            "upload_to_opdm": "False",
            "upload_to_minio": "False",
            "send_merge_report": "False",
            "force_outage_fix": "True",
        }
    }

    worker = HandlerMergeModels()
    finished_task = worker.handle(sample_task)
