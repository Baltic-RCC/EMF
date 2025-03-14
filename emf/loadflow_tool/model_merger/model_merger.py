import logging
import math

import pypowsybl

import config
import json
import time
from uuid import uuid4
import datetime
from emf.task_generator.time_helper import parse_datetime
from io import BytesIO
from zipfile import ZipFile
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import opdm, minio_api, elastic
from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
from emf.loadflow_tool import loadflow_settings
from emf.loadflow_tool.helper import opdmprofile_to_bytes
from emf.loadflow_tool.model_merger import merge_functions
from emf.task_generator.task_generator import update_task_status
from emf.common.logging.custom_logger import get_elk_logging_handler
from emf.loadflow_tool.replacement import run_replacement, get_available_tsos, run_replacement_local
from emf.loadflow_tool import scaler
# TODO - move this async solution to some common module
from concurrent.futures import ThreadPoolExecutor
from lxml import etree
from emf.loadflow_tool.model_merger.temporary_fixes import run_post_merge_processing, run_pre_merge_processing, fix_model_outages

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


class HandlerMergeModels:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio_api.ObjectStorage()
        self.elk_logging_handler = get_elk_logging_handler()

    def handle(self, task_object: dict, **kwargs):

        start_time = datetime.datetime.now(datetime.UTC)
        merge_log = {"uploaded_to_opde": False,
                     "uploaded_to_minio": False,
                     "scaled": False,
                     "exclusion_reason": [],
                     "replacement": False,
                     "replaced_entity": [],
                     "replacement_reason": [],
                     "outages_corrected": False,
                     "outage_fixes": [],
                     "outages_unmapped": []}

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
            additional_models_data = self.minio_service.get_latest_models_and_download(time_horizon, scenario_datetime,
                                                                                       local_import_models,
                                                                                       bucket_name=INPUT_MINIO_BUCKET,
                                                                                       prefix=INPUT_MINIO_FOLDER)

            missing_local_import = [tso for tso in local_import_models if tso not in [model['pmd:TSO'] for model in additional_models_data]]
            merge_log.get('exclusion_reason').extend([{'tso': tso, 'reason': 'Model missing from PDN'} for tso in missing_local_import])

            # local replacement if configured
            if model_replacement_local and missing_local_import:
                try:
                    logger.info(f"Running replacement for local storage missing models: {missing_local_import}")
                    replacement_models_local = run_replacement_local(missing_local_import, time_horizon,
                                                                     scenario_datetime)
                    if replacement_models_local:
                        for model in replacement_models_local:
                            additional_models_data_replace = self.minio_service.get_latest_models_and_download(
                                model['pmd:timeHorizon'],
                                model['pmd:scenarioDate'],
                                [model['pmd:TSO']],
                                bucket_name=INPUT_MINIO_BUCKET,
                                prefix=INPUT_MINIO_FOLDER)[0]
                            additional_models_data_replace["@time_horizon"] = model["pmd:timeHorizon"]
                            additional_models_data_replace["@timestamp"] = model["pmd:scenarioDate"]
                            additional_models_data_replace["pmd:versionNumber"] = model["pmd:versionNumber"]
                            additional_models_data.append(additional_models_data_replace)

                        logger.info(f"Local storage replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models_local]}")
                        merge_log.get('replaced_entity').extend([{'tso': model['pmd:TSO'], 'replacement_time_horizon': model[
                                                                          'pmd:timeHorizon'], 'replacement_scenario_date': model[
                                                                          'pmd:scenarioDate']} for model in replacement_models_local])

                except Exception as error:
                    logger.error(f"Failed to run replacement: {error} {error.with_traceback()}")
        else:
            additional_models_data = []


        # Check model validity and availability
        valid_models = [model for model in filtered_models if model['valid']]
        invalid_models = [model['pmd:TSO'] for model in filtered_models if model not in valid_models]
        if invalid_models:
            merge_log.get('exclusion_reason').extend([{'tso': tso, 'reason': 'Model is not valid'} for tso in invalid_models])

        # Check missing models for replacement
        if included_models:
            missing_models = [model for model in included_models if model not in [model['pmd:TSO'] for model in valid_models]]
            if missing_models:
                merge_log.get('exclusion_reason').extend([{'tso': tso, 'reason': 'Model missing from OPDM'} for tso in missing_models])
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
                    logger.info(
                        f"Replacement model(s) found: {[model['pmd:fileName'] for model in replacement_models]}")
                    merge_log.get('replaced_entity').extend([{'tso': model['pmd:TSO'],
                                                              'replacement_time_horizon': model['pmd:timeHorizon'],
                                                              'replacement_scenario_date': model['pmd:scenarioDate']}
                                                             for model in replacement_models])
                    valid_models = valid_models + replacement_models
                    merge_log.update({'replacement': True})
                    # TODO put exclusion_reason logging under replacement
            except Exception as error:
                logger.error(f"Failed to run replacement: {error}")

        valid_models = valid_models + additional_models_data
        valid_tso = [tso['pmd:TSO'] for tso in valid_models]

        # Return None if there are no models to be merged
        if not valid_models:
            logger.warning("Found no valid models to merge, returning None")
            return None

        # Load all selected models
        input_models = valid_models + [latest_boundary]
        if len(input_models) < 2:
            logger.warning("Found no models to merge, returning None")
            return None

        # Run pre-processing
        pre_p_start = datetime.datetime.now(datetime.UTC)
        if pre_temp_fixes:
            input_models = run_pre_merge_processing(input_models, merging_area)
        pre_p_end = datetime.datetime.now(datetime.UTC)
        logger.debug(f"Pre-processing took: {(pre_p_end - pre_p_start).total_seconds()} seconds")

        # Load network model and merge
        merge_start = datetime.datetime.now(datetime.UTC)
        merged_model = merge_functions.load_model(input_models)
        # TODO - run other LF if default fails

        # Crosscheck replaced model outages with latest UAP if atleast one baltic model was replaced
        replaced_tso_list = [model['tso'] for model in merge_log['replaced_entity']]

        # Fix model outages
        if force_outage_fix:
            merged_model, merge_log = fix_model_outages(merged_model, valid_tso, merge_log, scenario_datetime, time_horizon,
                                                        debug=False)
        elif merging_area == 'BA' and any(tso in ['LITGRID', 'AST', 'ELERING'] for tso in replaced_tso_list):
            merged_model, merge_log = fix_model_outages(merged_model, replaced_tso_list, merge_log, scenario_datetime, time_horizon)

        # Various fixes from igmsshvscgmssh error
        if remove_non_generators_from_slack_participation:
            network_pre_instance = merged_model["network"]
            try:
                all_generators = network_pre_instance.get_elements(element_type=pypowsybl.network.ElementType.GENERATOR,
                                                                   all_attributes=True).reset_index()
                generators_mask = (all_generators['CGMES.synchronousMachineOperatingMode'].str.contains('generator'))
                not_generators = all_generators[~generators_mask]
                generators = all_generators[generators_mask]
                curve_points = (network_pre_instance
                                .get_elements(
                    element_type=pypowsybl.network.ElementType.REACTIVE_CAPABILITY_CURVE_POINT,
                    all_attributes=True).reset_index())
                curve_limits = (curve_points.merge(generators[['id']], on='id')
                                .groupby('id').agg(curve_p_min=('p', 'min'), curve_p_max=('p', 'max'))).reset_index()
                curve_generators = generators.merge(curve_limits, on='id')
                # low end can be zero
                curve_generators = curve_generators[(curve_generators['target_p'] > curve_generators['curve_p_max']) |
                                                    ((curve_generators['target_p'] > 0) &
                                                     (curve_generators['target_p'] < curve_generators['curve_p_min']))]
                if not curve_generators.empty:
                    logger.warning(f"Found {len(curve_generators.index)} generators for "
                                   f"which p > max(reactive capacity curve(p)) or p < min(reactive capacity curve(p))")
                    # in order this to work curve_p_min <= p_min <= target_p <= p_max <= curve_p_max
                    # Solution 1: set max_p from curve max, it should contain p on target-p
                    upper_limit_violated = curve_generators[
                        (curve_generators['max_p'] > curve_generators['curve_p_max'])]
                    if not upper_limit_violated.empty:
                        logger.warning(f"Updating max p from curve for {len(upper_limit_violated.index)} generators")
                        upper_limit_violated['max_p'] = upper_limit_violated['curve_p_max']
                        network_pre_instance.update_generators(upper_limit_violated[['id', 'max_p']].set_index('id'))

                    lower_limit_violated = curve_generators[
                        (curve_generators['min_p'] < curve_generators['curve_p_min'])]
                    if not lower_limit_violated.empty:
                        logger.warning(f"Updating min p from curve for {len(lower_limit_violated.index)} generators")
                        lower_limit_violated['min_p'] = lower_limit_violated['curve_p_min']
                        network_pre_instance.update_generators(lower_limit_violated[['id', 'min_p']].set_index('id'))

                    # Solution 2: discard generator from participating
                    extensions = network_pre_instance.get_extensions('activePowerControl')
                    remove_curve_generators = extensions.merge(curve_generators[['id']],
                                                               left_index=True, right_on='id')
                    if not remove_curve_generators.empty:
                        remove_curve_generators['participate'] = False
                        network_pre_instance.update_extensions('activePowerControl',
                                                               remove_curve_generators.set_index('id'))
                condensers = all_generators[(all_generators['CGMES.synchronousMachineType'].str.contains('condenser'))
                                            & (abs(all_generators['p']) > 0)
                                            & (abs(all_generators['target_p']) == 0)]
                # Fix condensers that have p not zero by setting their target_p to equal to p
                if not condensers.empty:
                    logger.warning(f"Found {len(condensers.index)} condensers for which p ~= 0 & target_p = 0")
                    condensers.loc[:, 'target_p'] = condensers['p'] * (-1)
                    network_pre_instance.update_generators(condensers[['id', 'target_p']].set_index('id'))
                # Remove all not generators from active power distribution
                if not not_generators.empty:
                    logger.warning(f"Removing {len(not_generators.index)} machines from power distribution")
                    extensions = network_pre_instance.get_extensions('activePowerControl')
                    remove_not_generators = extensions.merge(not_generators[['id']], left_index=True, right_on='id')
                    remove_not_generators['participate'] = False
                    remove_not_generators = remove_not_generators.set_index('id')
                    network_pre_instance.update_extensions('activePowerControl', remove_not_generators)
            except Exception as ex:
                logger.error(f"Unable to pre-process for igm-cgm-ssh error: {ex}")
            merged_model["network"] = network_pre_instance

        solved_model = merge_functions.run_lf(merged_model, loadflow_settings=getattr(loadflow_settings, MERGE_LOAD_FLOW_SETTINGS))
        # Perform scaling
        if model_scaling:
            # Get aligned schedules
            ac_schedules = scaler.query_acnp_schedules(time_horizon=time_horizon, scenario_timestamp=scenario_datetime)
            dc_schedules = scaler.query_hvdc_schedules(time_horizon=time_horizon, scenario_timestamp=scenario_datetime)
            # Scale balance if all schedules were received
            if all([ac_schedules, dc_schedules]):
                solved_model['network'] = scaler.scale_balance(network=solved_model['network'],
                                                               ac_schedules=ac_schedules,
                                                               dc_schedules=dc_schedules)
            else:
                logger.warning(f"Schedule reference data not available, skipping model scaling")

        merge_end = datetime.datetime.now(datetime.UTC)
        logger.info(f"Loadflow status of main island: {solved_model['LOADFLOW_RESULTS'][0]['status_text']}")

        # Update time_horizon in case of generic ID process type
        new_time_horizon = None
        if time_horizon.upper() == "ID":
            max_time_horizon_value = 36
            _task_creation_time = parse_datetime(task_creation_time, keep_timezone=False)
            _scenario_datetime = parse_datetime(scenario_datetime, keep_timezone=False)
            time_horizon = '01'  # DEFAULT VALUE, CHANGE THIS
            time_diff = _scenario_datetime - _task_creation_time
            # column B
            # time_horizon = f"{math.ceil((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
            # column C (original)
            # time_horizon = f"{int((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
            # column D
            # time_horizon = f"{math.floor((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
            if time_diff.days >= 0 and time_diff.days <= 1:
                # column E
                # time_horizon_actual = math.ceil((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600)
                # column F
                # time_horizon_actual = int((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600)
                # column G
                time_horizon_actual = math.floor((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600)
                # just in case cut it to bigger than 1 once again
                time_horizon_actual = max(time_horizon_actual, 1)
                if time_horizon_actual <= max_time_horizon_value:
                    time_horizon = f"{time_horizon_actual:02d}"
            new_time_horizon = time_horizon
            # Check this, is it correct to  update the value here or it should be given to post processing"
            # task_properties["time_horizon"] = time_horizon
            logger.info(f"Setting intraday time horizon to: {time_horizon}")

        # Run post-processing
        post_p_start = datetime.datetime.now(datetime.UTC)
        sv_data, ssh_data = run_post_merge_processing(input_models,
                                                      solved_model,
                                                      task_properties,
                                                      SMALL_ISLAND_SIZE,
                                                      enable_temp_fixes=post_temp_fixes,
                                                      time_horizon=new_time_horizon)

        # Package both input models and exported CGM profiles to in memory zip files
        serialized_data = merge_functions.export_to_cgmes_zip([ssh_data, sv_data])
        post_p_end = datetime.datetime.now(datetime.UTC)
        logger.debug(f"Post proocessing took: {(post_p_end - post_p_start).total_seconds()} seconds")
        logger.debug(f"Merging took: {(merge_end - merge_start).total_seconds()} seconds")

        # Upload to OPDM
        if model_upload_to_opdm:
            try:
                for item in serialized_data:
                    logger.info(f"Uploading to OPDM: {item.name}")
                    time.sleep(2)
                    async_call(function=self.opdm_service.publication_request, callback=log_opdm_response,
                               file_path_or_file_object=item)
                    merge_log.update({'uploaded_to_opde': True})
            except:
                logging.error(f"""Unexpected error on uploading to OPDM:""", exc_info=True)

        # Create zipped model data
        if merging_area == 'BA':
            cgm_name = f"RMM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"
        else:
            cgm_name = f"CGM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"
        cgm_data = BytesIO()
        with ZipFile(cgm_data, "w") as cgm_zip:

            # Include CGM model files
            for item in serialized_data:
                cgm_zip.writestr(item.name, item.getvalue())

            # Include original IGM files
            for object in input_models:
                for instance in object['opde:Component']:
                    if instance['opdm:Profile']['pmd:cgmesProfile'] in ['EQ', 'TP', 'EQBD', 'TPBD']:
                        file_object = opdmprofile_to_bytes(instance)
                        logging.info(f"Adding file: {file_object.name}")
                        cgm_zip.writestr(file_object.name, file_object.getvalue())

        cgm_object = cgm_data
        cgm_object.name = f"{OUTPUT_MINIO_FOLDER}/{cgm_name}.zip"

        # Send to Object Storage
        if model_upload_to_minio:
            logger.info(f"Uploading CGM to MINIO: {OUTPUT_MINIO_BUCKET}/{cgm_object.name}")
            try:
                response = self.minio_service.upload_object(cgm_object, bucket_name=OUTPUT_MINIO_BUCKET)
                if response:
                    merge_log.update({'uploaded_to_minio': True})
            except:
                logging.error(f"""Unexpected error on uploading to Object Storage:""", exc_info=True)

        logger.info(f"CGM creation done for {cgm_name}")

        end_time = datetime.datetime.now(datetime.UTC)
        task_duration = end_time - start_time
        logger.info(f"Task ended at {end_time}, total run time {task_duration}",
                    extra={"task_duration": task_duration.total_seconds(),
                           "task_start_time": start_time.isoformat(),
                           "task_end_time": end_time.isoformat()})

        # Set task to finished
        update_task_status(task, "finished")
        logger.debug(task)

        # Update merge log
        merge_log.update({'task': task,
                          'small_island_size': SMALL_ISLAND_SIZE,
                          'loadflow_settings': MERGE_LOAD_FLOW_SETTINGS,
                          'merge_duration': f'{(merge_end - merge_start).total_seconds()}',
                          'content_reference': cgm_object.name,
                          'cgm_name': cgm_name})

        # Send merge report to Elastic
        if model_merge_report_send_to_elk:
            logger.info(f"Sending merge report to Elastic")
            try:
                merge_report = merge_functions.generate_merge_report(solved_model, valid_models, merge_log)
                try:
                    response = elastic.Elastic.send_to_elastic(index=MERGE_REPORT_ELK_INDEX,
                                                               json_message=merge_report)
                except Exception as error:
                    logger.error(f"Merge report sending to Elastic failed: {error}")
            except Exception as error:
                logger.error(f"Failed to create merge report: {error}")

        # Stop Trace
        self.elk_logging_handler.stop_trace()

        logger.info(f"Merge task finished for model: {cgm_name}")

        return task


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
