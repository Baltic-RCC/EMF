import logging
import config
import json
from uuid import uuid4
import datetime
from emf.task_generator.time_helper import parse_datetime
from io import BytesIO
from zipfile import ZipFile
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import opdm, minio
from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
from emf.loadflow_tool import loadflow_settings
from emf.loadflow_tool.model_merger.merge_functions import filter_models, fix_sv_tapsteps, fix_sv_shunts, load_model, run_lf, create_sv_and_updated_ssh, export_to_cgmes_zip
from emf.task_generator.task_generator import update_task_status

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.merger)


class HandlerRmmToPdnAndMinio:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio.ObjectStorage()

    def handle(self, task_object: dict, **kwargs):

        start_time = datetime.datetime.utcnow()

        # Parse relevant data from Task
        task = task_object

        if not isinstance(task, dict):
            task = json.loads(task_object)

        # Set task to started
        update_task_status(task, "started")

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

        # TODO - task to contain and increase version number
        version = task_properties["version"]

        # Collect valid models from ObjectStorage
        valid_models = get_latest_models_and_download(time_horizon, scenario_datetime, valid=True)
        latest_boundary = get_latest_boundary()

        # Filter out models that are not to be used in merge
        filtered_models = filter_models(valid_models, included_models, excluded_models, filter_on='pmd:TSO')

        # Get additional models directly from Minio
        additional_models_data = self.minio_service.get_latest_models_and_download(time_horizon, scenario_datetime, local_import_models, bucket_name=INPUT_MINIO_BUCKET, prefix=INPUT_MINIO_FOLDER)

        # Load all selected models
        input_models = filtered_models + additional_models_data + [latest_boundary]
        merged_model = load_model(input_models)

        # TODO - run other LF if default fails
        solved_model = run_lf(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT)

        # Update time_horizon in case of generic ID process type
        if time_horizon.upper() == "ID":
            _task_creation_time = parse_datetime(task_creation_time, keep_timezone=False)
            _scenario_datetime = parse_datetime(scenario_datetime, keep_timezone=False)

            time_horizon = f"{int((_scenario_datetime - _task_creation_time).seconds/3600):02d}"
            logger.info(f"Setting ID TimeHorizon to {time_horizon}")

        # TODO - get version dynamically form ELK
        sv_data, ssh_data = create_sv_and_updated_ssh(solved_model, input_models, scenario_datetime, time_horizon, version, merging_area, merging_entity, mas)

        # Fix SV
        sv_data = fix_sv_shunts(sv_data, input_models)
        sv_data = fix_sv_tapsteps(sv_data, ssh_data)

        # Package both input models and exported CGM profiles to in memory zip files
        serialized_data = export_to_cgmes_zip([ssh_data, sv_data])

        # Set RMM name
        rmm_name = f"RMM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"

        rmm_data = BytesIO()
        with ZipFile(rmm_data, "w") as rmm_zip:

            # Include CGM model files
            for item in serialized_data:
                rmm_zip.writestr(item.name, item.getvalue())

            # Include original IGM files
            for object in input_models:
                for instance in object['opde:Component']:
                    with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                        for file_name in instance_zip.namelist():
                            logging.info(f"Adding file: {file_name}")
                            rmm_zip.writestr(file_name, instance_zip.open(file_name).read())

        # Upload to Object Storage
        rmm_object = rmm_data
        rmm_object.name = f"{OUTPUT_MINIO_FOLDER}/{rmm_name}.zip"
        logger.info(f"Uploading RMM to MINO {OUTPUT_MINIO_BUCKET}/{rmm_object.name}")

        try:
            self.minio_service.upload_object(rmm_object, bucket_name=OUTPUT_MINIO_BUCKET)
        except:
            logging.error(f"""Unexpected error on uploading to Object Storage:""", exc_info=True)

        logger.info(f"RMM creation done for {rmm_name}")
        end_time = datetime.datetime.utcnow()
        task_duration = end_time - start_time
        logger.info(f"Task ended at {end_time}, total run time {task_duration}",
                    extra={"task_duration": task_duration.total_seconds(),
                           "task_start_time": start_time.isoformat(),
                           "task_end_time": end_time.isoformat()})

        # Set task to started
        update_task_status(task, "finished")

        logger.debug(task)

        return task


if __name__ == "__main__":

    import sys
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    sample_task = {
        "@context": "https://example.com/task_context.jsonld",
        "@type": "Task",
        "@id": "urn:uuid:ee3c57bf-fa4e-402c-82ac-7352c0d8e118",
        "process_id": "https://example.com/processes/CGM_CREATION",
        "run_id": "https://example.com/runs/IntraDayCGM/1",
        "job_id": "urn:uuid:d9343f48-23cd-4d8a-ae69-1940a0ab1837",
        "task_type": "automatic",
        "task_initiator": "teenus.testrscjslv1",
        "task_priority": "normal",
        "task_creation_time": "2024-05-28T20:39:42.448064",
        "task_update_time": "",
        "task_status": "created",
        "task_status_trace": [
            {
                "status": "created",
                "timestamp": "2024-05-28T20:39:42.448064"
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
            "timestamp_utc": "2024-05-22T11:30:00+00:00",
            "merge_type": "EU",
            "merging_entity": "BALTICRSC",
            "included": ["ELERING", "AST", "PSE"],
            "excluded": [],
            "local_import": ["LITGRID"],
            "time_horizon": "ID",
            "version": "106",
            "mas": "http://www.baltic-rsc.eu/OperationalPlanning/RMM"
        }
    }

    worker = HandlerRmmToPdnAndMinio()
    finished_task = worker.handle(sample_task)



