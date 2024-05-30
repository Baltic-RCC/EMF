import logging
import config
from io import BytesIO
from zipfile import ZipFile
from typing import List
import json

from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic, opdm, minio
from emf.loadflow_tool.merger import *

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

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
        "timestamp_utc": "2024-05-25T02:30:00+00:00",
        "merge_type": "EU",
        "included": [],
        "excluded": [],
        "time_horizon": "1D"
    }
}



class HandlerRmmToPdnAndMinio:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio.ObjectStorage()
        self.edx_service = edx.create_client()

    def handle(self, task_object: dict, **kwargs):

        start_time = datetime.datetime.utcnow()

        # Parse relevant data from Task
        task = json.loads(task_object)

        task["task_status_trace"].append(
            {
                "status": "task_started",
                "timestamp": start_time.isoformat()
            }
        )

        task_properties = task.get('task_properties', {})
        included_models = task_properties.get('included', [])
        excluded_models = task_properties.get('excluded', [])
        local_import_models = task_properties.get('local', [])

        time_horizon = task_properties["time_horizon"]
        scenario_datetime = task_properties["timestamp_utc"]
        merging_area = task_properties["merge_type"]

        # TODO add to task
        merging_entity = "BALTICRSC"
        mas = 'http://www.baltic-rsc.eu/OperationalPlanning/RMM'

        # TODO - task to contain and increase version number
        version = "104"

        # Set RMM name
        rmm_name = f"RMM_{time_horizon}_{version}_{parse_datetime(scenario_datetime):%Y%m%dT%H%MZ}_{merging_area}_{uuid4()}"

        # Collect models
        valid_models = get_latest_models_and_download(time_horizon, scenario_date, valid=True)
        latest_boundary = get_latest_boundary()

        filtered_models = filter_models(valid_models, included_models, excluded_models)

        # Get additional models


        if time_horizon == 'ID':
            # takes any integer between 0-32 which can be in model name
            model_name_pattern = f"{parse_datetime(scenario_date):%Y%m%dT%H%M}Z-({'0[0-9]|1[0-9]|2[0-9]|3[0-6]'})-({'|'.join(local_import_models)})"
        else:
            model_name_pattern = f"{parse_datetime(scenario_date):%Y%m%dT%H%M}Z-{time_horizon}-({'|'.join(local_import_models)})"
        additional_model_metadata = {'bamessageid': model_name_pattern}

        additional_models = self.minio_service.query_objects(
                                                            bucket_name=INPUT_MINIO_BUCKET,
                                                            prefix=INPUT_MINIO_FOLDER,
                                                            metadata=additional_model_metadata,
                                                            use_regex=True)

        # data_list = [
        #     {
        #         "pmd:content-reference": "something",
        #         'opde:Component':
        #         [
        #             {
        #                 'opdm:Profile':
        #                 {
        #                     "pmd:content-reference": "something",
        #                     "DATA": "something",
        #                 }
        #             }
        #
        #         ]
        #     }
        # ]

        additional_models_data = []


        # Add to data to load
        if additional_models:
            logger.info(f"Number of additional models returned -> {len(additional_models)}")
            for model in additional_models:

                opdm_object = {
                    "pmd:content-reference": model.object_name,
                    'opde:Component': []
                }

                logger.info(f"Loading additional model {model.name}", extra={"additional_model_name": model.name})
                model_data = BytesIO(self.minio_service.download_object(model.object_name))
                model_data.name = f"{model.name}.zip"

                with ZipFile(model_data) as source_zip:

                    for file_name in source_zip.namelist():
                        logging.info(f"Adding file: {file_name}")

                        data = BytesIO()
                        data.name = f"{file_name.split('.', maxsplit=1)[0]}.zip"
                        with ZipFile(data, "w") as data_zip:
                            data_zip.writestr(file_name, source_zip.open(file_name).read())

                        opdm_object['opde:Component'].append(
                            {
                                'opdm:Profile':
                                {
                                    "pmd:content-reference": data.name,
                                    "DATA": data.getvalue(),
                                }
                            }
                        )

                additional_models_data.append(opdm_object)
        else:
            logger.info(
                f"No additional models returned from {INPUT_MINIO_BUCKET} with -> prefix: {INPUT_MINIO_FOLDER}, metadata: {additional_model_metadata}")

        input_models = filtered_models + additional_models_data + [latest_boundary]
        merged_model = load_model(input_models)

        # TODO - run other LF if default fails
        solved_model = run_lf(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT)

        # TODO - get version dynamically form ELK
        sv_data, ssh_data = create_sv_and_updated_ssh(solved_model, valid_models, time_horizon, version, merging_area,
                                                      merging_entity, mas)

        # Fix SV
        sv_data = fix_sv_shunts(sv_data, valid_models)
        sv_data = fix_sv_tapsteps(sv_data, ssh_data)

        # Package both input models and exported CGM profiles to in memory zip files
        serialized_data = export_to_cgmes_zip([ssh_data, sv_data])

        rmm_data = BytesIO()
        with ZipFile(rmm_data, "w") as rmm_zip:

            # Include CGM model files
            for item in serialized_data:
                rmm_zip.writestr(item.file_name, item.getvalue())

            # Include original IGM files
            for object in input_models:
                for instance in object['opde:Component']:
                    with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                        for file_name in instance_zip.namelist():
                            logging.info(f"Adding file: {file_name}")
                            rmm_zip.writestr(file_name, instance_zip.open(file_name).read())



        # Upload to PDN
        business_type = OUTPUT_EDX_RECEIVER     # "MODELS-RMM"
        receiver = OUTPUT_EDX_BUSINESS_TYPE     # "PUBLICATION@SC-38V-0000000012-W"

        logger.info(f"Publishing {business_type} to {receiver}")
        try:
            rmm_publication_response = self.edx_service.send_message(receiver_EIC=receiver,
                                                                    business_type=business_type,
                                                                    content=rmm_data.getvalue(),
                                                                    ba_message_id=rmm_name.replace("_", "-"))

            logger.debug(self.edx_service.check_message_status(rmm_publication_response))

        except:
            logging.error(f"""Unexpected error on publishing to EDX:""", exc_info=True)

        # Upload to Object Storage
        rmm_object = rmm_data
        rmm_object.name = f"{OUTPUT_MINIO_FOLDER}/{rmm_name}.zip"
        logger.info(f"Uploading RMM to MINO {OUTPUT_MINIO_BUCKET}/{rmm_object.name}")
        try:

            self.minio_service.upload_object(rmm_object)

        except:
            logging.error(f"""Unexpected error on uploading to Object Storage:""", exc_info=True)

        logger.info(f"RMM creation done for {rmm_name}")
        end_time = datetime.now()
        task_duration = end_time - start_time
        logger.info(f"Task ended at {end_time}, total run time {task_duration}",
                    extra={"task_duration": task_duration.total_seconds(),
                           "task_start_time": start_time.isoformat(),
                           "task_end_time": end_time.isoformat()})

        task["task_status_trace"].append(
            {
                "status": "task_finished",
                "timestamp": datetime.utcnow().isoformat()
            }
        )

        logger.debug(task)

        return task
