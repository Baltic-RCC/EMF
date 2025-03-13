import logging
import config
from io import BytesIO
from zipfile import ZipFile
import json

from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, opdm, minio_api

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)


class HandlerModelsToMinio:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio_api.ObjectStorage()

    def handle(self, message: bytes, properties: dict, **kwargs):

        # Load from binary to json
        opdm_objects = json.loads(message)

        # Download each OPDM object network model from OPDE
        for opdm_object in opdm_objects:

            # Get model from OPDM
            self.opdm_service.download_object(opdm_object=opdm_object)

            # Put all components to bytesio zip (each component to different zip)
            for component in opdm_object['opde:Component']:

                # Sanitize content-reference url
                content_reference = component['opdm:Profile']['pmd:content-reference']
                content_reference = content_reference.replace('//', '/')

                # Check whether profile already exist in object storage (Minio)
                if component['opdm:Profile']['pmd:cgmesProfile'] == "EQ":  # TODO currently only for EQ
                    profile_exist = self.minio_service.object_exists(bucket_name=MINIO_BUCKET, object_name=content_reference)
                    if profile_exist:
                        logger.info(f"Profile already stored in object storage: {content_reference}")
                        continue

                # Put content data into bytes object
                output_object = BytesIO()
                with ZipFile(output_object, "w") as component_zip:
                    with ZipFile(BytesIO(component['opdm:Profile']['DATA'])) as profile_zip:
                        for file_name in profile_zip.namelist():
                            logger.debug(f"Adding file: {file_name}")
                            component_zip.writestr(file_name, profile_zip.open(file_name).read())

                # Upload components to minio storage
                output_object.name = content_reference
                logger.info(f"Uploading component to object storage: {output_object.name}")
                self.minio_service.upload_object(file_path_or_file_object=output_object, bucket_name=MINIO_BUCKET)

        return message, properties


class HandlerModelsToValidator:

    def __init__(self):
        pass

    def handle(self, message: bytes, properties: dict, **kwargs):

        # Load from binary to json
        opdm_objects = json.loads(message)

        for opdm_object in opdm_objects:
            # Append message headers with OPDM root metadata
            extracted_meta = {key: value for key, value in opdm_object.items() if isinstance(value, str)}
            properties.headers.update(extracted_meta)

        # Publish to other queue/exchange
        rmq_channel = kwargs.get('channel', None)
        if rmq_channel:
            logger.info(f"Publishing message to exchange/queue: {OUTPUT_RMQ_QUEUE}")
            rmq_channel.basic_publish(exchange='', routing_key=OUTPUT_RMQ_QUEUE, body=message, properties=properties)

        return message, properties


if __name__ == "__main__":
    # Testing
    import sys
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -45s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Get models metadata from OPDE
    # transfer_model_meta_from_opde_to_elk()

    # Get models from OPDM and store to MINIO
    service = elastic.Elastic()
    opdm_object = service.get_doc_by_id(index="models-opde-202309", id='723eb242-686c-42f1-85e3-81d38aab31e0').body['_source']
    opdm_service = opdm.OPDM()
    minio_service = minio_api.ObjectStorage()
    updated_opdm_objects = opde_models_to_minio(
        opdm_objects=[opdm_object],
        opdm_service=opdm_service,
        minio_service=minio_service,
    )
