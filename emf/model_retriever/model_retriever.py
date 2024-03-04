import logging
import config
from io import BytesIO
from zipfile import ZipFile
from typing import List
import json
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic, opdm
from emf.common.converters import opdm_metadata_to_json
from emf.loadflow_tool.validator import validate_model

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)


class HandlerModelsToMinio:

    def __init__(self):
        self.opdm_service = opdm.OPDM()
        self.minio_service = minio.ObjectStorage()

    def handle(self, message_body: bytes):
        # Load from binary to json
        opdm_objects = json.loads(message_body)

        # Download each OPDM object network model from OPDE
        updated_opdm_objects = []
        for opdm_object in opdm_objects:

            # Get model from OPDM
            response = opdm_service.download_object(opdm_object=opdm_object)

            # Put all components to bytesio zip (each component to different zip)
            for component in response['opde:Component']:
                output_object = BytesIO()
                with ZipFile(output_object, "w") as component_zip:
                    with ZipFile(BytesIO(component['opdm:Profile']['DATA'])) as profile_zip:
                        for file_name in profile_zip.namelist():
                            logger.debug(f"Adding file: {file_name}")
                            component_zip.writestr(file_name, profile_zip.open(file_name).read())

                # Upload components to minio storage
                output_object.name = component['opdm:Profile']['pmd:content-reference']
                output_object.name = output_object.name.replace('//', '/')  # sanitize double slash in url
                logger.info(f"Uploading component to object storage: {output_object.name}")
                minio_service.upload_object(file_path_or_file_object=output_object, bucket_name=MINIO_BUCKET)

            # TODO backup solution
            # Put all components to bytesio zip (all components to one zip)
            # output_object = BytesIO()
            # with ZipFile(output_object, "w") as global_zip:
            #     for instance in response['opde:Component']:
            #         with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
            #             for file_name in instance_zip.namelist():
            #                 logger.debug(f"Adding file: {file_name}")
            #                 global_zip.writestr(file_name, instance_zip.open(file_name).read())

            # Upload model to minio storage
            # _name = f"{opdm_object['opde:Object-Type']}_{opdm_object['pmd:validFrom']}_{opdm_object['pmd:timeHorizon']}_{opdm_object['pmd:TSO']}_{opdm_object['pmd:versionNumber']}.zip"
            # output_object.name = opdm_object['pmd:content-reference']
            # minio_service.upload_object(file_path_or_file_object=output_object, bucket_name=MINIO_BUCKET)

            updated_opdm_objects.append(opdm_object)

        return updated_opdm_objects


class HandlerModelsValidator:

    def __init__(self):
        self.opdm_service = opdm.OPDM()

    def handle(self, opdm_objects: List[dict]):
        # Get the latest boundary set for validation
        latest_boundary = self.opdm_service.get_latest_boundary()

        # Run network model validation
        for opdm_object in opdm_objects:
            response = validate_model(opdm_objects=[opdm_object, latest_boundary])
            response.pop('NETWORK')  # pop out pypowsybl network object
            opdm_object["validation-status"] = response
            for component in opdm_object['opde:Component']:  # pop out initial binary network model data
                component['opdm:Profile'].pop('DATA')

        # TODO think whether DATA or pypowsybl network should be returned here in return. Also which one, original binary data or pypowsybl network object
        return opdm_objects


class HandlerMetadataToElastic:
    """Handler to send OPDM metadata object to Elastic"""
    def __init__(self):
        self.elastic_service = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])

    def handle(self, opdm_objects: List[dict], properties: dict | None = None):
        self.elastic_service.send(byte_string=json.dumps(opdm_objects, default=str).encode('utf-8'), properties=properties)
        logger.info(f"Network model metadata sent to object-storage.elk")

        return opdm_objects


def transfer_model_meta_from_opde_to_elk():
    message_types = EDX_MESSAGE_TYPE.split(",")
    elk_handler = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])
    service = edx.EDX(converter=opdm_metadata_to_json, handler=elk_handler, message_types=message_types)
    service.run()


def get_opdm_object_from_edx(message_type: str, edx_service: object):

    message = edx_service.receive_message(message_type)
    if not message.receivedMessage:
        logger.info(f"No messages available with message type: {message_type}")
        return None, None

    logger.info(f"Downloading message with ID: {message.receivedMessage.messageID}")

    # Extract data
    body = message.receivedMessage.content

    # Extract metadata
    properties = dict(message.receivedMessage.__values__)
    properties.pop('content', None)

    logger.info(f'Received message with metadata {properties}', extra=properties)

    # Convert message to json
    body, content_type = opdm_metadata_to_json.convert(body)
    logger.info(f"Message converted to: {content_type}")

    # ACK/Mark message received and move to next one
    edx_service.confirm_received_message(message.receivedMessage.messageID)
    logger.info(f"Number of messages left {message.remainingMessagesCount}")

    return body, properties


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
    from emf.common.integrations import minio

    service = elastic.Elastic()
    opdm_object = service.get_doc_by_id(index="models-opde-202309", id='723eb242-686c-42f1-85e3-81d38aab31e0').body['_source']
    opdm_service = opdm.OPDM()
    minio_service = minio.ObjectStorage()
    updated_opdm_objects = opde_models_to_minio(
        opdm_objects=[opdm_object],
        opdm_service=opdm_service,
        minio_service=minio_service,
    )
