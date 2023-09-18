import logging
import config
from io import BytesIO
from zipfile import ZipFile
import json
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic, opdm
from emf.common.converters import opdm_metadata_to_json

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)

# EDX > opdm objects
# OPDM.download_object(opdm object) > opdm objects (enhanced with data
# a) NEW MINIO.upload_opdm(opdm object) > opdm objects
# b) MINIO.upload_opdm(bytes) > url
# b) update opdm object with url
# loadflow_tool.validate(opdm_object + bds opdmobject) -> validated model
# update opdm object with validation status
# opdm object remove binary data
# elk handler.batch_upload(opdm_object)


def transfer_model_meta_from_opde_to_elk():
    message_types = EDX_MESSAGE_TYPE.split(",")
    elk_handler = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True, id_metadata_list=['opde:Id'])
    service = edx.EDX(converter=opdm_metadata_to_json, handler=elk_handler, message_types=message_types)
    service.run()


def get_opdm_object_from_edx(message_type: str, edx_service: object):

    message = edx_service.receive_message(message_type)
    if not message.receivedMessage:
        logger.info(f"No messages available with message type: {message_type}")
        return

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


def opde_models_to_minio(opdm_objects: list, opdm_service: object, minio_service: object) -> list:
    """
    Method to move igm models from OPDE to Minio storage
    :param opdm_objects: list of opdm objects
    :param opdm_service: opdm api service instance
    :param minio_service: minio api service instance
    :return: list of opdm objects updated with minio url where it was stored
    """
    updated_opdm_objects = []

    for opdm_object in opdm_objects:
        # Get model from OPDM
        response = opdm_service.download_object(opdm_object={'opdm:OPDMObject': opdm_object})

        # Put all components to bytesio zip
        output_object = BytesIO()
        with ZipFile(output_object, "w") as global_zip:
            for instance in response['opdm:OPDMObject']['opde:Component']:
                with ZipFile(BytesIO(instance['opdm:Profile']['DATA'])) as instance_zip:
                    for file_name in instance_zip.namelist():
                        logger.debug(f"Adding file: {file_name}")
                        global_zip.writestr(file_name, instance_zip.open(file_name).read())

        # Upload model to minio storage
        _name = f"{opdm_object['opde:Object-Type']}_{opdm_object['pmd:validFrom']}_{opdm_object['pmd:timeHorizon']}_{opdm_object['pmd:TSO']}_{opdm_object['pmd:versionNumber']}.zip"
        output_object.name = f"EMF_OS/{_name}"
        minio_service.upload_object(file_path_or_file_object=output_object,
                                    bucket_name="opde-confidential-models")

        # Update metadata object by stored file url
        opdm_object['URL'] = output_object.name
        updated_opdm_objects.append(opdm_object)

    return updated_opdm_objects


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
