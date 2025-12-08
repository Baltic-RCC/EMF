import logging
import config
from io import BytesIO
import json
import triplets
import time
from emf.common.helpers.opdm_objects import create_opdm_objects
from emf.common.helpers.utils import zip_xml
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, opdm, minio_api

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_retriever.model_retriever)


class HandlerModelsFromOPDM:

    def __init__(self):
        while True:
            try:
                self.opdm_service = opdm.OPDM()
                logger.info("Connected to OPDM successfully")
                break
            except Exception as e:
                logger.error(f"Failed to connect to OPDM: {e}")
                time.sleep(60)  # wait 60 seconds before retry

    def handle(self, message: bytes, properties: dict, **kwargs):
        # Load from binary to json
        opdm_objects = json.loads(message)

        for opdm_object in opdm_objects:

            party = opdm_object.get('pmd:TSO', '') # import only the filtered parties
            time_horizon = opdm_object.get('pmd:timeHorizon', '') # import only filtered timeframes
            process_party_exclusion = PROCESS_PARTY.split(',')
            process_timehorizon_exclusion = PROCESS_TH.split(',')
            if (party not in process_party_exclusion) and (time_horizon not in process_timehorizon_exclusion):
                self.opdm_service.download_object(opdm_object=opdm_object)
                opdm_object["data-source"] = "OPDM"
            else:
                logger.warning(f"{party} and {time_horizon} message not processed due to configured filtering") # if out of filter raise exception and move on
                properties.header['success'] = False
                return  opdm_objects, properties

        return opdm_objects, properties


class HandlerModelsFromBytesIO:

    def __init__(self):
        pass

    def handle(self, message: bytes, properties: dict, **kwargs):

        message_content = BytesIO(message)
        message_content.name = 'unknown.zip'
        rdfxml_files = triplets.rdf_parser.find_all_xml([message_content])

        # Repackage to form of zip(xml)
        rdfzip_files = []
        for xml in rdfxml_files:
            rdfzip_files.append(zip_xml(xml_file_object=xml))

        # Create OPDM objects
        opdm_objects = create_opdm_objects(models=[rdfzip_files], metadata={"data-source": "PDN"})

        return opdm_objects, properties


class HandlerModelsToMinio:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()

    def handle(self, message: bytes, properties: dict, **kwargs):

        opdm_objects = message

        if isinstance(message, bytes):
            opdm_objects = json.loads(message)

        # Download each OPDM object network model from OPDE
        for opdm_object in opdm_objects:

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
                        component['opdm:Profile']['DATA'] = None
                        continue

                # Put content data into bytes object
                output_object = BytesIO(component['opdm:Profile']['DATA'])

                # Delete data
                component['opdm:Profile']['DATA'] = None

                # Upload components to minio storage
                output_object.name = content_reference
                logger.info(f"Uploading component to object storage: {output_object.name}")
                self.minio_service.upload_object(file_path_or_file_object=output_object, bucket_name=MINIO_BUCKET)

            # Store minio bucket name in metadata object
            opdm_object["minio-bucket"] = MINIO_BUCKET

        return json.dumps(opdm_objects), properties


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
