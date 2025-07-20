import logging
import pandas as pd
import config
import json
from emf.common.config_parser import parse_app_properties
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.integrations import elastic, minio_api
from emf.common.integrations.object_storage import models
from triplets.rdf_parser import load_all_to_dataframe
from emf.model_quality.model_statistics import get_system_metrics
from emf.model_quality.quality_functions import generate_quality_report, process_zipped_cgm, set_common_metadata

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)


class HandlerModelQuality:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()
        self.elastic_service = elastic.Elastic()

    def handle(self, message: bytes, properties: dict, **kwargs):

        # Load OPDM metadata objects from binary to json
        model_metadata = json.loads(message)
        object_type = properties.headers['opde:Object-Type']
        common_metadata = set_common_metadata(model_metadata, object_type)

        if object_type == 'CGM':
            model_data = self.minio_service.download_object(model_metadata.get('minio-bucket', 'opde-confidential-models'),
                                                              model_metadata.get('pmd:content-reference'))
            logger.info(f"Loading merged model")
            unzipped = process_zipped_cgm(model_data)
            network= load_all_to_dataframe(unzipped)

        elif object_type == 'IGM':
            latest_boundary = models.get_latest_boundary()
            model_data = [models.get_content(metadata=opdm_object) for opdm_object in model_metadata]
            try:
                for opdm_object in model_data:
                    network = load_opdm_objects_to_triplets(opdm_objects=[opdm_object, latest_boundary])
            except:
                logger.error("Failed to load IGM")
                network = pd.DataFrame
        else:
            logger.error("Incorrect or missing metadata")
            network = pd.DataFrame

        # Generate quality report and network statistics
        if not network.empty:
            qa_report = generate_quality_report(network, object_type, model_metadata)
            try:
                model_statistics = get_system_metrics(network)
            except Exception as e:
                model_statistics = {}
                logger.error(f"Failed to get model statistics: {e}")
        else:
            raise TypeError("Model was not loaded correctly, either missing in MinIO or incorrect data")

        if model_statistics:
            model_statistics.update(common_metadata)
            try:
                response = self.elastic_service.send_to_elastic(index=ELK_STATISTICS_INDEX, json_message=model_statistics)
            except Exception as error:
                logger.error(f"Statistics report sending to Elastic failed: {error}")

            logger.info(f"Statistics report sent to elastic index: '{ELK_STATISTICS_INDEX}'")
        else:
            raise TypeError("Statistics report generator failed, data not sent")

        # Send validation report to Elastic
        if qa_report:
            qa_report.update(common_metadata)
            try:
                response = self.elastic_service.send_to_elastic(index=ELK_QUALITY_INDEX, json_message=qa_report)
            except Exception as error:
                logger.error(f"Validation report sending to Elastic failed: {error}")

            logger.info(f"Quality report sent to elastic index: '{ELK_QUALITY_INDEX}'")
        else:
            logger.error("Error, quality report generator failed, data not sent")

        return message, properties
