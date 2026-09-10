import logging
import config
import json
from emf.common.config_parser import parse_app_properties
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.integrations import elastic, minio_api
from emf.common.integrations.object_storage import models
from triplets.rdf_parser import load_all_to_dataframe
from emf.common.helpers.statistics import get_system_metrics, get_tieflow_data
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
        rule_sets = {'igm_rule_set': IGM_RULE_SET.split(','), 'cgm_rule_set': CGM_RULE_SET.split(',')}

        if object_type == 'CGM':
            try:
                model_data = self.minio_service.download_object(
                    model_metadata.get('minio-bucket', 'opde-confidential-models'),
                    model_metadata.get('pmd:content-reference'))
                logger.info(f"Loading merged model")
                unzipped = process_zipped_cgm(model_data)
                network = load_all_to_dataframe(unzipped)
            except Exception as error:
                logger.error(f"Failed to load CGM data: {error}", exc_info=True)
                return message, properties
            common_metadata = set_common_metadata(model_metadata, object_type)
            self._generate_and_send_report(network, object_type, model_metadata, common_metadata, rule_sets)

        elif object_type == 'IGM':
            if not model_metadata:
                logger.warning("No OPDM objects received for quality check, exiting")
                return message, properties

            latest_boundary = models.get_latest_boundary()
            for opdm_object in model_metadata:
                try:
                    model_data = models.get_content(metadata=opdm_object)
                    network = load_opdm_objects_to_triplets(opdm_objects=[model_data, latest_boundary])
                except Exception as error:
                    logger.error(f"Failed to load IGM data for TSO '{opdm_object.get('pmd:TSO')}': {error}",
                                exc_info=True)
                    continue
                common_metadata = set_common_metadata(opdm_object, object_type)
                self._generate_and_send_report(network, object_type, opdm_object, common_metadata, rule_sets)

        else:
            logger.error("Object type metadata is incorrect")

        return message, properties

    def _generate_and_send_report(self, network, object_type, model_metadata, common_metadata, rule_sets):

        if network.empty:
            logger.error("Model was not loaded correctly, either missing in MinIO or incorrect data")
            return

        tieflow_data = get_tieflow_data(network)
        try:
            qa_report = generate_quality_report(self, network=network, object_type=object_type,
                                                model_metadata=model_metadata, rule_sets=rule_sets,
                                                tieflow_data=tieflow_data)
        except Exception as error:
            qa_report = {}
            logger.error(f"Failed to generate quality report: {error}", exc_info=True)

        try:
            model_statistics = get_system_metrics(network, tieflow_data=tieflow_data)
        except Exception as error:
            model_statistics = {}
            logger.error(f"Failed to get model statistics: {error}", exc_info=True)

        if model_statistics:
            model_statistics.update(common_metadata)
            try:
                self.elastic_service.send_to_elastic(index=ELK_STATISTICS_INDEX, json_message=model_statistics)
                logger.info(f"Statistics report sent to elastic index: '{ELK_STATISTICS_INDEX}'")
            except Exception as error:
                logger.error(f"Statistics report sending to Elastic failed: {error}", exc_info=True)
        else:
            logger.error("Statistics report generator failed, data not sent")

        # Send validation report to Elastic
        if qa_report:
            qa_report.update(common_metadata)
            try:
                self.elastic_service.send_to_elastic(index=ELK_QUALITY_INDEX, json_message=qa_report)
                logger.info(f"Quality report sent to elastic index: '{ELK_QUALITY_INDEX}'")
            except Exception as error:
                logger.error(f"Validation report sending to Elastic failed: {error}", exc_info=True)
        else:
            logger.error("Error, quality report generator failed, data not sent")
