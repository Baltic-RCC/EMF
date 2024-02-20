import logging
import config
from emf.common.integrations import elastic
from emf.common.integrations.minio import ObjectStorage
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.integrations.object_storage)


def query_model_data_from_elastic(query_dict, index=ELASTIC_QUERY_INDEX, return_payload=False):

    # Create elastic query syntax
    _query_match_list = [{"match": {key: query_dict.get(key)}} for key in query_dict]
    query = {"bool": {"must": _query_match_list}}

    response = elastic.Elastic().client.search(index=index, query=query)
    content = response['hits']['hits'][0]['_source']

    if return_payload:
        logger.info(f"Getting data from MinIO")
        opde_components = content['opde:Component']
        for num, component in enumerate(opde_components):
            content_reference = opde_components[num]["opdm:Profile"]['pmd:content-reference']
            logger.info(f"Downloading {content_reference}")
            opde_components[num]["opdm:Profile"]["DATA"] = get_payload_from_minio(content_reference)
        logger.info("Model profile data downloaded")

    return content


def get_payload_from_minio(object_name, bucket_name=MINIO_BUCKET_NAME):

    return ObjectStorage().download_object(bucket_name, object_name)


if __name__ == '__main__':
    import sys

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    test_query = {"pmd:TSO": "TERNA",
                  "pmd:timeHorizon": "2D",
                  "pmd:scenarioDate": "2024-02-15T22:30:00Z",
                  }

    test_response = query_model_data_from_elastic(test_query, return_payload=True)
    logger.info("Test script finished")