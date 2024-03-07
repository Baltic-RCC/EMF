import logging
import config
from emf.common.integrations import elastic
from emf.common.integrations.minio import ObjectStorage
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.integrations.object_storage)
elastic_service = elastic.Elastic()
minio_service = ObjectStorage()


def query_data(metadata_query: dict, index=ELASTIC_QUERY_INDEX, return_payload=False):
    """
    Queries Elasticsearch based on provided metadata queries.

    Args:
        metadata_query (dict): A dictionary containing metadata fields and their values to be queried.
        index (str): The index to query data from. Defaults to ELASTIC_QUERY_INDEX from config variables.
        return_payload (bool): Optional. If True, retrieves the full content for each hit.
            Defaults to False.

    Returns:
        list: A list of dictionaries containing the retrieved content from Elasticsearch.

    Note:
        The function constructs an Elasticsearch query based on the provided metadata_query.
        It retrieves data from the specified index and processes the response to extract content.

    Example:
        To query data with metadata fields 'TSO' and 'timeHorizon' and return payload:
        >>> metadata_query = {"pmd:TSO": "TERNA", "pmd:timeHorizon": "2D"}
        ... response = query_data(metadata_query, return_payload=True)
    """

    # Create elastic query syntax
    query_match_list = [{"match": {key: metadata_query.get(key)}} for key in metadata_query]
    query = {"bool": {"must": query_match_list}}

    response = elastic_service.client.search(index=index, query=query, size='10000')
    content_list = [content["_source"] for content in response["hits"]["hits"]]

    if return_payload:
        for num, item in enumerate(content_list):
            content_list[num] = get_content(item)

    return content_list


def get_content(metadata: dict, bucket_name=MINIO_BUCKET_NAME):
    """
    Retrieves content data from MinIO based on metadata information.

    Args:
        metadata (dict): A dictionary containing metadata information.
        bucket_name (str): The name of the MinIO bucket to fetch data from.
            Defaults to MINIO_BUCKET_NAME from config variables.

    Returns:
        list: A list of dictionaries representing content components with updated 'DATA' field.

    Note:
        It expects metadata to contain 'opde:Component' information.
        For each component, it downloads data from MinIO and updates the 'DATA' field in the component dictionary.
    """

    logger.info(f"Getting data from MinIO")
    for component in metadata["opde:Component"]:
        content_reference = component.get("opdm:Profile").get("pmd:content-reference")
        logger.info(f"Downloading {content_reference}")
        component["opdm:Profile"]["DATA"] = minio_service.download_object(bucket_name, content_reference)

    return metadata


if __name__ == '__main__':
    import sys

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    test_query = {"pmd:TSO": "TERNA",
                  "pmd:timeHorizon": "2D",
                  "pmd:scenarioDate": "2024-02-15T22:30:00Z",
                  }

    test_response = query_data(test_query, return_payload=True)
    logger.info("Test script finished")
