from emf.common.integrations import object_storage
from aniso8601 import parse_datetime
import logging
import pandas
import sys
from emf.common.integrations import opdm

logger = logging.getLogger(__name__)


def query_data(metadata_query: dict,
               query_filter: str | None = None,
               index: str = object_storage.ELASTIC_MODELS_INDEX,
               return_payload: bool = False,
               size: str = '10000',
               sort: dict | None = None,
               scroll: str = '1m',
               ):
    """
    Queries Elasticsearch based on provided metadata queries.

    Args:
        metadata_query (dict): A dictionary containing metadata fields and their values to be queried.
        query_filter (dict): Optional. A dictionary specifying parameters by which to filter the query.
        index (str): The index to query data from. Defaults to ELASTIC_MODELS_INDEX from config variables.
        return_payload (bool): Optional. If True, retrieves the full content for each hit.
            Defaults to False.

    Returns:
        list: A list of dictionaries containing the retrieved content from Elasticsearch.

    Note:
        The function constructs an Elasticsearch query based on the provided metadata_query.
        It retrieves data from the specified index and processes the response to extract content.

    Example:
        To query data with metadata fields 'TSO' and 'timeHorizon' and return payload:
            metadata_query = {"pmd:TSO": "TERNA", "pmd:timeHorizon": "2D"}
            response = query_data(metadata_query, return_payload=True)
    """

    # Create elastic query syntax
    # {
    #     "bool": {
    #         "must": [
    #             {"match": {"pmd:TSO": "TERNA"}},
    #             {"terms": {"pmd:timeHorizon": ["01", "02"]}}
    #         ]
    #     }
    # }

    # Validate index definition to be able to search all index by pattern
    if "*" not in index:
        index = f"{index}*"

    match_and_term_list = []
    for key, value in metadata_query.items():
        if isinstance(value, list):
            match_and_term_list.append({"terms": {key: value}})
        else:
            match_and_term_list.append({"match": {key: value}})

    if query_filter:
        query = {"bool": {"must": match_and_term_list, "filter": {"range": {"pmd:creationDate": {"gte": query_filter}}}}}
    else:
        query = {"bool": {"must": match_and_term_list}}

    # Return query results
    response = object_storage.elastic_service.client.search(index=index, query=query, size=size, sort=sort, scroll=scroll)
    scroll_id = response['_scroll_id']
    hits = response["hits"]["hits"]
    content_list = [content["_source"] for content in hits]
    while len(hits) > 0:
        response = object_storage.elastic_service.client.scroll(scroll_id=scroll_id, scroll=scroll)
        hits = response["hits"]["hits"]
        if hits:
            content_list.extend([content["_source"] for content in hits])

    if return_payload:
        for num, item in enumerate(content_list):
            content_list[num] = get_content(item)

    # Delete scroll after retrieving data
    object_storage.elastic_service.client.clear_scroll(scroll_id=scroll_id)

    return content_list


def get_content(metadata: dict):
    """
    Retrieves content data from MinIO based on metadata information.

    Args:
        metadata (dict): A dictionary containing metadata information.

    Returns:
        list: A list of dictionaries representing content components with updated 'DATA' field.

    Note:
        It expects metadata to contain 'opde:Component' information.
        For each component, it downloads data from MinIO and updates the 'DATA' field in the component dictionary.
    """

    logger.info(f"Getting content of metadata object from MinIO: {metadata['opde:Id']}")
    bucket_name = metadata.get("minio-bucket", "opdm-data")  # by default use "opdm-data" bucket if missing in meta
    logger.debug(f"S3 storage bucket used: {bucket_name}")
    components_received = []
    for component in metadata["opde:Component"]:
        content_reference = component.get("opdm:Profile").get("pmd:content-reference")
        logger.info(f"Downloading object: {content_reference}")
        content = object_storage.minio_service.download_object(bucket_name, content_reference)
        component["opdm:Profile"]["DATA"] = content
        components_received.append(bool(content))  # collect boolean flags of received components

    if not all(components_received):  # at least one is False
        logger.warning(f"[FALLBACK] At least some content did not exist in MinIO storage, requesting from OPDM...")
        metadata = opdm.OPDM().download_object(metadata)  # TODO maybe to make OPDM connection instance globally

    return metadata


def get_latest_boundary():

    logger.info(f"Retrieving latest boundary set")

    # Query data from ELK
    boundaries = query_data({"opde:Object-Type.keyword": "BDS"})

    # Convert to dataframe for sorting out the latest boundary
    boundary_data = pandas.DataFrame(boundaries)

    # Convert date and version to respective formats
    boundary_data['date_time'] = pandas.to_datetime(boundary_data['pmd:scenarioDate'], format='ISO8601')
    boundary_data['version'] = pandas.to_numeric(boundary_data['pmd:versionNumber'])

    # Sort out official boundary
    official_boundary_data = boundary_data[boundary_data["opde:Context"] == {'opde:IsOfficial': 'true'}]

    # Get the latest boundary meta
    latest_boundary_meta = boundaries[list(official_boundary_data.sort_values(["date_time", "version"], ascending=False).index)[0]]

    # Download the latest boundary
    return get_content(metadata=latest_boundary_meta)


def get_latest_models_and_download(time_horizon: str,
                                   scenario_date: str,
                                   valid: bool = True,
                                   tso: str | None = None,
                                   object_type: str = 'IGM',
                                   data_source: str | None = None
                                   ):

    logger.info(f"Retrieving latest network models of type: {object_type}")

    meta = {'pmd:validFrom': f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}",
            'pmd:timeHorizon': time_horizon,
            'opde:Object-Type': object_type}

    if tso:
        meta['pmd:TSO'] = tso

    if valid:
        meta["valid"] = valid

    if data_source:
        meta["data-source"] = data_source

    if time_horizon.upper() == "ID":
        meta['pmd:timeHorizon'] = [f"{i:02d}" for i in range(1, 32)]
        # TODO - This is not a nice solution, needs to be moved to somewhere more close to business function as this can change

    models_metadata_raw = query_data(metadata_query=meta, return_payload=False)

    models_downloaded = []

    if models_metadata_raw:
        # Sort for highest timeHorizon (for intraday) and for highest version
        models = pandas.DataFrame(models_metadata_raw)
        latest_models = models.sort_values(["pmd:timeHorizon", "pmd:versionNumber"], ascending=[True, False]).groupby("pmd:modelPartReference").first()

        for model in latest_models.to_dict("records"):
            try:
                models_downloaded.append(get_content(metadata=model))
            except:
                logger.error(f"Could not download model for {time_horizon} {scenario_date} {model['pmd:TSO']}")
                logger.error(sys.exc_info())
    else:
        logger.warning(f"Models not available on Object Storage")

    return models_downloaded


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    test_query = {"pmd:TSO": "TERNA",
                  "pmd:timeHorizon": "2D",
                  "pmd:scenarioDate": "2025-02-15T22:30:00Z",
                  }
    test_filter = "now-2w"
    test_response = query_data(test_query, query_filter=test_filter, return_payload=True)

    #models = get_latest_models_and_download("1D", '20240526T1530Z', valid=False)
    models = get_latest_models_and_download("ID", '20240522T1530Z', valid=True)
    bds = get_latest_boundary()
    logger.info("Test script finished")