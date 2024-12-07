import datetime
import requests
import ndjson
import logging
import pandas as pd
import json
from typing import List
from elasticsearch import Elasticsearch
import config
from emf.common.config_parser import parse_app_properties

import warnings
from elasticsearch.exceptions import ElasticsearchWarning
warnings.simplefilter('ignore', ElasticsearchWarning)

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.integrations.elastic)


class Elastic:

    def __init__(self, server: str = ELK_SERVER, debug: bool = False):
        self.server = server
        self.debug = debug
        self.client = Elasticsearch(self.server)

    @staticmethod
    def send_to_elastic(index: str,
                        json_message: dict,
                        id: str = None,
                        server: str = ELK_SERVER,
                        iso_timestamp: str = None,
                        debug: bool = False):
        """
        Method to send single message to ELK
        :param index: index pattern in ELK
        :param json_message: message in json format
        :param id:
        :param server: url of ELK server
        :param iso_timestamp: message timestamp
        :param debug: flag for debug mode
        :return:
        """

        # Creating timestamp value if it is not provided in function call
        if not iso_timestamp:
            iso_timestamp = datetime.datetime.utcnow().isoformat(sep="T")

        # Adding timestamp value to message
        json_message["@timestamp"] = iso_timestamp

        # Create server url with relevant index pattern
        _index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{_index}/_doc"

        if id:
            url = url + f"/{id}"

        # Executing POST to push message into ELK
        if debug:
            logger.debug(f"Sending data to {url}")
        if json_message.get('args', None):  # TODO revise if this is proper solution
            json_message.pop('args')
        json_data = json.dumps(json_message, default=str, ensure_ascii=True, skipkeys=True)
        response = requests.post(url=url, data=json_data.encode(), headers={"Content-Type": "application/json"})
        if debug:
            logger.debug(f"ELK response: {response.content}")

        return response

    @staticmethod
    def send_to_elastic_bulk(index: str,
                             json_message_list: List[dict],
                             id_from_metadata: bool = False,
                             id_metadata_list: List[str] | None = None,
                             server: str = ELK_SERVER,
                             batch_size: int = int(BATCH_SIZE),
                             iso_timestamp: str | None = None,
                             debug: bool = False):
        """
        Method to send bulk message to ELK
        :param index: index pattern in ELK
        :param json_message_list: list of messages in json format
        :param id_from_metadata:
        :param id_metadata_list:
        :param server: url of ELK server
        :param batch_size: maximum size of batch
        :param iso_timestamp: timestamp to be included in documents
        :param debug: flag for debug mode
        :return:
        """

        # Validate if_metadata_list parameter if id_from_metadata is True
        if id_from_metadata and id_metadata_list is None:
            raise Exception(f"Argument id_metadata_list not provided")

        # Creating timestamp value if it is not provided in function call
        if not iso_timestamp:
            iso_timestamp = datetime.datetime.utcnow().isoformat(sep="T")

        # Adding timestamp value to messages
        json_message_list = [{**element, '@timestamp': iso_timestamp} for element in json_message_list]

        # Define server url with relevant index pattern (monthly indication is added)
        index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{index}/_bulk"

        if id_from_metadata:
            id_separator = "_"
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index, "_id": id_separator.join([str(element.get(key, '')) for key in id_metadata_list])}}, element)]
        else:
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index}}, element)]

        response_list = []
        for batch in range(0, len(json_message_list), batch_size):
            # Executing POST to push messages into ELK
            if debug:
                logger.debug(f"Sending batch ({batch}-{batch + batch_size})/{len(json_message_list)} to {url}")
            response = requests.post(url=url,
                                     data=(ndjson.dumps(json_message_list[batch:batch + batch_size])+"\n").encode(),
                                     timeout=None,
                                     headers={"Content-Type": "application/x-ndjson"})
            if debug:
                logger.debug(f"ELK response: {response.content}")
            response_list.append(response.ok)

        return all(response_list)

    def get_doc_by_id(self, index: str, id: str):
        response = self.client.get(index=index, id=id)

        return response

    def get_docs_by_query(self, index: str, query: dict, size: int | None = None, return_df: bool = True):

        response = self.client.search(index=index, query=query, size=size)
        if self.debug:
            logger.info(f"Returned total {response['hits']['total']['value']} document")
        response = response['hits']['hits']
        if return_df:
            response = pd.json_normalize(response)
            response.columns = response.columns.astype(str).map(lambda x: x.replace("_source.", ""))

        return response

    def query_schedules_from_elk(self,
                                 index: str,
                                 utc_start: str,
                                 utc_end: str,
                                 metadata: dict,
                                 period_overlap: bool = False,
                                 latest_by_field: str | None = None) -> pd.DataFrame | None:
        """
        Method to get schedule from ELK by given metadata dictionary
        :param index: index pattern
        :param utc_start: start time in utc. Example: '2023-08-08T23:00:00'
        :param utc_end: end time in utc. Example: '2023-08-09T00:00:00'
        :param metadata: dict of metadata
        :param period_overlap: returns also overlapping periods
        :param latest_by_field: name of the field where filtering to the latest data is applied. If None - returns all data
        :return: dataframe
        """
        # Build Elk query from given start/end times and metadata
        _query_match_list = []
        if period_overlap:
            _query_match_list.append({"range": {"utc_start": {"lte": utc_start}}})
            _query_match_list.append({"range": {"utc_end": {"gte": utc_end}}})
        else:
            _query_match_list.append({"range": {"utc_start": {"gte": utc_start}}})
            _query_match_list.append({"range": {"utc_end": {"lte": utc_end}}})

        for key, val in metadata.items():
            _query_match_list.append({"match": {key: val}})

        query = {"bool": {"must": _query_match_list}}

        # Query documents
        try:
            schedules_df = self.get_docs_by_query(index=index, size=10000, query=query)
            if schedules_df.empty:
                logger.warning(f"No schedules retrieved on query: {query}")
                return None
        except Exception as e:
            logger.warning(f"Query returned error: {e}")
            return None

        # Filtering to only latest data available by given field name
        if latest_by_field:
            pass  # TODO

        return schedules_df


class HandlerSendToElastic:

    def __init__(self,
                 index: str,
                 server: str = ELK_SERVER,
                 id_from_metadata: bool = False,
                 id_metadata_list: List[str] | None = None,
                 headers=None,
                 auth=None,
                 verify=False,
                 debug=False):

        self.index = index
        self.server = server
        self.id_from_metadata = id_from_metadata
        self.id_metadata_list = id_metadata_list
        self.debug = debug

        if not headers:
            headers = {'Content-Type': 'text/json'}

        self.session = requests.Session()
        self.session.verify = verify
        self.session.headers.update(headers)
        self.session.auth = auth

    def handle(self, byte_string, properties):

        Elastic.send_to_elastic_bulk(index=self.index,
                                     json_message_list=json.loads(byte_string),
                                     id_from_metadata=self.id_from_metadata,
                                     id_metadata_list=self.id_metadata_list,
                                     server=self.server,
                                     debug=self.debug)

        # TODO add support for properties argument


if __name__ == '__main__':

    # Create client
    server = "http://test-rcc-logs-master.elering.sise:9200"
    service = Elastic(server=server)

    # Example get documents by query
    # query = {"match": {"scenario_date": "2023-03-21"}}
    # df = service.get_docs_by_query(index='csa-debug', size=200, query=query)

    # Example send document
    # json_message = {'user': 'testuser', 'message': None}
    #
    # try:
    #     Elk.send_to_elastic(index="test", json_message=json_message, server=server, debug=True)
    # except Exception as error:
    #     print(f"Message sending failed with error {error}")
    #     print(json_message)
    #     raise error
