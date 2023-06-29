import datetime
import requests
import ndjson
import logging
import pandas as pd
from elasticsearch import Elasticsearch
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
warnings.simplefilter('ignore', ElasticsearchWarning)

logger = logging.getLogger(__name__)

class Elk:

    def __init__(self, server, debug=False):
        self.server = server
        self.debug = debug
        self.client = Elasticsearch(self.server)

    @staticmethod
    def send_to_elastic(index,
                        json_message,
                        id=None,
                        server="http://test-rcc-logs-master.elering.sise:9200",
                        iso_timestamp=None,
                        debug=False):
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
        json_message["log_timestamp"] = iso_timestamp

        # Create server url with relevant index pattern
        _index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{_index}/_doc"

        if id:
            url = f"{server}/{id}"

        # Executing POST to push message into ELK
        if debug:
            logger.debug(f"Sending data to {url}")
        response = requests.post(url=url, json=json_message)
        if debug:
            logger.debug(f"ELK response -> {response.content}")

    @staticmethod
    def send_to_elastic_bulk(index,
                             json_message_list,
                             id_from_metadata=False,
                             id_metadata_list=('mRID', 'revisionNumber', 'TimeSeries.mRID', 'position'),
                             server="http://test-rcc-logs-master.elering.sise:9200",
                             batch_size=1000,
                             debug=False):
        """
        Method to send bulk message to ELK
        :param index: index pattern in ELK
        :param json_message_list: list of messages in json format
        :param id_from_metadata:
        :param id_metadata_list:
        :param server: url of ELK server
        :param batch_size: maximum size of batch
        :param debug: flag for debug mode
        :return:
        """

        # Create server url with relevant index pattern
        _index = f"{index}-{datetime.datetime.today():%Y%m}"
        url = f"{server}/{_index}/_bulk"

        if id_from_metadata:
            id_separator = "_"
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index, "_id": id_separator.join([str(element.get(key, '')) for key in id_metadata_list])}}, element)]
        else:
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index}}, element)]

        response_list = []
        for index in range(0, len(json_message_list), batch_size):

            # Executing POST to push messages into ELK
            if debug:
                logger.debug(f"Sending batch ({index}-{index + batch_size})/{len(json_message_list)} to {url}")
            response = requests.post(url=url, data=(ndjson.dumps(json_message_list[index:index + batch_size])+"\n").encode(), timeout=None, headers={"Content-Type": "application/x-ndjson"})
            if debug:
                logger.debug(f"ELK response -> {response.content}")

        return response_list

    def get_doc_by_id(self, index, id):
        response = self.client.get(index=index, id=id)

        return response

    def get_docs_by_query(self, index, query, size=None, return_df=True):

        response = self.client.search(index=index, query=query, size=size)
        if self.debug:
            logger.info(f"Returned total {response['hits']['total']['value']} document")
        response = response['hits']['hits']
        if return_df:
            response = pd.json_normalize(response)
            response.columns = response.columns.astype(str).map(lambda x: x.replace("_source.", ""))

        return response

if __name__ == '__main__':

    # Create client
    server = "http://test-rcc-logs-master.elering.sise:9200"
    service = Elk(server=server)

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
