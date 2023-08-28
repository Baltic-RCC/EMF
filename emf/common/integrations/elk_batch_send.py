import requests
import logging
import ndjson
import json

logger = logging.getLogger(__name__)


class Handler:

    def __init__(self, url, index, headers=None, auth=None, verify=False):

        if not headers:
            headers = {'Content-Type': 'text/json'}

        self.url = url
        self.index = index

        self.session = requests.Session()
        self.session.verify = verify
        self.session.headers.update(headers)
        self.session.auth = auth

    def send(self, byte_string, properties):

        json_message_list = json.loads(byte_string)
        id_from_metadata = False
        id_metadata_list = ('mRID', 'revisionNumber', 'TimeSeries.mRID', 'position')
        batch_size = 20000  # TODO move to config later

        #TODO add support for properties argument

        # Define url for post method
        url = f"{self.url}/{self.index}/_bulk"
        # print(url)

        if id_from_metadata:
            id_separator = "_"
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": self.index, "_id": id_separator.join([str(element.get(key, '')) for key in id_metadata_list])}}, element)]
        else:
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": self.index}}, element)]

        response_list = []
        for index in range(0, len(json_message_list), batch_size):
            logger.info(f"Sending batch ({index}-{index + batch_size})/{len(json_message_list)} to {self.url}")
            response = self.session.post(url=url,
                                         data=(ndjson.dumps(json_message_list[index:index + batch_size]) + "\n").encode(),
                                         timeout=None,
                                         headers={"Content-Type": "application/x-ndjson"})
            logger.debug(f"ELK response -> {response.content}")
            response_list.append(response.ok)

        return all(response_list)