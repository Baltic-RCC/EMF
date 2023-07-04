import requests
import logging
import ndjson
import settings

logger = logging.getLogger(__name__)


class Handler:

    def __init__(self, target_uri, headers=None, auth=None, verify=False):

        if not headers:
            headers = {'Content-Type': 'text/json'}

        self.target_uri = target_uri

        self.session = requests.Session()
        self.session.verify = verify
        self.session.headers.update(headers)
        self.session.auth = auth

    def send(self, byte_string, properties):

        index = settings.elk_index
        json_message_list = byte_string
        id_from_metadata = False
        id_metadata_list = ('mRID', 'revisionNumber', 'TimeSeries.mRID', 'position')
        url = settings.elk_server
        batch_size = settings.elk_batch_size

        url = f"{url}/{index}/_bulk"
        # print(url)

        if id_from_metadata:
            id_separator = "_"
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index, "_id": id_separator.join([str(element.get(key, '')) for key in id_metadata_list])}}, element)]
        else:
            json_message_list = [value for element in json_message_list for value in ({"index": {"_index": index}}, element)]

        response_list = []
        for index in range(0, len(json_message_list), batch_size):
            logger.info(f"Sending batch ({index}-{index + batch_size})/{len(json_message_list)} to {url}")
            response = self.session.post(url=url,
                                     data=(ndjson.dumps(json_message_list[index:index + batch_size]) + "\n").encode(),
                                     timeout=None,
                                     headers={"Content-Type": "application/x-ndjson"})
            logger.info(f"ELK response -> {response.content}")
            response_list.append(response.ok)

        return all(response_list)
