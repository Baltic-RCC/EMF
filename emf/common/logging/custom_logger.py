import sys
import logging
import requests
from emf.common.integrations import elastic
import config
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.logging.custom_logger)

# Define stream logging handler configuration
logging.basicConfig(
    format=LOGGING_FORMAT,
    datefmt=LOGGING_DATEFMT,
    level=LOGGING_LEVEL,
    handlers=[logging.StreamHandler(sys.stdout)]
)


class ElkLoggingHandler(logging.StreamHandler):

    def __init__(self, elk_server=elastic.ELK_SERVER, index=LOGGING_INDEX, extra=None, fields_filter=None):
        """
        Initialize ELK logging handler
        :param elk_server: url of ELK stack server
        :param index: ELK index pattern
        :param extra: additional log field in dict format
        :param fields_filter: fields to filter out in list format, default None - all record attributes will be used
        """
        logging.StreamHandler.__init__(self)
        self.server = elk_server
        self.index = index
        self.extra = extra
        self.fields_filter = fields_filter
        self.connected = self.elk_connection()

    def elk_connection(self):
        try:
            response = requests.get(self.server, timeout=5)
            if response.status_code == 200:
                logger.info(f"Connection to {self.server} successful")
                return True
            else:
                logger.warning(f"ELK server response: [{response.status_code}] {response.reason}. Disabling ELK logging.")
        except requests.exceptions.ConnectTimeout:
            logger.warning(f"ELK server {self.server} does not responding with ConnectTimeout error. Disabling ELK logging.")
        except Exception as e:
            logger.warning(f"ELK server {self.server} returned unknown error: {e}")

    def elk_formatter(self, record):
        elk_record = record.__dict__
        if self.fields_filter:
            elk_record = {key: elk_record[key] for key in self.fields_filter if key in elk_record}

        return elk_record

    def emit(self, record):
        elk_record = self.elk_formatter(record=record)

        # Add extra global attributes from class initiation
        if self.extra:
            elk_record.update(self.extra)

        # Send to Elk
        elastic.Elastic.send_to_elastic(index=self.index, json_message=elk_record, server=self.server)


if __name__ == '__main__':
    # Start root logger
    STREAM_LOG_FORMAT = "%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s"
    logging.basicConfig(stream=sys.stdout,
                        format=STREAM_LOG_FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        )

    # Test ELK custom logger
    index = 'debug-emfos-logs'
    server = "http://test-rcc-logs-master.elering.sise:9200"
    elk_handler = ElkLoggingHandler(elk_server=server, index=index, extra={'time_horizon': '1D'})
    if elk_handler.connected:
        logger.addHandler(elk_handler)
    logger.info(f"Info message", extra={'extra': 'logger testing'})
