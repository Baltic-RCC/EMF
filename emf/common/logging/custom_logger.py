import sys
import logging
import requests
from emf.common.integrations import elastic
import config
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.logging.custom_logger)


def initialize_custom_logger(
        level: str = LOGGING_LEVEL,
        format: str = LOGGING_FORMAT,
        datefmt: str = LOGGING_DATEFMT,
        elk_server: str = elastic.ELK_SERVER,
        index: str = LOGGING_INDEX,
        extra: None | dict = None,
        fields_filter: None | list = None,
        ):

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.propagate = True

    # Configure stream logging handler
    root_logger.addHandler(StreamHandler(level=level, logging_format=format, datetime_format=datefmt))

    # Configure Elk logging handler
    elk_handler = ElkLoggingHandler(elk_server=elk_server, index=index, extra=extra, fields_filter=fields_filter)

    if elk_handler.connected:
        root_logger.addHandler(elk_handler)
    else:
        logger.warning(f"Elk logging handler not initialized")

    #TODO: KV: 2024-06-28 Should be deprecated
    return elk_handler

def get_elk_logging_handler():

    root_logger = logging.getLogger()
    logger.debug(root_logger)
    if root_logger:
        logger.debug(root_logger.handlers)
        for handler in root_logger.handlers:
            if isinstance(handler, ElkLoggingHandler):
                return handler

        logger.warning("ELK handler missing, trying to create one")
        handler = ElkLoggingHandler()
        root_logger.addHandler(handler)
        logger.debug(root_logger.handlers)

    return handler



class StreamHandler(logging.StreamHandler):
    def __init__(self, level=LOGGING_LEVEL, logging_format=LOGGING_FORMAT, datetime_format=LOGGING_DATEFMT):
        super().__init__(sys.stdout)
        self.setLevel(level)
        formatter = logging.Formatter(fmt=logging_format, datefmt=datetime_format)
        self.setFormatter(formatter)


class ElkLoggingHandler(logging.StreamHandler):

    _trace_parameter_names = ['task_id', 'process_id', 'run_id', 'job_id']

    def __init__(self, elk_server=elastic.ELK_SERVER, index=LOGGING_INDEX, extra=None, fields_filter=None):
        """
        Initialize ELK logging handler
        :param elk_server: url of ELK stack server
        :param index: ELK index pattern
        :param extra: additional log field in dict format
        :param fields_filter: fields to filter out in list format, default None - all record attributes will be used
        """
        super().__init__(self)
        self.server = elk_server
        self.index = index

        if extra:
            self.extra = extra
        else:
            self.extra = dict()

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

    # TODO - Move tracing to seperate class, that on destroy will stop tracing?
    def start_trace(self, trace_parameters: dict):
        parameters = trace_parameters.copy()

        if not parameters.get("task_id"):
            parameters["task_id"] = parameters.get('@id')

        for parameter_name in self._trace_parameter_names:
            if parameter_value := parameters.get(parameter_name, None):
                self.extra[parameter_name] = parameter_value
            else:
                logger.warning(f"Trace setup incomplete, missing {parameter_name}")

    def stop_trace(self):
        for parameter_name in self._trace_parameter_names:
            if parameter_name in self.extra:
                del self.extra[parameter_name]





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
