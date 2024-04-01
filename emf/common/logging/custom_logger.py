import os.path
import sys
import logging
from datetime import datetime, timedelta
from enum import Enum
from io import BytesIO
from zipfile import ZipFile

import requests

from emf.common.integrations import elastic
import config
from emf.common.config_parser import parse_app_properties
from emf.common.integrations.minio import ObjectStorage

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.logging.custom_logger)
PYPOWSYBL_LOGGER = 'powsybl'
PYPOWSYBL_LOGGER_DEFAULT_LEVEL = 1
CUSTOM_LOG_BUFFER_LINE_BREAK = '\r\n'
# Max allowed lifespan of link to file in minio bucket
DAYS_TO_STORE_DATA_IN_MINIO = 7  # Max allowed by Minio
# Default name of the subfolder for storing the results if needed
SEPARATOR_SYMBOL = '/'
WINDOWS_SEPARATOR = '\\'


def save_content_to_zip_file(content: {}):
    """
    Saves content to zip file (in memory)
    :param content: the content of zip file (key: file name, value: file content)
    :return: byte array
    """
    output_object = BytesIO()
    with ZipFile(output_object, "w") as output_zip:
        if content:
            for file_name in content:
                logger.info(f"Converting {file_name} to zip container")
                output_zip.writestr(file_name, content[file_name])
        output_object.seek(0)
    return output_object.getvalue()


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
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_formatter = logging.Formatter(fmt=format, datefmt=datefmt)
    stream_handler.setFormatter(stream_formatter)
    root_logger.addHandler(stream_handler)

    # Configure Elk logging handler
    elk_handler = ElkLoggingHandler(elk_server=elk_server, index=index, extra=extra, fields_filter=fields_filter)

    if elk_handler.connected:
        root_logger.addHandler(elk_handler)
    else:
        logger.warning(f"Elk logging handler not initialized")

    return elk_handler


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
                logger.warning(
                    f"ELK server response: [{response.status_code}] {response.reason}. Disabling ELK logging.")
        except requests.exceptions.ConnectTimeout:
            logger.warning(
                f"ELK server {self.server} does not responding with ConnectTimeout error. Disabling ELK logging.")
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


class LogStream(object):
    """
    Some custom container for storing log related data
    """

    def __init__(self, formatter):
        # self.logs = ''
        self.logs = []
        self.formatter = formatter
        self.single_entry = None

    def write(self, message):
        """
        Writes the log message to the buffer
        :param message: log message
        """
        # formatted_message = self.formatter.format(message)
        # self.logs += formatted_message
        # self.logs += CUSTOM_LOG_BUFFER_LINE_BREAK
        self.logs.append(message)

    def format_for_writing(self):
        """
        Reduces double linebreaks to single linebreaks and some little adjustments
        """
        # double_of_line_break = CUSTOM_LOG_BUFFER_LINE_BREAK + CUSTOM_LOG_BUFFER_LINE_BREAK
        # self.logs.replace(double_of_line_break, CUSTOM_LOG_BUFFER_LINE_BREAK)
        # self.logs = '\n'.join(self.logs.splitlines())
        pass

    def flush(self):
        pass

    def reset(self):
        """
        Resets the internal buffers
        """
        self.logs = []
        self.single_entry = None

    def get_logs(self):
        """
        Gets the content
        :return: tuple of logs and entry that triggered reporting process
        """
        return self.logs, self.single_entry


class PyPowsyblLogReportingPolicy(Enum):
    """
    Some additional reporting types
    """
    """
    Gathers all the pypowsybl output and reports everything when stop_working is called
    """
    ALL_ENTRIES = "all_entries"
    """
    Gathers all the pypowsybl output and reports when at least one entry reached to logging level
    """
    ENTRIES_IF_LEVEL_REACHED = "entries_if_level_was_reached"
    """
    Reports a single logging record that was over the logging level
    """
    ENTRY_ON_LEVEL = "entry_on_level"
    """
    Gathers only entries that are on the level or higher level
    """
    ENTRIES_ON_LEVEL = "entries_on_level_and_higher"
    """
    Reports all collected logging records from the last point when the level was reached
    """
    ENTRIES_COLLECTED_TO_LEVEL = "entries_collected_to_level"


def get_buffer_size(buffer):
    """
    Returns the length of the buffer
    :param buffer: input buffer
    :return: length of the buffer
    """
    return len(buffer.encode('utf-8'))


def check_the_folder_path(folder_path: str):
    """
    Checks folder path for special characters
    :param folder_path: input given
    :return checked folder path
    """
    if not folder_path.endswith(SEPARATOR_SYMBOL):
        folder_path = folder_path + SEPARATOR_SYMBOL
    double_separator = SEPARATOR_SYMBOL + SEPARATOR_SYMBOL
    # Escape '\\'
    folder_path = folder_path.replace(WINDOWS_SEPARATOR, SEPARATOR_SYMBOL)
    # Escape '//'
    folder_path = folder_path.replace(double_separator, SEPARATOR_SYMBOL)
    return folder_path


class PyPowsyblLogGatherer:
    """
    Governing class for the PyPowsyblLogHandler
    Note that for posting the data to elastic, minio the default configuration (elastic.properties, mini.properties)
    is used
    """

    def __init__(self,
                 topic_name: str = None,
                 reporting_level=None,
                 tso: str = None,
                 print_to_console: bool = False,
                 send_to_elastic: bool = True,
                 upload_to_minio: bool = False,
                 report_on_command: bool = True,
                 path_to_local_folder: str = LOCAL_FOLDER_FOR_PYPOWSYBL_LOGS,
                 minio_bucket: str = MINIO_BUCKET_FOR_PYPOWSYBL_LOGS,
                 logging_policy: PyPowsyblLogReportingPolicy = PyPowsyblLogReportingPolicy.ENTRIES_IF_LEVEL_REACHED,
                 elk_server=elastic.ELK_SERVER,
                 index=ELASTIC_INDEX_FOR_PYPOWSYBL_LOGS):
        """
        Initializes the pypowsybl log gatherer.
        :param topic_name: name (string) that can be used to distinguish log files
        :param reporting_level: logging.level which triggers reporting
        :param tso: the name of the tso (for naming the files)
        :param print_to_console: If True then prints the pypowsybl log to console, false: consume it internally
        :param send_to_elastic:  If True then posts a log entry that triggered the gathering to elastic
        :param upload_to_minio:  If True then posts a log buffer to minio as .log file
        :param report_on_command: If true then log entries/buffer are reported when entry has triggered gathering
        and dedicated function is called manually (e.g. report when all validation failed)
        :param minio_bucket: the name of the bucket in minio
        :param logging_policy: determines which and how to collect (entire log or only entries on the level etc)
        :param elk_server: name of the elk server instance
        :index: name of the index in elastic search where to post the log entries
        """
        self.topic_name = topic_name
        self.formatter = logging.Formatter(LOGGING_FORMAT)
        self.package_logger = logging.getLogger(PYPOWSYBL_LOGGER)
        self.package_logger.setLevel(PYPOWSYBL_LOGGER_DEFAULT_LEVEL)
        self.reporting_level = reporting_level
        self.tso = tso
        self.report_on_command = report_on_command
        self.reporting_triggered_externally = True
        self.reset_triggers_for_reporting()
        if self.reporting_level is None:
            self.reporting_level = logging.ERROR
        self.gathering_handler = PyPowsyblLogGatheringHandler(formatter=self.formatter,
                                                              parent=self,
                                                              report_level=self.reporting_level)
        self.package_logger.addHandler(self.gathering_handler)
        # Switch reporting to console on or off
        self.package_logger.propagate = print_to_console
        # Initialize the elk instance
        self.path_to_local_folder = check_the_folder_path(path_to_local_folder)
        self.elastic_server = elk_server
        self.index = index
        self.send_to_elastic = send_to_elastic
        self.report_to = True
        self.logging_policy = None
        self.set_reporting_policy(logging_policy)

        self.minio_instance = None
        self.minio_bucket = minio_bucket
        if upload_to_minio:
            try:
                self.minio_instance = ObjectStorage()
            except Exception as ex:
                # Check the exception
                logger.warning(f"Cannot connect to Minio, staying offline")
        self.identifier = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        # if needed use identifier
        # self.identifier = uuid.uuid4()

    def set_report_on_command(self, report_on_command: bool = False):
        """
        Sets manual reporting status
        if report_on_command is true then reporting happens when self.report_to is true and trigger_to_report_externally
        is called with value true
        if report_on_command is false then reporting (posting the logs) happens when self.report_to is true
        :param report_on_command: new status for manual reporting
        """
        self.report_on_command = report_on_command
        self.reporting_triggered_externally = False if self.report_on_command else True

    def reset_triggers_for_reporting(self):
        """
        Resets the triggers used:
        reporting_to which triggered by the log entry
        reporting_triggered_externally which triggered outside manually
        """
        self.reporting_triggered_externally = False if self.report_on_command else True
        self.report_to = False

    @property
    def elastic_is_connected(self):
        """
        Borrowed from elastic handler above
        Do check up to elastic, handle errors
        """
        try:
            response = requests.get(self.elastic_server, timeout=5)
            if response.status_code == 200:
                return True
            else:
                logger.warning(
                    f"ELK server response: [{response.status_code}] {response.reason}. Disabling ELK logging.")
        except requests.exceptions.ConnectTimeout:
            logger.warning(f"{self.elastic_server}: Timeout. Disabling ELK logging.")
        except Exception as e:
            logger.warning(f"{self.elastic_server}: unknown error: {e}")
        return False

    def post_log_report(self, buffer='', single_entry=None):
        """
        Handles the created report by sending it to elastic or saving it to local storage
        Checks if send_to_elastic is enabled and instance of elastic is available. Composes a message where fields
        are standard field of a logging.Record. Adds the entire log as a string to log_data field
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        """
        try:
            if self.send_to_elastic and self.elastic_is_connected:
                elastic_content = self.compose_elastic_message(buffer, single_entry)
                if elastic_content is not None:
                    response = elastic.Elastic.send_to_elastic(index=self.index,
                                                               json_message=elastic_content,
                                                               server=self.elastic_server)
                    if response.ok:
                        # TODO: Is message pending needed?
                        # For example if sending message failed, keep it somewhere and send it when connection is 
                        # available
                        self.reset_triggers_for_reporting()
                        return
            raise ConnectionError
        except ConnectionError:
            if not self.send_to_elastic:
                logger.info("Saving log to local storage")
            else:
                logger.error(f"Sending log to elastic failed, saving to local storage...")
            self.compose_log_file(buffer, single_entry)
            self.reset_triggers_for_reporting()
        # except Exception:
        #     logger.error(f"Unable to post log report: {Exception}")

    def set_reporting_policy(self, new_policy: PyPowsyblLogReportingPolicy):
        """
        Updates logging policy to new value
        :param new_policy: new policy value
        """
        self.logging_policy = new_policy
        self.gathering_handler.set_reporting_policy(self.logging_policy)
        if self.logging_policy != PyPowsyblLogReportingPolicy.ALL_ENTRIES:
            self.reset_triggers_for_reporting()

    def set_tso(self, tso_name: str):
        """
        Sets the tso to new tso, handles the log of previous tso
        :param tso_name: name of tso
        """
        self.stop_working()
        self.gathering_handler.start_gathering()
        self.tso = tso_name

    def trigger_to_report_externally(self, trigger_reporting: bool = True):
        """
        Calls reporting when self.report_on_command is set to true
        NOTE: That this works on policies which report at the end
        :param trigger_reporting: if true then if self.report_to is true (set by log entry) the log entry/buffers
        are reported otherwise not
        """
        if self.report_on_command:
            self.reporting_triggered_externally = trigger_reporting

    def set_to_reporting(self):
        """
        Handles the logging event, decides whether and what to post:
        Posts if
        1) log entry that reached to level when policy is set to PyPowsyblLogReportingPolicy.ENTRY_ON_LEVEL
        2) log entry and log buffer when policy is set to PyPowsyblLogReportingPolicy.ENTRIES_COLLECTED_TO_LEVEL
        """
        self.report_to = True
        # logger.info(f"{self.topic_name}: {self.get_reporting_level()} from Pypowsybl, setting to report")
        if self.logging_policy == PyPowsyblLogReportingPolicy.ENTRY_ON_LEVEL:
            logger.info(f"Passing at once")
            buffer, single_entry = self.get_logs()
            self.post_log_report(single_entry=single_entry)
        elif self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_COLLECTED_TO_LEVEL:
            logger.info(f"Sending content gathered")
            buffer, single_entry = self.get_logs()
            self.post_log_report(buffer=buffer, single_entry=single_entry)

    def start_working(self):
        """
        Starts gathering the logs
        """
        self.gathering_handler.start_gathering()

    def stop_working(self):
        """
        Stops gathering the logs, retrieves them from buffer and decides whether to post them:
        posts if
        1) self.logging_policy is set to PyPowsyblLogReportingPolicy.ALL_ENTRIES or
        2) self.logging_policy is set to PyPowsyblLogReportingPolicy.ENTRIES_IF_LEVEL_REACHED and level was reached
        (self.report_to is True)
        : return: None
        """
        self.gathering_handler.stop_gathering()
        buffer, single_entry = self.get_logs()
        if (buffer is None or buffer == '') and single_entry is None:
            return
        # Check if post is needed
        # 1. If reporting was set to be triggered externally and no triggering case occurred
        if self.report_on_command is False or self.reporting_triggered_externally is True:
            # 2. If other conditions are met
            if (self.logging_policy == PyPowsyblLogReportingPolicy.ALL_ENTRIES or
                    (self.report_to and
                     (self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_IF_LEVEL_REACHED or
                      self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL))):
                self.post_log_report(buffer, single_entry)
        self.reset_triggers_for_reporting()

    def get_logs(self):
        """
        Gets and formats logs
        """
        log_list, single_log = self.gathering_handler.get_buffer()
        buffer = self.format_buffer_to_string(log_list)
        single_entry = single_log
        return buffer, single_entry

    def format_buffer_to_string(self, buffer):
        """
        Returns log buffer combined to a string
        Note! Be aware of the line break, it is currently set to Windows style!
        """
        return CUSTOM_LOG_BUFFER_LINE_BREAK.join([self.formatter.format(message) for message in buffer])

    def get_reporting_level(self):
        """
        Gets required logging.Loglevel as a string
        : return log level as a string
        """
        return logging.getLevelName(self.reporting_level)

    def compose_log_file(self, buffer: str = '', single_entry: logging.LogRecord = None, file_name: str = None):
        """
        Saves buffer to local log file: buffer if exists, last entry otherwise
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        :param file_name: name of the file where the content should be saved. Note that if not specified, the
        a default file name will be used (combination of topic, tso and date and time of the analysis)
        :return log message dictionary
        """
        file_name = self.check_and_get_file_name(file_name)
        if buffer != '' and buffer is not None:
            payload = '\n'.join(buffer.splitlines())
        elif single_entry is not None:
            payload = self.formatter.format(single_entry)
        else:
            return None
        # And create directories
        directory_name = os.path.dirname(file_name)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)
        with open(file_name, mode='w', encoding="utf-8") as log_file:
            log_file.write(payload)
        return file_name

    def check_and_get_file_name(self, file_name: str = None, use_folders: bool = True, use_local: bool = True):
        """
        Gets some predefined file name to be used when saving the logs
        :param file_name: the input, if exists, leave empty otherwise
        :param use_folders: create sub folders for storing file
        :param use_local: store as a relative path when saving to local computer
        :return file name
        """
        if file_name is None or file_name == '':
            time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
            file_name = f"{self.topic_name}_pypowsybl_error_log_for_{self.tso}_from_{time_moment_now}.log"
        if use_folders:
            file_name = (MINIO_FOLDER_FOR_PYPOWSYBL_LOGS +
                         SEPARATOR_SYMBOL +
                         str(self.identifier) +
                         SEPARATOR_SYMBOL + file_name)
        if use_local:
            file_name = self.path_to_local_folder + file_name
        return file_name

    def post_log_to_minio(self, buffer='', file_name: str = None):
        """
        Posts log as a file to minio
        :param buffer: logs as a string
        :param file_name: if given
        :return: file name and link to file, the link to the file
        """
        link_to_file = None
        if self.minio_instance is not None and buffer != '' and buffer is not None:
            # check if the given bucket exists
            if not self.minio_instance.client.bucket_exists(bucket_name=self.minio_bucket):
                logger.warning(f"{self.minio_bucket} does not exist")
                return link_to_file
            # Adjust the filename to the default n
            file_name = self.check_and_get_file_name(file_name, use_local=False)
            file_object = BytesIO(str.encode(buffer))
            file_object.name = file_name
            self.minio_instance.upload_object(file_path_or_file_object=file_object,
                                              bucket_name=self.minio_bucket,
                                              metadata=None)
            time_to_expire = timedelta(days=DAYS_TO_STORE_DATA_IN_MINIO)
            link_to_file = self.minio_instance.client.get_presigned_url(method="GET",
                                                                        bucket_name=self.minio_bucket,
                                                                        object_name=file_object.name,
                                                                        expires=time_to_expire)
        return file_name, link_to_file

    def compose_elastic_message(self, buffer: str = '', single_entry: logging.LogRecord = None):
        """
        Put together a dictionary consisting of first log entry from the pypowsybl that met response level and the log
        entry for the entire process
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        :return log message dictionary
        """
        message_dict = {}
        # Add first log entry that reached to level as a content of the payload
        if single_entry is not None and isinstance(single_entry, logging.LogRecord):
            message_dict = single_entry.__dict__
        file_name, link_to_log_file = self.post_log_to_minio(buffer=buffer)
        message_dict[ELASTIC_FIELD_FOR_FILENAME] = file_name
        if link_to_log_file != '' and link_to_log_file is not None:
            message_dict[ELASTIC_FIELD_FOR_MINIO_BUCKET] = MINIO_BUCKET_FOR_PYPOWSYBL_LOGS
            message_dict[ELASTIC_FIELD_FOR_LOG_DATA] = link_to_log_file
        message_dict[ELASTIC_FIELD_FOR_TSO] = self.tso
        message_dict[ELASTIC_FIELD_FOR_TOPIC] = self.topic_name
        return message_dict


class PyPowsyblLogGatheringHandler(logging.StreamHandler):
    """
    Initializes custom log handler to start and gather logs.
    Depending on the policy either gathers logs to buffer or looks out for log entry which on the report level or
    does both
    """

    def __init__(self,
                 formatter: logging.Formatter,
                 parent: PyPowsyblLogGatherer = None,
                 logging_policy: PyPowsyblLogReportingPolicy = PyPowsyblLogReportingPolicy.ALL_ENTRIES,
                 report_level=logging.ERROR):
        """
        Constructor:
        :param formatter: the formatter for converting the log entries
        :param parent: the parent to whom to report to
        :logging_policy: check if buffer is needed or not
        :report_level: log level when caught propagates to parent to trigger event
        """
        super().__init__()
        self.parent = parent
        self.gathering = False
        self.originator_type = 'IGM_validation'
        self.formatter = formatter
        if self.formatter is None:
            self.formatter = logging.Formatter(LOGGING_FORMAT)
        self.gathering_buffer = LogStream(self.formatter)
        self.report_level = report_level
        self.logging_policy = None
        self.write_all = False
        self.write_only_levels = False
        self.set_reporting_policy(logging_policy)

    def set_reporting_policy(self, new_policy: PyPowsyblLogReportingPolicy):
        """
        Sets reporting policy to new value
        :param new_policy: new policy value
        """
        self.logging_policy = new_policy
        self.write_all = (self.logging_policy != PyPowsyblLogReportingPolicy.ENTRY_ON_LEVEL and
                          self.logging_policy != PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL)
        self.write_only_levels = self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL

    def emit(self, record: logging.LogRecord) -> None:
        """
        Stores the log output from pypowsybl to internal buffer. Looks for log level as event to trigger reporting
        in parent
        :param record: log record
        """
        if self.gathering:
            # Bypass the buffer if the entire log is not required
            if self.write_all:
                self.gathering_buffer.write(message=record)
            if record.levelno >= self.report_level:
                if self.write_only_levels:
                    self.gathering_buffer.write(message=record)
                self.gathering_buffer.single_entry = record
                self.parent.set_to_reporting()

    def start_gathering(self):
        """
        Resets the buffer to empty and turns gathering on
        :return: None
        """
        self.reset_gathering()
        self.gathering = True

    def stop_gathering(self):
        """
        Stops the gathering, leaves the content to buffer
        :return: None
        """
        self.gathering = False

    def reset_gathering(self):
        """
        Resets the gathering status to default
        :return: None
        """
        self.gathering_buffer.reset()

    def get_buffer(self):
        """
        Returns gathering buffer and last entry, resets the buffer
        :return: log stream instance
        """
        return self.gathering_buffer.get_logs()


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
