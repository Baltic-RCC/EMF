import io
import logging
import os
from datetime import datetime, timedelta
from enum import Enum
from io import BytesIO

import requests

import config
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic
from emf.common.integrations.minio_api import ObjectStorage
from emf.common.integrations.object_storage.file_system_general import check_the_folder_path, SEPARATOR_SYMBOL

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.logging.pypowsybl_logger)

PYPOWSYBL_LOGGER = 'powsybl'
PYPOWSYBL_LOGGER_DEFAULT_LEVEL = 1
CUSTOM_LOG_BUFFER_LINE_BREAK = '\r\n'
DAYS_TO_STORE_DATA_IN_MINIO = 7  # Max allowed by Minio

ELASTIC_FIELD_FOR_LOG_DATA = 'log_data'
ELASTIC_FIELD_FOR_SUBTOPIC = 'tso'
ELASTIC_FIELD_FOR_TOPIC = 'topic'
ELASTIC_FIELD_FOR_FILENAME = 'log_file_name'
ELASTIC_FIELD_FOR_MINIO_BUCKET = 'minio_bucket'

PY_LOGGING_LEVEL = LOGGING_LEVEL
PY_LOGGING_FORMAT = LOGGING_FORMAT
PY_LOG_TO_LOCAL_STORAGE = SAVE_PYPOWSYBL_LOG_TO_LOCAL_STORAGE
PY_LOCAL_STORAGE_FOLDER = LOCAL_FOLDER_FOR_PYPOWSYBL_LOGS
PY_LOG_TO_ELASTIC = SAVE_PYPOWSYBL_LOG_TO_ELASTIC
PY_LOG_ELASTIC_INDEX = ELASTIC_INDEX_FOR_PYPOWSYBL_LOGS
PY_LOG_TO_MINIO = SAVE_PYPOWSYBL_LOG_TO_MINIO
PY_LOG_MINIO_BUCKET = MINIO_BUCKET_FOR_PYPOWSYBL_LOGS
PY_LOG_MINIO_PATH = MINIO_FOLDER_FOR_PYPOWSYBL_LOGS


class LogStringStream(io.StringIO):
    """
    Some custom container for storing log related data
    """
    def __init__(self):
        super().__init__()
        self.single_entry = None

    def reset(self):
        """
        Resets the internal buffers
        """
        self.single_entry = None
        self.truncate(0)
        self.seek(0)

    def get_logs(self):
        """
        Gets the content
        :return: tuple of logs and entry that triggered reporting process
        """
        return self.getvalue(), self.single_entry

    def write(self, message):
        """
        Writes the log message to the buffer
        :param message: log message
        """
        super().write(message + CUSTOM_LOG_BUFFER_LINE_BREAK)


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


class LoggerWhiteList(logging.Filter):

    """
    Uses a "whitelist" of loggers (name) from which to use logs
    """

    def __init__(self, *whitelist):
        """
        Constructor
        :param whitelist: some list of logger names
        """
        super().__init__()
        self.whitelist = [logging.Filter(name) for name in whitelist]

    def filter(self, record):
        """
        Filters the logs by saved logger names
        """
        return any(log_filter.filter(record) for log_filter in self.whitelist)


class LevelAndHigherFilter(logging.Filter):

    def __init__(self, log_level):
        super().__init__()
        self.log_level = log_level

    def filter(self, record):
        return record.levelno >= self.log_level


class PyPowsyblLogGatheringPublisher:

    def __init__(self,
                 topic_name: str = None,
                 subtopic_name: str = None,
                 send_to_elastic: bool = PY_LOG_TO_ELASTIC,
                 elastic_server=elastic.ELK_SERVER,
                 elastic_index=PY_LOG_ELASTIC_INDEX,
                 upload_to_minio: bool = PY_LOG_TO_MINIO,
                 minio_bucket: str = PY_LOG_MINIO_BUCKET,
                 minio_folder_in_bucket: str = PY_LOG_MINIO_PATH,
                 save_local_storage: bool = PY_LOG_TO_LOCAL_STORAGE,
                 path_to_local_folder: str = PY_LOCAL_STORAGE_FOLDER):
        """
        Initializes the pypowsybl log gatherer.
        :param topic_name: name (string) that can be used to distinguish log files
        :param subtopic_name: the name of the subtopic_name (for naming the files)
        :param send_to_elastic:  If True then posts a log entry that triggered the gathering to elastic
        :param elastic_server: name of the elk server instance
        :param elastic_index: index to where to send elastic report
        :param upload_to_minio:  If True then posts a log buffer to minio as .log file
        :param minio_bucket: the name of the bucket in minio
        :param minio_folder_in_bucket: path if specified in the bucket
        :param save_local_storage: save the file to local storage
        :param path_to_local_folder: where to store log file in local storage
        """
        self.topic_name = topic_name or ''
        self.subtopic_name = subtopic_name
        self.formatter = logging.Formatter(PY_LOGGING_FORMAT)
        self.send_to_elastic = send_to_elastic
        self.elastic_server = elastic_server
        self.elastic_index = elastic_index
        self.upload_to_minio = upload_to_minio
        self.minio_instance = None
        self.minio_bucket = minio_bucket
        self.minio_folder_in_bucket = minio_folder_in_bucket
        if upload_to_minio:
            try:
                self.minio_instance = ObjectStorage()
            except Exception as ex:
                logger.warning(f"Cannot connect to Minio: {ex}, staying offline")
        self.save_local_storage = save_local_storage
        self.path_to_local_folder = check_the_folder_path(path_to_local_folder)
        self.identifier = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

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

    def post_logs(self, buffer=None, single_entry=None):
        """
        Handles the created report by sending it to elastic or saving it to local storage
        Checks if send_to_elastic is enabled and instance of elastic is available. Composes a message where fields
        are standard field of a logging.Record. Adds the entire log as a string to log_data field
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        """
        if buffer:
            buffer = self.format_buffer_to_string(buffer)
        if self.send_to_elastic:
            try:
                if not self.elastic_is_connected:
                    raise ConnectionError
                elastic_content = self.compose_elastic_message(buffer, single_entry)
                response = elastic.Elastic.send_to_elastic(index=self.elastic_index,
                                                           json_message=elastic_content,
                                                           server=self.elastic_server)
                if not response.ok:
                    raise ConnectionError
            except ConnectionError:
                logger.error(f"Sending log to elastic failed")
        if self.save_local_storage:
            self.save_log_to_local_storage(buffer, single_entry)

    def format_record(self, record: logging.LogRecord):
        return self.formatter.format(record)

    def format_buffer_to_string(self, buffer):
        """
        Returns log buffer combined to a string
        Note! Be aware of the line break, it is currently set to Windows style!
        """
        if isinstance(buffer, list):
            return CUSTOM_LOG_BUFFER_LINE_BREAK.join([self.formatter.format(message) for message in buffer])
        else:
            return buffer

    def save_log_to_local_storage(self,
                                  buffer: str = '',
                                  single_entry: logging.LogRecord = None,
                                  file_name: str = None):
        """
        Saves buffer to local log file: buffer if exists, last entry otherwise
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        :param file_name: name of the file where the content should be saved. Note that if not specified,
         default file name will be used (combination of topic, subtopic_name and date and time of the analysis)
        :return log message dictionary
        """
        file_name = self.check_and_get_file_name(file_name)
        if self.path_to_local_folder:
            file_name = self.path_to_local_folder + file_name
        if buffer:
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

    def check_and_get_file_name(self, file_name: str = None):
        """
        Gets some predefined file name to be used when saving the logs
        :param file_name: the input, if exists, leave empty otherwise
        :return file name
        """
        if file_name is None or file_name == '':
            time_moment_now = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
            sub_topic = self.subtopic_name or ''
            file_name = f"{self.topic_name}_pypowsybl_log_for_{sub_topic}_from_{time_moment_now}.log"
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
            file_name = self.check_and_get_file_name(file_name)
            if self.minio_folder_in_bucket:
                file_name = (self.minio_folder_in_bucket +
                             SEPARATOR_SYMBOL +
                             str(self.identifier) +
                             SEPARATOR_SYMBOL + file_name)
            # buffer = self.format_buffer_to_string(buffer)
            file_object = BytesIO(str.encode(buffer))
            file_object.name = file_name
            self.minio_instance.upload_object(file_path_or_file_object=file_object,
                                              bucket_name=self.minio_bucket)
            time_to_expire = timedelta(days=DAYS_TO_STORE_DATA_IN_MINIO)
            link_to_file = self.minio_instance.client.get_presigned_url(method="GET",
                                                                        bucket_name=self.minio_bucket,
                                                                        object_name=file_object.name,
                                                                        expires=time_to_expire)
        return file_name, link_to_file

    def compose_elastic_message(self,
                                buffer: str = '',
                                single_entry: logging.LogRecord = None):
        """
        Put together a dictionary consisting of first log entry from the pypowsybl that met response level and the log
        entry for the entire process
        :param buffer: buffer containing log entries
        :param single_entry: first entry that reached to required level
        : return log message dictionary
        """
        message_dict = {}
        # Add first log entry that reached to level as a content of the payload
        if single_entry is not None and isinstance(single_entry, logging.LogRecord):
            message_dict = single_entry.__dict__
        if self.upload_to_minio:
            file_name, link_to_log_file = self.post_log_to_minio(buffer=buffer)
            message_dict[ELASTIC_FIELD_FOR_FILENAME] = file_name
            if link_to_log_file != '' and link_to_log_file is not None:
                message_dict[ELASTIC_FIELD_FOR_MINIO_BUCKET] = self.minio_bucket
                message_dict[ELASTIC_FIELD_FOR_LOG_DATA] = link_to_log_file
        if self.topic_name and self.topic_name != '':
            message_dict[ELASTIC_FIELD_FOR_TOPIC] = self.topic_name
        if self.subtopic_name and self.subtopic_name != '':
            message_dict[ELASTIC_FIELD_FOR_SUBTOPIC] = self.subtopic_name
        return message_dict


class PyPowsyblLogGatheringHandler(logging.StreamHandler):
    """
    Initializes custom log handler to start and gather logs.
    Depending on the policy either gathers logs to buffer or looks out for log entry which on the report level or
    does both
    """

    def __init__(self,
                 formatter: logging.Formatter = None,
                 logging_policy: PyPowsyblLogReportingPolicy = PyPowsyblLogReportingPolicy.ALL_ENTRIES,
                 report_level=logging.INFO,
                 print_to_console: bool = False,
                 topic_name: str = 'UNNAMED',
                 sub_topic_name: str = None,
                 send_to_elastic: bool = PY_LOG_TO_ELASTIC,
                 elastic_server=elastic.ELK_SERVER,
                 elastic_index=PY_LOG_ELASTIC_INDEX,
                 upload_to_minio: bool = PY_LOG_TO_MINIO,
                 minio_bucket: str = PY_LOG_MINIO_BUCKET,
                 minio_folder_in_bucket: str = PY_LOG_MINIO_PATH,
                 save_local_storage: bool = PY_LOG_TO_LOCAL_STORAGE,
                 path_to_local_folder: str = PY_LOCAL_STORAGE_FOLDER):
        """
        Constructor:
        :param formatter: the formatter for converting the log entries
        :param logging_policy: check if buffer is needed or not
        :param report_level: log level when caught propagates to parent to trigger event
        :param print_to_console: propagate the log further
        :param topic_name: name (string) that can be used to distinguish log files
        :param sub_topic_name: the name of the subtopic_name (for naming the files)
        :param send_to_elastic:  If True then posts a log entry that triggered the gathering to elastic
        :param elastic_server: name of the elk server instance
        :param elastic_index: index to where to send elastic report
        :param upload_to_minio:  If True then posts a log buffer to minio as .log file
        :param minio_bucket: the name of the bucket in minio
        :param minio_folder_in_bucket: path if specified in the bucket
        :param save_local_storage: save the file to local storage
        :param path_to_local_folder: where to store log file in local storage
        """

        self.gathering = False
        self.level_reached = False
        self.formatter = formatter or logging.Formatter(PY_LOGGING_FORMAT)
        self.gathering_buffer = LogStringStream()
        self.level_filters = []
        super().__init__(stream=self.gathering_buffer)
        self.report_level = report_level
        self.logging_policy = None
        self.write_all = False
        self.write_only_levels = False
        self.set_reporting_policy(logging_policy)

        self.publisher = PyPowsyblLogGatheringPublisher(topic_name=topic_name,
                                                        subtopic_name=sub_topic_name,
                                                        send_to_elastic=send_to_elastic,
                                                        elastic_server=elastic_server,
                                                        elastic_index=elastic_index,
                                                        upload_to_minio=upload_to_minio,
                                                        minio_bucket=minio_bucket,
                                                        minio_folder_in_bucket=minio_folder_in_bucket,
                                                        save_local_storage=save_local_storage,
                                                        path_to_local_folder=path_to_local_folder)
        self.gathering = True
        # atexit.register(self.report_at_the_end)
        # signal.signal(signal.SIGTERM, self.report_at_the_end)
        # signal.signal(signal.SIGINT, self.report_at_the_end)
        # # Set up the pypowsybl logger
        package_logger = logging.getLogger(PYPOWSYBL_LOGGER)

        if package_logger:
            package_logger.setLevel(PYPOWSYBL_LOGGER_DEFAULT_LEVEL)
            package_logger.propagate = print_to_console
            package_logger.addHandler(self)
            self.addFilter(LoggerWhiteList(PYPOWSYBL_LOGGER))
        else:
            self.gathering = False

    def add_level_filter(self, logging_level: PyPowsyblLogReportingPolicy):
        """
        Add level filters, currently higher end (level or higher) is implemented
        """
        if logging_level == PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL:
            new_level_filter = LevelAndHigherFilter(log_level=self.report_level)
            self.level_filters.append(new_level_filter)
            self.addFilter(new_level_filter)

    def remove_level_filter(self):
        for level_filter in self.level_filters:
            if level_filter in self.filters:
                self.filters.remove(level_filter)

    def close(self):
        super().close()
        self.report_at_the_end()

    def set_reporting_policy(self, new_policy: PyPowsyblLogReportingPolicy):
        """
        Sets reporting policy to new value
        :param new_policy: new policy value
        """
        self.logging_policy = new_policy
        self.write_all = (self.logging_policy != PyPowsyblLogReportingPolicy.ENTRY_ON_LEVEL and
                          self.logging_policy != PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL)
        self.remove_level_filter()
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
                self.gathering_buffer.write(message=self.publisher.format_record(record=record))
            if record.levelno >= self.report_level:
                if self.write_only_levels:
                    self.gathering_buffer.write(message=self.publisher.format_record(record=record))
                self.gathering_buffer.single_entry = record
                self.report_on_emit()

    def post_logs(self):
        """
        Posts logs from buffer to set destinations
        """
        buffer, single_entry = self.get_buffer()
        self.publisher.post_logs(buffer=buffer, single_entry=single_entry)
        self.reset_gathering()

    def report_on_emit(self):
        """
        Reports during emission deciding by PyPowsyblLogReportingPolicy
        """
        if self.logging_policy == PyPowsyblLogReportingPolicy.ENTRY_ON_LEVEL:
            buffer, single_entry = self.get_buffer()
            self.publisher.post_logs(single_entry=single_entry)
            self.reset_gathering()
        elif self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_COLLECTED_TO_LEVEL:
            self.post_logs()

    def report_at_the_end(self):
        """
        Reports at the end of the script or changing of topic or stopping the gathering
        """
        buffer, single_entry = self.get_buffer()
        if (buffer is None or buffer == '') and single_entry is None:
            return
        if (single_entry or
                self.logging_policy == PyPowsyblLogReportingPolicy.ALL_ENTRIES or
                self.logging_policy == PyPowsyblLogReportingPolicy.ENTRIES_ON_LEVEL):
            self.publisher.post_logs(buffer=buffer, single_entry=single_entry)
        self.reset_gathering()

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
        self.report_at_the_end()

    def reset_gathering(self):
        """
        Resets the gathering status to default
        :return: None
        """
        self.gathering_buffer.reset()
        # When needed to speed up create new instance rather than emptying it
        # self.gathering_buffer = LogStringStream()

    def get_buffer(self):
        """
        Returns gathering buffer and last entry, resets the buffer
        :return: log stream instance
        """
        return self.gathering_buffer.get_logs()

    def set_sub_topic_name(self, new_subtopic_name: str):
        """
        Changes the subtopic name that publisher uses for file names elastic logs etc. (for example save output
        from each tso igm validation to separate file)
        :param new_subtopic_name: name of the subtopic
        """
        self.report_at_the_end()
        self.publisher.subtopic_name = new_subtopic_name

    def set_topic_name(self, new_topic_name: str):
        """
        Changes the topic name that publisher uses for file names elastic logs etc. (for example IGM_validation,
        CGM_creation, loadflow etc.)
        :param new_topic_name: name of the topic
        """
        self.report_at_the_end()
        self.publisher.topic_name = new_topic_name


def get_pypowsybl_log_handler():
    """
    Gets the PyPowsyblLogGatheringHandler from root if exists, none otherwise
    """
    handlers = logging.getLogger().handlers
    for handler in handlers:
        if isinstance(handler, PyPowsyblLogGatheringHandler):
            return handler
    return None
