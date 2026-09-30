import importlib.util
import logging
from datetime import datetime
from unittest import mock

import pytest

from emf.common.integrations import elastic
from emf.common.logging import pypowsybl_logger

Policy = pypowsybl_logger.PyPowsyblLogReportingPolicy
OFFLINE = {"send_to_elastic": False, "upload_to_minio": False, "save_local_storage": False}


@pytest.fixture(autouse=True)
def restore_powsybl_logger():
    package_logger = logging.getLogger(pypowsybl_logger.PYPOWSYBL_LOGGER)
    handlers, level, propagate = package_logger.handlers[:], package_logger.level, package_logger.propagate
    yield
    package_logger.handlers[:] = handlers
    package_logger.setLevel(level)
    package_logger.propagate = propagate


@pytest.fixture
def handler_factory():
    def _handler(**kwargs):
        handler = pypowsybl_logger.PyPowsyblLogGatheringHandler(**{**OFFLINE, **kwargs})
        handler.publisher.post_logs = mock.Mock()
        return handler
    return _handler


def _log(level, message, name=pypowsybl_logger.PYPOWSYBL_LOGGER):
    logging.getLogger(name).log(level, message)


def _posted(handler):
    return [(c.kwargs.get("buffer"), getattr(c.kwargs.get("single_entry"), "msg", None))
            for c in handler.publisher.post_logs.call_args_list]


def test_log_string_stream_collects_lines_and_resets():
    stream = pypowsybl_logger.LogStringStream()
    stream.write("first")
    stream.write("second")
    stream.single_entry = "entry"

    assert stream.get_logs() == ("first\r\nsecond\r\n", "entry")
    stream.reset()
    assert stream.get_logs() == ("", None)


def test_handler_attaches_to_pypowsybl_logger(handler_factory):
    handler = handler_factory(print_to_console=False)

    package_logger = logging.getLogger(pypowsybl_logger.PYPOWSYBL_LOGGER)
    assert handler in package_logger.handlers
    assert package_logger.propagate is False
    assert package_logger.level == pypowsybl_logger.PYPOWSYBL_LOGGER_DEFAULT_LEVEL


def test_all_entries_policy_reports_whole_buffer_at_the_end(handler_factory):
    handler = handler_factory(logging_policy=Policy.ALL_ENTRIES, report_level=logging.ERROR)
    _log(logging.INFO, "loading network")
    _log(logging.WARNING, "voltage out of range")
    handler.publisher.post_logs.assert_not_called()

    handler.stop_gathering()

    [(buffer, single_entry)] = _posted(handler)
    assert "loading network" in buffer and "voltage out of range" in buffer
    assert single_entry is None
    assert handler.get_buffer() == ("", None)


def test_entry_on_level_policy_reports_only_the_triggering_record(handler_factory):
    handler = handler_factory(logging_policy=Policy.ENTRY_ON_LEVEL, report_level=logging.WARNING)
    _log(logging.INFO, "loading network")
    _log(logging.ERROR, "load flow diverged")

    assert _posted(handler) == [(None, "load flow diverged")]
    assert handler.get_buffer() == ("", None)


def test_entries_collected_to_level_policy_reports_buffer_when_level_reached(handler_factory):
    handler = handler_factory(logging_policy=Policy.ENTRIES_COLLECTED_TO_LEVEL, report_level=logging.ERROR)
    _log(logging.INFO, "iteration 1")
    _log(logging.ERROR, "load flow diverged")
    _log(logging.INFO, "iteration 2")

    [(buffer, single_entry)] = _posted(handler)
    assert "iteration 1" in buffer and "load flow diverged" in buffer
    assert single_entry == "load flow diverged"
    assert "iteration 2" in handler.get_buffer()[0]


def test_entries_on_level_policy_buffers_only_records_at_or_above_level(handler_factory):
    handler = handler_factory(logging_policy=Policy.ENTRIES_ON_LEVEL, report_level=logging.WARNING)
    _log(logging.INFO, "iteration 1")
    _log(logging.WARNING, "voltage out of range")

    buffer, _ = handler.get_buffer()
    assert "voltage out of range" in buffer and "iteration 1" not in buffer


def test_stopped_handler_ignores_records(handler_factory):
    handler = handler_factory()
    handler.stop_gathering()
    _log(logging.ERROR, "after stop")

    assert handler.get_buffer() == ("", None)
    handler.publisher.post_logs.assert_not_called()


def test_changing_subtopic_reports_previous_subtopic_logs(handler_factory):
    handler = handler_factory(sub_topic_name="AST")
    _log(logging.INFO, "AST validation")

    handler.set_sub_topic_name("PSE")

    assert "AST validation" in _posted(handler)[0][0]
    assert handler.publisher.subtopic_name == "PSE"


@pytest.mark.xfail(strict=True, reason="PyPowsyblLogGatheringHandler ignores its formatter argument, lines use the default format")
def test_handler_formats_buffer_with_given_formatter(handler_factory):
    handler = handler_factory(formatter=logging.Formatter("%(levelname)s|%(message)s"))
    _log(logging.INFO, "loading network")

    assert handler.get_buffer()[0] == "INFO|loading network\r\n"


@pytest.fixture
def publisher(tmp_path):
    return pypowsybl_logger.PyPowsyblLogGatheringPublisher(topic_name="IGM_validation", subtopic_name="AST",
                                                           send_to_elastic=True, elastic_server="http://elk.test",
                                                           elastic_index="pypowsybl-logs", upload_to_minio=False,
                                                           save_local_storage=False, path_to_local_folder=str(tmp_path))


def _record(message="load flow diverged"):
    return logging.LogRecord(pypowsybl_logger.PYPOWSYBL_LOGGER, logging.ERROR, __file__, 1, message, (), None)


def test_publisher_sends_triggering_record_with_topic_to_elastic(publisher):
    with mock.patch.object(pypowsybl_logger.requests, "get", return_value=mock.Mock(status_code=200)), \
            mock.patch.object(elastic.Elastic, "send_to_elastic", return_value=mock.Mock(ok=True)) as send:
        publisher.post_logs(buffer="line", single_entry=_record())

    kwargs = send.call_args.kwargs
    assert (kwargs["index"], kwargs["server"]) == ("pypowsybl-logs", "http://elk.test")
    assert kwargs["json_message"]["msg"] == "load flow diverged"
    assert (kwargs["json_message"]["topic"], kwargs["json_message"]["tso"]) == ("IGM_validation", "AST")


def test_publisher_skips_elastic_when_unreachable(publisher, caplog):
    with mock.patch.object(pypowsybl_logger.requests, "get", return_value=mock.Mock(status_code=503, reason="down")), \
            mock.patch.object(elastic.Elastic, "send_to_elastic") as send:
        publisher.post_logs(buffer="line", single_entry=_record())

    send.assert_not_called()
    assert "Sending log to elastic failed" in caplog.text


def test_publisher_saves_buffer_to_local_file(publisher, tmp_path):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2024, 5, 1, 10, 30, 0)

    with mock.patch.object(pypowsybl_logger, "datetime", FixedDatetime):
        file_name = publisher.save_log_to_local_storage(buffer="first\r\nsecond")

    assert file_name == f"{tmp_path}/IGM_validation_pypowsybl_log_for_AST_from_01-05-2024_10-30-00.log"
    assert (tmp_path / "IGM_validation_pypowsybl_log_for_AST_from_01-05-2024_10-30-00.log").read_text() == "first\nsecond"


def _module_with_properties(monkeypatch, **env):
    """Fresh copy of pypowsybl_logger with properties overridden via env, the imported module stays untouched"""
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("pypowsybl_logger_with_env", pypowsybl_logger.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.xfail(strict=True, reason="SAVE_PYPOWSYBL_LOG_TO_* properties are used as strings, so 'False' is truthy")
@pytest.mark.parametrize("flag", ["SAVE_PYPOWSYBL_LOG_TO_ELASTIC", "SAVE_PYPOWSYBL_LOG_TO_MINIO",
                                  "SAVE_PYPOWSYBL_LOG_TO_LOCAL_STORAGE"])
def test_disabled_property_turns_destination_off(monkeypatch, tmp_path, flag):
    monkeypatch.chdir(tmp_path)
    module = _module_with_properties(monkeypatch, **{flag: "False"})
    enabled = {"SAVE_PYPOWSYBL_LOG_TO_ELASTIC": "send_to_elastic", "SAVE_PYPOWSYBL_LOG_TO_MINIO": "upload_to_minio",
               "SAVE_PYPOWSYBL_LOG_TO_LOCAL_STORAGE": "save_local_storage"}[flag]
    explicit_off = {key: False for key in OFFLINE if key != enabled}

    with mock.patch.object(module, "ObjectStorage") as object_storage, \
            mock.patch.object(module.requests, "get", return_value=mock.Mock(status_code=200)) as elk_get, \
            mock.patch.object(elastic.Elastic, "send_to_elastic"):
        publisher = module.PyPowsyblLogGatheringPublisher(topic_name="topic", **explicit_off)
        publisher.post_logs(buffer="line", single_entry=_record())

    object_storage.assert_not_called()
    elk_get.assert_not_called()
    assert list(tmp_path.iterdir()) == []
