import logging
from unittest import mock

import pytest
import requests

from emf.common.integrations import elastic
from emf.common.logging import custom_logger

SERVER = "http://elk.test:9200"


@pytest.fixture(autouse=True)
def restore_root_logger():
    root = logging.getLogger()
    handlers, level, factory = root.handlers[:], root.level, logging.getLogRecordFactory()
    console_level = custom_logger.console_handler.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)
    logging.setLogRecordFactory(factory)
    custom_logger.console_handler.setLevel(console_level)


@pytest.fixture
def elk_get():
    with mock.patch.object(custom_logger.requests, "get", return_value=mock.Mock(status_code=200)) as get:
        yield get


@pytest.fixture
def send_to_elastic():
    with mock.patch.object(elastic.Elastic, "send_to_elastic") as send:
        yield send


def _handler(**kwargs):
    return custom_logger.ElkLoggingHandler(elk_server=SERVER, api_key="key", index="emfos-logs", **kwargs)


def _record(msg="Model %s merged", args=("AST",), **extra):
    record = logging.LogRecord("emf.test", logging.INFO, __file__, 10, msg, args, None)
    record.__dict__.update(extra)
    return record


@pytest.mark.parametrize("ssl_verify, verify", [("False", False), ("True", True)])
def test_elk_connection_succeeds_on_http_200(elk_get, monkeypatch, ssl_verify, verify):
    monkeypatch.setenv("ELK_SSL_VERIFY", ssl_verify)

    handler = _handler()

    assert handler.connected is True
    elk_get.assert_called_once_with(SERVER, timeout=5, headers={"Authorization": "ApiKey key"}, verify=verify)


@pytest.mark.parametrize("outcome", [{"return_value": mock.Mock(status_code=503, reason="Unavailable")},
                                     {"side_effect": requests.exceptions.ConnectTimeout("timeout")},
                                     {"side_effect": RuntimeError("unexpected")}], ids=["http-503", "timeout", "other-error"])
def test_elk_connection_failure_disables_handler(outcome, caplog):
    with mock.patch.object(custom_logger.requests, "get", **outcome):
        handler = _handler()

    assert not handler.connected
    assert caplog.records[-1].levelname == "WARNING"


def test_elk_formatter_uses_all_record_fields_with_formatted_message(elk_get):
    elk_record = _handler().elk_formatter(_record(tso="AST"))

    assert elk_record["msg"] == "Model AST merged"
    assert (elk_record["levelname"], elk_record["name"], elk_record["tso"]) == ("INFO", "emf.test", "AST")


def test_elk_formatter_keeps_only_filtered_fields(elk_get):
    handler = _handler(fields_filter=["msg", "levelname", "not_a_field"])

    assert handler.elk_formatter(_record()) == {"msg": "Model AST merged", "levelname": "INFO"}


def test_emit_sends_record_with_handler_extra_without_index_rollover(elk_get, send_to_elastic):
    handler = _handler(extra={"worker": "model-merger", "worker_uuid": "123"})

    handler.emit(_record())

    kwargs = send_to_elastic.call_args.kwargs
    assert (kwargs["index"], kwargs["server"], kwargs["index_rollover"]) == ("emfos-logs", SERVER, False)
    assert kwargs["json_message"]["msg"] == "Model AST merged"
    assert kwargs["json_message"]["worker"] == "model-merger"
    assert kwargs["json_message"]["worker_uuid"] == "123"


@pytest.mark.xfail(strict=True, raises=requests.ConnectionError,
                   reason="ElkLoggingHandler.emit lets send errors propagate into the logging call instead of handleError")
def test_logging_does_not_raise_when_elasticsearch_is_unreachable(elk_get, send_to_elastic):
    send_to_elastic.side_effect = requests.ConnectionError("refused")
    test_logger = logging.getLogger("emf.test.unreachable_elk")
    handler = _handler()
    test_logger.addHandler(handler)
    try:
        with mock.patch.object(handler, "handleError"):
            test_logger.error("Merge failed")
    finally:
        test_logger.removeHandler(handler)


def test_start_trace_sets_trace_fields_and_falls_back_to_task_id_from_at_id(elk_get, caplog):
    handler = _handler(extra={"worker": "validator"})

    handler.start_trace({"@id": "urn:uuid:task", "process_id": "process", "run_id": "run", "other": "ignored"})

    assert handler.extra == {"worker": "validator", "task_id": "urn:uuid:task", "process_id": "process", "run_id": "run"}
    assert "missing job_id" in caplog.text


def test_start_trace_prefers_explicit_task_id(elk_get):
    handler = _handler()

    handler.start_trace({"@id": "urn:uuid:task", "task_id": "explicit", "process_id": "p", "run_id": "r", "job_id": "j"})

    assert handler.extra["task_id"] == "explicit"


def test_stop_trace_removes_only_trace_fields(elk_get):
    handler = _handler(extra={"worker": "validator"})
    handler.start_trace({"task_id": "t", "process_id": "p", "run_id": "r", "job_id": "j"})

    handler.stop_trace()
    handler.stop_trace()

    assert handler.extra == {"worker": "validator"}


@pytest.mark.parametrize("debug, level", [(True, logging.DEBUG), (False, logging.INFO)])
def test_set_console_log_level(debug, level):
    custom_logger.set_console_log_level(debug)

    assert custom_logger.console_handler.level == level


def test_logging_context_token_adds_fields_to_new_records():
    logging.setLogRecordFactory(custom_logger.record_factory)
    make_record = lambda: logging.getLogger("emf.test").makeRecord("emf.test", logging.INFO, __file__, 1, "msg", (), None)

    token = custom_logger.set_logging_context_token(job_id="job", process_id="process", run_id="run",
                                                    scenario_timestamp="2024-05-01T10:30:00Z", task_id="task",
                                                    time_horizon="1D", version="001", merge_type="BA")
    try:
        record = make_record()
    finally:
        custom_logger.log_context.reset(token)

    assert (record.__dict__["@job_id"], record.__dict__["@task_id"], record.__dict__["@time_horizon"],
            record.__dict__["@version"], record.__dict__["merge_type"]) == ("job", "task", "1D", "001", "BA")
    assert "@task_id" not in make_record().__dict__


@pytest.mark.parametrize("status_code, attached", [(200, True), (500, False)])
def test_initialize_custom_logger_attaches_handler_only_when_elk_reachable(send_to_elastic, status_code, attached):
    with mock.patch.object(custom_logger.requests, "get", return_value=mock.Mock(status_code=status_code, reason="error")):
        handler = custom_logger.initialize_custom_logger(elk_server=SERVER, index="emfos-logs", extra={"worker": "test"})

    assert isinstance(handler, custom_logger.ElkLoggingHandler)
    assert handler.extra == {"worker": "test"}
    assert (handler in logging.getLogger().handlers) is attached


def test_get_elk_logging_handler_reuses_existing_handler(real_elk_logging_handler, elk_get, send_to_elastic):
    existing = _handler()
    logging.getLogger().addHandler(existing)

    assert custom_logger.get_elk_logging_handler() is existing
    assert logging.getLogger().handlers.count(existing) == 1


def test_get_elk_logging_handler_creates_and_attaches_missing_handler(real_elk_logging_handler, elk_get, send_to_elastic):
    root = logging.getLogger()
    root.handlers[:] = [h for h in root.handlers if not isinstance(h, custom_logger.ElkLoggingHandler)]

    handler = custom_logger.get_elk_logging_handler()

    assert isinstance(handler, custom_logger.ElkLoggingHandler)
    assert handler in root.handlers
