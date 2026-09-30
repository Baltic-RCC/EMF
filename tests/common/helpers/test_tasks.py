import logging
from datetime import datetime
from unittest import mock

import pytest

from emf.common.helpers import tasks
from emf.common.helpers.tasks import get_task_debug_flag, update_task_status
from emf.common.helpers.utils import convert_dict_str_to_bool

NOW = datetime(2025, 6, 10, 16, 50, 30, 123456)


@pytest.fixture
def fixed_utcnow():
    with mock.patch.object(tasks, "datetime") as patched:
        patched.utcnow.return_value = NOW
        yield NOW.isoformat()


@pytest.fixture
def send_to_elastic():
    with mock.patch("emf.common.helpers.tasks.Elastic.send_to_elastic") as send:
        yield send


def test_update_task_status_sets_status_update_time_and_appends_trace(merge_task, fixed_utcnow, send_to_elastic):
    previous_trace = list(merge_task["task_status_trace"])

    update_task_status(merge_task, "started", publish=False)

    assert merge_task["task_status"] == "started"
    assert merge_task["task_update_time"] == fixed_utcnow
    assert merge_task["task_status_trace"] == previous_trace + [{"status": "started", "timestamp": fixed_utcnow}]


def test_update_task_status_keeps_whole_trace_over_several_updates(merge_task, fixed_utcnow, send_to_elastic):
    merge_task["task_status_trace"] = []

    for status in ("created", "started", "finished"):
        update_task_status(merge_task, status, publish=False)

    assert [entry["status"] for entry in merge_task["task_status_trace"]] == ["created", "started", "finished"]
    assert merge_task["task_status"] == "finished"


def test_update_task_status_converts_boolean_strings_in_task_properties(merge_task, fixed_utcnow, send_to_elastic):
    merge_task["task_properties"].update({"upload_to_opdm": " TRUE ", "scaling": "False", "local_import": ["True"]})

    update_task_status(merge_task, "created", publish=False)

    properties = merge_task["task_properties"]
    assert properties["replacement"] is False
    assert properties["post_temp_fixes"] is True
    assert properties["upload_to_opdm"] is True
    assert properties["scaling"] is False
    assert properties["debug"] is False
    assert properties["version"] == "001"
    assert properties["merge_type"] == "EU"
    assert properties["local_import"] == ["True"]


def test_update_task_status_without_publish_sends_nothing(merge_task, fixed_utcnow, send_to_elastic):
    update_task_status(merge_task, "created", publish=False)

    send_to_elastic.assert_not_called()


def test_update_task_status_publishes_task_to_elastic_with_its_id(merge_task, fixed_utcnow, send_to_elastic):
    update_task_status(merge_task, "finished")

    send_to_elastic.assert_called_once_with(index=tasks.TASK_ELK_INDEX, json_message=merge_task, id=merge_task["@id"])
    assert send_to_elastic.call_args.kwargs["json_message"]["task_status"] == "finished"


def test_update_task_status_logs_elastic_failure_instead_of_raising(merge_task, fixed_utcnow, send_to_elastic, caplog):
    send_to_elastic.side_effect = ConnectionError("Elasticsearch unreachable")

    with caplog.at_level(logging.ERROR, logger="emf.common.helpers.tasks"):
        update_task_status(merge_task, "started")

    assert merge_task["task_status"] == "started"
    assert any("Elasticsearch unreachable" in message for message in caplog.messages)


@pytest.mark.parametrize("task, expected", [
    ({"task_properties": {"debug": True}}, True),
    ({"task_properties": {"debug": False}}, False),
    ({"task_properties": {}}, False),
    ({}, False),
])
def test_get_task_debug_flag(task, expected):
    assert get_task_debug_flag(task) is expected


def test_debug_flag_from_string_is_false_after_model_merger_conversion(merge_task):
    # model_merger converts the task with convert_dict_str_to_bool before reading the flag
    assert merge_task["task_properties"]["debug"] == "false"

    assert get_task_debug_flag(convert_dict_str_to_bool(merge_task)) is False


@pytest.mark.parametrize("debug, expected", [("false", False), ("False", False), ("true", True), ("TRUE", True)])
def test_debug_flag_from_string_is_parsed_after_update_task_status(merge_task, fixed_utcnow, send_to_elastic, debug, expected):
    merge_task["task_properties"]["debug"] = debug

    update_task_status(merge_task, "created", publish=False)

    assert get_task_debug_flag(merge_task) is expected
