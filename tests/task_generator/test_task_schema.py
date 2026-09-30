import copy
import itertools
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

import croniter
import jsonschema
import pandas as pd
import pytest
from pytz import timezone

import config
from emf.common.helpers.tasks import update_task_status
from emf.common.helpers.time import reference_times
from emf.task_generator.task_generator import generate_tasks

REPO_ROOT = Path(__file__).resolve().parents[2]
TASK_SCHEMA = json.loads((REPO_ROOT / "emf" / "common" / "schemas" / "task.jsonld").read_text())
PROCESS_CONF = json.loads(config.paths.task_generator.process_conf.read_text())
TIMEFRAME_CONF = json.loads(config.paths.task_generator.timeframe_conf.read_text())
ALL_RUNS = [(process, run) for process in PROCESS_CONF for run in process["runs"]]


def schema_errors(task: dict) -> list[str]:
    return sorted(error.message for error in jsonschema.Draft7Validator(TASK_SCHEMA).iter_errors(task))


def first_task_of_run(process: dict, run: dict) -> dict:
    """First task the run creates at its next scheduled time after 2025-06-01, with Elastic mocked"""
    time_zone = timezone(run.get("time_zone", process["time_zone"]))
    run_time = croniter.croniter(run["run_at"], time_zone.localize(datetime(2025, 6, 1))).get_next(datetime)
    process_conf = [{**copy.deepcopy(process), "runs": [copy.deepcopy(run)]}]
    with mock.patch("emf.task_generator.task_versioning._get_matching_tasks", return_value=pd.DataFrame()), \
            mock.patch("emf.common.helpers.tasks.Elastic.send_to_elastic"):
        tasks = generate_tasks("PT1M", "currentMinuteStart", process_conf, copy.deepcopy(TIMEFRAME_CONF),
                               timetravel_now=(run_time - timedelta(seconds=30)).isoformat())
        return next(itertools.islice(tasks, 1))


def run_param(process: dict, run: dict):
    run_type = run["@id"].rsplit("/runs/", 1)[-1]
    marks = []
    if run_type == "YearAheadCGM":
        marks = [pytest.mark.xfail(strict=True, reason="YearAheadCGM in process_conf.json lacks the required outage_update "
                                                       "property (merger's TaskConfig.from_task raises KeyError)")]
    return pytest.param(process, run, id=run_type, marks=marks)


def test_task_schema_is_a_valid_draft7_schema():
    jsonschema.Draft7Validator.check_schema(TASK_SCHEMA)


@pytest.mark.parametrize("process, run", [run_param(process, run) for process, run in ALL_RUNS])
def test_generated_task_of_every_configured_run_matches_schema(process, run):
    assert schema_errors(first_task_of_run(process, run)) == []


@pytest.mark.xfail(strict=True, reason="examples/merge_task_example.json has debug as the string 'false', "
                                       "the schema (and get_task_debug_flag) expect a boolean")
def test_merge_task_example_matches_schema(merge_task):
    assert schema_errors(merge_task) == []


def test_merge_task_example_matches_schema_after_status_update(merge_task):
    update_task_status(merge_task, "created", publish=False)

    assert schema_errors(merge_task) == []


@pytest.mark.parametrize("change, expected_error", [
    (lambda task: task.pop("task_status_trace"), "'task_status_trace' is a required property"),
    (lambda task: task["task_properties"].pop("outage_update"), "'outage_update' is a required property"),
    (lambda task: task.update({"task_type": "scheduled"}), "'scheduled' is not one of ['automatic', 'manual']"),
    (lambda task: task.update({"task_priority": "urgent"}), "'urgent' is not one of ['low', 'normal', 'high']"),
    (lambda task: task.update({"@id": "84969ee9-16e4-492b-bbd3-928000e5c8c2"}), "does not match"),
    (lambda task: task.update({"task_timeout": "P1D"}), "does not match"),
    (lambda task: task["task_properties"].update({"version": None}), "None is not of type 'string'"),
])
def test_schema_rejects_invalid_tasks(merge_task, change, expected_error):
    merge_task["task_properties"]["debug"] = False
    change(merge_task)

    errors = schema_errors(merge_task)

    assert len(errors) == 1
    assert expected_error in errors[0]


@pytest.mark.parametrize("process, run", ALL_RUNS, ids=[run["@id"].rsplit("/runs/", 1)[-1] for _, run in ALL_RUNS])
def test_every_run_refers_to_a_usable_time_frame(process, run):
    time_frames = {frame["@id"].rsplit("/", 1)[-1]: frame for frame in TIMEFRAME_CONF}

    time_frame = time_frames[run["time_frame"]]

    assert time_frame["reference_time_start"] in reference_times
    assert time_frame["reference_time_end"] in reference_times
    assert croniter.croniter.is_valid(run["run_at"]) and croniter.croniter.is_valid(run["data_timestamps"])
