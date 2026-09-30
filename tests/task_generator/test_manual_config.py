import configparser
import copy
import json
from datetime import datetime, timezone
from unittest import mock

import pytest

import config
from emf.task_generator import manual_config
from emf.task_generator.manual_config import build_manual_run_config, select_run, select_timeframe
from emf.task_generator.task_generator import generate_tasks

PROCESS_CONF = json.loads(config.paths.task_generator.process_conf.read_text())
TIMEFRAME_CONF = json.loads(config.paths.task_generator.timeframe_conf.read_text())


@pytest.fixture
def settings():
    """Shipped task_generator.properties values, as worker.py's globals() without env overrides"""
    properties = configparser.RawConfigParser()
    properties.optionxform = str
    properties.read(config.paths.task_generator.task_generator)
    return dict(properties.items("MAIN"))


@pytest.fixture
def freeze_now(monkeypatch):
    def _freeze_now(utc_now: datetime):
        class FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return utc_now.astimezone(tz) if tz else utc_now.replace(tzinfo=None)
        monkeypatch.setattr(manual_config, "datetime", FrozenDatetime)
    return _freeze_now


def build(settings, **overrides):
    return build_manual_run_config(copy.deepcopy(PROCESS_CONF), copy.deepcopy(TIMEFRAME_CONF), {**settings, **overrides})


def generate(process_conf, timeframe_conf, timestamp):
    with mock.patch("emf.task_generator.task_generator.set_task_version"), \
            mock.patch("emf.task_generator.task_generator.update_task_status"):
        return list(generate_tasks("PT1M", "currentMinuteStart", process_conf, timeframe_conf, timestamp, task_type="manual"))


@pytest.mark.parametrize("run_type, expected_process", [
    ("IntraDayCGM/1", "CGM_CREATION"),
    ("IntraDayRMM/EOD", "RMM_CREATION"),
    ("DayAheadCGM", "CGM_CREATION"),
    ("TwoDaysAheadCGM", "CGM_CREATION"),
])
def test_select_run_matches_run_type_exactly(run_type, expected_process):
    process, run = select_run(PROCESS_CONF, run_type)

    assert run["@id"] == f"https://example.com/runs/{run_type}"
    assert process["@id"] == f"https://example.com/processes/{expected_process}"


@pytest.mark.parametrize("run_type", ["IntraDayCGM", "DayAhead", "CGM", "dayaheadcgm", "IntraDayCGM/1 ", ""])
def test_select_run_rejects_partial_matches(run_type):
    with pytest.raises(ValueError, match="No run found"):
        select_run(PROCESS_CONF, run_type)


@pytest.mark.parametrize("time_frame", ["D-1", "H-8", "H-16", "ID", "Y-1"])
def test_select_timeframe_matches_exactly(time_frame):
    assert select_timeframe(TIMEFRAME_CONF, time_frame)["@id"] == f"https://example.com/timeHorizons/{time_frame}"


@pytest.mark.parametrize("time_frame", ["D", "H-", "d-1", "H-2", ""])
def test_select_timeframe_rejects_partial_matches(time_frame):
    with pytest.raises(ValueError, match="No timeframe configuration found"):
        select_timeframe(TIMEFRAME_CONF, time_frame)


def test_manual_config_is_trimmed_to_the_selected_run_firing_every_minute(settings, freeze_now):
    freeze_now(datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc))

    process_conf, timeframe_conf, _ = build(settings, RUN_TYPE="DayAheadRMM")

    assert len(process_conf) == 1
    assert process_conf[0]["@id"] == "https://example.com/processes/RMM_CREATION"
    assert process_conf[0]["time_zone"] == "Europe/Brussels"
    assert [run["@id"] for run in process_conf[0]["runs"]] == ["https://example.com/runs/DayAheadRMM"]
    assert process_conf[0]["runs"][0]["run_at"] == "* * * * *"
    assert [frame["@id"] for frame in timeframe_conf] == ["https://example.com/timeHorizons/D-1"]


def test_tso_lists_are_split_and_always_overwritten(settings, freeze_now):
    freeze_now(datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc))

    process_conf, _, _ = build(settings, RUN_TYPE="DayAheadRMM", INCLUDED_TSO="AST, PSE ,ELERING", EXCLUDED_TSO="LITGRID",
                               LOCAL_IMPORT="", REPLACE_TSO="")

    properties = process_conf[0]["runs"][0]["properties"]
    assert properties["included"] == ["AST", "PSE", "ELERING"]
    assert properties["excluded"] == ["LITGRID"]
    assert properties["local_import"] == []  # configured default is ["LITGRID"]
    assert properties["replace_tso"] == []


@pytest.mark.parametrize("task_version", ["", "5"])
def test_task_version_always_overwrites_configured_version(settings, freeze_now, task_version):
    freeze_now(datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc))

    process_conf, _, _ = build(settings, RUN_TYPE="DayAheadRMM", TASK_VERSION=task_version)

    assert process_conf[0]["runs"][0]["properties"]["version"] == task_version


@pytest.mark.parametrize("setting_key, property_key", [
    ("RUN_REPLACEMENT", "replacement"),
    ("RUN_SCALING", "scaling"),
    ("OUTAGE_UPDATE", "outage_update"),
    ("FORCE_OUTAGE_FIX", "force_outage_fix"),
    ("UPLOAD_TO_OPDM", "upload_to_opdm"),
    ("UPLOAD_TO_MINIO", "upload_to_minio"),
    ("SEND_MERGE_REPORT", "send_merge_report"),
    ("POST_TEMP_FIXES", "post_temp_fixes"),
    ("LVL8_REPORTING", "lvl8_reporting"),
    ("TASK_MERGING_ENTITY", "merging_entity"),
    ("DEBUG", "debug"),
])
def test_passthrough_settings_overwrite_run_properties_only_when_set(settings, freeze_now, setting_key, property_key):
    freeze_now(datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc))
    configured = select_run(PROCESS_CONF, "DayAheadRMM")[1]["properties"]

    set_conf, _, _ = build(settings, RUN_TYPE="DayAheadRMM", **{setting_key: "OVERRIDE"})
    blank_conf, _, _ = build(settings, RUN_TYPE="DayAheadRMM", **{setting_key: ""})

    assert set_conf[0]["runs"][0]["properties"][property_key] == "OVERRIDE"
    assert blank_conf[0]["runs"][0]["properties"].get(property_key) == configured.get(property_key)


@pytest.mark.parametrize("reference_time, expected_timestamp_utc", [
    ("currentHourStart", "2025-06-30T21:30"),
    ("currentDayStart", "2025-06-29T22:30"),
])
def test_timestamp_override_creates_a_single_hour_task(settings, reference_time, expected_timestamp_utc):
    process_conf, timeframe_conf, timestamp = build(settings, RUN_TYPE="DayAheadCGM", TIMESTAMP="2025-06-30T23:05+0200",
                                                    TASK_REFERENCE_TIME=reference_time)

    assert timestamp == "2025-06-30T23:05:01+02:00"
    assert {key: timeframe_conf[0][key] for key in ("reference_time_start", "reference_time_end", "period_start", "period_end")} == {
        "reference_time_start": reference_time, "reference_time_end": reference_time, "period_start": "PT0M", "period_end": "PT1H"}
    tasks = generate(process_conf, timeframe_conf, timestamp)
    assert [task["task_properties"]["timestamp_utc"] for task in tasks] == [expected_timestamp_utc]


@pytest.mark.parametrize("run_type", ["IntraDayRMM/EOD", "WeekAheadRMM", "MonthAheadRMM"])
def test_now_reference_run_types_use_current_time(settings, freeze_now, run_type):
    freeze_now(datetime(2025, 6, 10, 8, 15, 42, tzinfo=timezone.utc))

    _, _, timestamp = build(settings, RUN_TYPE=run_type)

    assert timestamp == "2025-06-10T10:15:01+02:00"


@pytest.mark.parametrize("run_type, utc_now, expected_timestamp", [
    ("DayAheadCGM", datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc), "2025-06-10T18:50:01+02:00"),
    ("DayAheadRMM", datetime(2025, 1, 15, 20, 15, tzinfo=timezone.utc), "2025-01-15T18:00:01+01:00"),
    ("DayAheadCGM", datetime(2025, 3, 30, 18, 15, tzinfo=timezone.utc), "2025-03-30T18:50:01+02:00"),
    ("IntraDayCGM/2", datetime(2025, 6, 10, 8, 15, tzinfo=timezone.utc), "2025-06-10T07:05:01+02:00"),
    ("IntraDayCGM/1", datetime(2025, 6, 10, 8, 15, tzinfo=timezone.utc), "2025-06-09T23:05:01+02:00"),
    ("IntraDayRMM/1", datetime(2025, 6, 10, 8, 15, tzinfo=timezone.utc), "2025-06-09T23:00:01+02:00"),
    ("IntraDayCGM/1", datetime(2025, 10, 26, 8, 15, tzinfo=timezone.utc), "2025-10-25T23:05:01+02:00"),
])
def test_scheduled_run_types_reconstruct_the_last_scheduled_slot(settings, freeze_now, run_type, utc_now, expected_timestamp):
    freeze_now(utc_now)

    _, _, timestamp = build(settings, RUN_TYPE=run_type)

    assert timestamp == expected_timestamp


@pytest.mark.xfail(strict=True, reason="previous day start is computed with replace() on the DST day's offset, "
                                       "so on 2025-03-30 the late-night slot resolves to 2025-03-28 instead of 2025-03-29")
def test_day_shift_run_type_on_spring_dst_day_uses_previous_day_slot(settings, freeze_now):
    freeze_now(datetime(2025, 3, 30, 8, 15, tzinfo=timezone.utc))

    _, _, timestamp = build(settings, RUN_TYPE="IntraDayCGM/1")

    assert timestamp == "2025-03-29T23:05:01+01:00"


def test_manual_run_generates_the_reconstructed_run_with_overrides(settings, freeze_now):
    freeze_now(datetime(2025, 6, 10, 18, 15, tzinfo=timezone.utc))
    process_conf, timeframe_conf, timestamp = build(settings, RUN_TYPE="DayAheadRMM", INCLUDED_TSO="AST",
                                                    RUN_REPLACEMENT="False")

    tasks = generate(process_conf, timeframe_conf, timestamp)

    assert len(tasks) == 24
    assert tasks[0]["task_properties"]["timestamp_utc"] == "2025-06-10T22:30"
    assert tasks[-1]["task_properties"]["timestamp_utc"] == "2025-06-11T21:30"
    assert {task["run_id"] for task in tasks} == {"https://example.com/runs/DayAheadRMM"}
    assert {task["task_type"] for task in tasks} == {"manual"}
    assert {task["job_id"] for task in tasks} == {tasks[0]["job_id"]}
    assert all(task["task_properties"]["included"] == ["AST"] for task in tasks)
    assert all(task["task_properties"]["replacement"] == "False" for task in tasks)
    assert all(task["task_properties"]["version"] == "" for task in tasks)
