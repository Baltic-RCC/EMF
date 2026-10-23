import copy
import json
from unittest import mock

import pytest

import config
from emf.task_generator import task_generator
from emf.task_generator.task_generator import generate_tasks

PROCESS_CONF = json.loads(config.paths.task_generator.process_conf.read_text())
TIMEFRAME_CONF = json.loads(config.paths.task_generator.timeframe_conf.read_text())
MANUAL_ONLY_RUN_TYPES = [run["@id"].rsplit("/runs/", 1)[-1] for process in PROCESS_CONF for run in process["runs"] if not run.get("run_at")]


def select_runs(*run_types: str) -> list:
    """Deep copy of process_conf.json keeping only the given runs, e.g. select_runs("DayAheadCGM")"""
    process_conf = copy.deepcopy(PROCESS_CONF)
    for process in process_conf:
        process["runs"] = [run for run in process["runs"] if run["@id"].rsplit("/runs/", 1)[-1] in run_types]
    return [process for process in process_conf if process["runs"]]


def timestamps(tasks: list) -> list:
    return [task["task_properties"]["timestamp_utc"] for task in tasks]


@pytest.fixture
def patched_status_and_version():
    with mock.patch("emf.task_generator.task_generator.set_task_version") as set_task_version, \
            mock.patch("emf.task_generator.task_generator.update_task_status") as update_task_status:
        yield set_task_version, update_task_status


@pytest.fixture
def generate(patched_status_and_version):
    def _generate(process_conf, now, window_duration="PT1M", window_reference="currentMinuteStart", **kwargs):
        return list(generate_tasks(window_duration, window_reference, process_conf, copy.deepcopy(TIMEFRAME_CONF),
                                   timetravel_now=now, **kwargs))
    return _generate


@pytest.mark.parametrize("now, expected_count", [
    ("2025-06-10T18:48:30+02:00", 0),
    ("2025-06-10T18:49:30+02:00", 24),  # run at 18:50 is the window end
    ("2025-06-10T18:50:00+02:00", 24),  # run exactly now
    ("2025-06-10T18:50:30+02:00", 0),  # run already created by the previous minute's window
], ids=["before-window", "run-at-window-end", "run-exactly-now", "after-window"])
def test_run_is_picked_up_only_when_in_one_minute_window(generate, now, expected_count):
    assert len(generate(select_runs("DayAheadCGM"), now)) == expected_count


def test_day_ahead_run_creates_hourly_tasks_for_next_local_day(generate):
    with mock.patch.object(task_generator, "TASK_SCHEDULE_SHIFT", "P0D"):
        tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00")

    assert timestamps(tasks) == ["2025-06-10T22:30", "2025-06-10T23:30"] + [f"2025-06-11T{hour:02d}:30" for hour in range(22)]
    for task in tasks:
        assert task["job_period_start"] == "2025-06-10T22:00:00+00:00"
        assert task["job_period_end"] == "2025-06-11T22:00:00+00:00"
        assert task["task_gate_open"] == "2025-06-10T16:00:00+00:00"  # gate_open PT6H before period start
        assert task["task_gate_close"] == "2025-06-10T17:00:00+00:00"  # gate_close PT5H before period start
        assert task["task_properties"]["reference_schedule_start_utc"] == task["task_properties"]["timestamp_utc"]
    assert tasks[0]["task_properties"]["reference_schedule_end_utc"] == "2025-06-10T22:45"


def test_generated_task_fields(generate, monkeypatch):
    monkeypatch.setenv("USER", "operator")
    tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00")
    task = tasks[0]

    assert task["@context"] == "https://example.com/task_context.jsonld"
    assert task["@type"] == "Task"
    assert task["@id"].startswith("urn:uuid:")
    assert len({task["@id"] for task in tasks}) == len(tasks)
    assert task["process_id"] == "https://example.com/processes/CGM_CREATION"
    assert task["run_id"] == "https://example.com/runs/DayAheadCGM"
    assert task["task_type"] == "automatic"
    assert task["task_initiator"] == "operator"
    assert task["task_priority"] == "normal"
    assert task["task_timeout"] == "PT1H"
    assert task["task_retry_count"] == 0
    assert task["task_status_trace"] == [] and task["task_dependencies"] == []
    assert task["task_properties"]["reference_schedule_time_horizon"] == task_generator.TASK_SCHEDULE_TIME_HORIZON
    assert {"merge_type": "EU", "time_horizon": "1D", "version": "001"}.items() <= task["task_properties"].items()


def test_each_task_is_versioned_and_marked_created(generate, patched_status_and_version):
    set_task_version, update_task_status = patched_status_and_version

    tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00")

    assert [call.kwargs["task"] for call in set_task_version.call_args_list] == tasks
    assert [call.kwargs for call in update_task_status.call_args_list] == [{"task": task, "status_text": "created"} for task in tasks]


@pytest.mark.parametrize("run_type, now, expected_count, expected_first", [
    ("IntraDayCGM/1", "2025-06-10T23:04:30+02:00", 24, "2025-06-10T22:30"),
    ("IntraDayCGM/2", "2025-06-10T07:04:30+02:00", 16, "2025-06-10T06:30"),
    ("IntraDayCGM/3", "2025-06-10T15:04:30+02:00", 8, "2025-06-10T14:30"),
    ("TwoDaysAheadCGM", "2025-06-10T19:49:30+02:00", 24, "2025-06-11T22:30"),
    ("IntraDayRMM/1", "2025-06-10T22:59:30+02:00", 24, "2025-06-10T22:30"),
    ("DayAheadRMM", "2025-06-10T17:59:30+02:00", 24, "2025-06-10T22:30"),
    ("TwoDaysAheadRMM", "2025-06-10T18:59:30+02:00", 24, "2025-06-11T22:30"),
])
def test_hourly_runs_cover_their_time_frame(generate, run_type, now, expected_count, expected_first):
    tasks = generate(select_runs(run_type), now)

    assert len(tasks) == expected_count
    assert tasks[0]["task_properties"]["timestamp_utc"] == expected_first
    assert {task["run_id"] for task in tasks} == {f"https://example.com/runs/{run_type}"}


def test_intraday_time_frame_covers_next_hour_until_end_of_day(generate):
    process_conf = select_runs("DayAheadCGM")
    process_conf[0]["runs"][0].update({"run_at": "00 15 * * *", "time_frame": "ID"})

    tasks = generate(process_conf, "2025-06-10T14:59:30+02:00")

    assert timestamps(tasks) == [f"2025-06-10T{hour:02d}:30" for hour in range(14, 22)]
    assert tasks[0]["job_period_start"] == "2025-06-10T14:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2025-06-10T22:00:00+00:00"


def test_job_id_is_shared_by_the_tasks_of_one_run_instance(generate):
    tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T00:00:30+02:00", window_duration="P2D",
                     window_reference="currentDayStart")

    job_ids = [task["job_id"] for task in tasks]
    assert len(tasks) == 48
    assert len(set(job_ids[:24])) == 1 and len(set(job_ids[24:])) == 1
    assert job_ids[0] != job_ids[24]
    assert job_ids[0].startswith("urn:uuid:")
    assert job_ids[0] != tasks[0]["@id"]


def test_each_run_gets_its_own_job_id(generate):
    tasks = generate(select_runs("DayAheadCGM", "TwoDaysAheadCGM"), "2025-06-10T00:00:30+02:00",
                     window_duration="P1D", window_reference="currentDayStart")

    job_ids_per_run = {}
    for task in tasks:
        job_ids_per_run.setdefault(task["run_id"], set()).add(task["job_id"])
    assert {run_id: len(job_ids) for run_id, job_ids in job_ids_per_run.items()} == {
        "https://example.com/runs/DayAheadCGM": 1,
        "https://example.com/runs/TwoDaysAheadCGM": 1,
    }
    assert len(set.union(*job_ids_per_run.values())) == 2


def test_properties_are_layered_base_then_process_then_run(generate):
    process_conf = select_runs("DayAheadCGM")
    process_conf[0]["properties"] = {"reference_schedule_time_horizon": "1D", "merge_type": "BA", "process_only": "p"}
    process_conf[0]["runs"][0]["properties"]["reference_schedule_end_utc"] = "from-run"

    properties = generate(process_conf, "2025-06-10T18:49:30+02:00")[0]["task_properties"]

    assert properties["reference_schedule_time_horizon"] == "1D"  # process over base
    assert properties["merge_type"] == "EU"  # run over process
    assert properties["reference_schedule_end_utc"] == "from-run"  # run over base
    assert properties["process_only"] == "p"
    assert properties["timestamp_utc"] == "2025-06-10T22:30"


def test_tags_priority_and_process_id_come_from_run_then_process(generate):
    process_conf = select_runs("DayAheadCGM", "DayAheadRMM")
    for process in process_conf:
        process.update({"tags": ["process-tag"], "priority": "low"})
    cgm_run = process_conf[0]["runs"][0]
    cgm_run.update({"tags": ["run-tag"], "priority": "high"})
    rmm_run = process_conf[1]["runs"][0]
    del rmm_run["process_id"]
    rmm_run["run_at"] = cgm_run["run_at"]

    tasks = {task["run_id"].rsplit("/", 1)[-1]: task for task in generate(process_conf, "2025-06-10T18:49:30+02:00")}

    assert tasks["DayAheadCGM"]["task_tags"] == ["process-tag", "run-tag"]
    assert tasks["DayAheadCGM"]["task_priority"] == "high"
    assert tasks["DayAheadRMM"]["task_priority"] == "low"
    assert tasks["DayAheadRMM"]["process_id"] == "https://example.com/processes/RMM_CREATION"


def test_boolean_strings_are_converted_and_task_is_published():
    with mock.patch("emf.task_generator.task_generator.set_task_version"), \
            mock.patch("emf.common.helpers.tasks.Elastic.send_to_elastic") as send_to_elastic:
        tasks = list(generate_tasks("PT1M", "currentMinuteStart", select_runs("DayAheadRMM"), copy.deepcopy(TIMEFRAME_CONF),
                                    timetravel_now="2025-06-10T17:59:30+02:00"))

    properties = tasks[0]["task_properties"]
    assert properties["replacement"] is True
    assert properties["scaling"] is False
    assert properties["upload_to_opdm"] is False
    assert properties["included"] == ["AST", "PSE", "ELERING"]
    assert tasks[0]["task_status"] == "created"
    assert [entry["status"] for entry in tasks[0]["task_status_trace"]] == ["created"]
    assert [call.kwargs["id"] for call in send_to_elastic.call_args_list] == [task["@id"] for task in tasks]


def test_process_time_shift_moves_the_reference_time(generate):
    tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00", process_time_shift="-P1D")

    assert tasks[0]["task_properties"]["timestamp_utc"] == "2025-06-09T22:30"
    assert tasks[0]["job_period_start"] == "2025-06-09T22:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2025-06-10T22:00:00+00:00"
    assert tasks[0]["task_gate_open"] == "2025-06-09T16:00:00+00:00"


@pytest.mark.parametrize("task_initiator, env, expected", [
    ("jane.doe", {"USER": "operator", "USERNAME": "win-operator"}, "jane.doe"),
    (None, {"USER": "operator", "USERNAME": "win-operator"}, "operator"),
    (None, {"USERNAME": "win-operator"}, "win-operator"),
    (None, {}, "unknown"),
    ("", {}, "unknown"),
])
def test_task_initiator_falls_back_to_user_environment(generate, monkeypatch, task_initiator, env, expected):
    for name in ("USER", "USERNAME"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00", task_type="manual", task_initiator=task_initiator)

    assert {task["task_initiator"] for task in tasks} == {expected}
    assert {task["task_type"] for task in tasks} == {"manual"}


def test_task_schedule_shift_moves_reference_schedule(generate):
    with mock.patch.object(task_generator, "TASK_SCHEDULE_SHIFT", "-P7D"):
        tasks = generate(select_runs("DayAheadCGM"), "2025-06-10T18:49:30+02:00")

    assert tasks[0]["task_properties"]["timestamp_utc"] == "2025-06-10T22:30"
    assert tasks[0]["task_properties"]["reference_schedule_start_utc"] == "2025-06-03T22:30"
    assert tasks[0]["task_properties"]["reference_schedule_end_utc"] == "2025-06-03T22:45"


@pytest.mark.xfail(strict=True, reason="TASK_SCHEDULE_SHIFT is added to a pytz aware local time without normalizing, "
                                       "so the shift is applied in UTC instead of local time across DST")
def test_task_schedule_shift_is_applied_in_local_time_across_dst(generate):
    with mock.patch.object(task_generator, "TASK_SCHEDULE_SHIFT", "P7D"):
        tasks = generate(select_runs("DayAheadCGM"), "2025-03-26T18:49:30+01:00")

    # 2025-03-27 00:30 CET shifted a week is 2025-04-03 00:30 CEST
    assert tasks[0]["task_properties"]["timestamp_utc"] == "2025-03-26T23:30"
    assert tasks[0]["task_properties"]["reference_schedule_start_utc"] == "2025-04-02T22:30"


def test_day_ahead_run_before_spring_dst_change_creates_23_tasks(generate):
    tasks = generate(select_runs("DayAheadCGM"), "2025-03-29T18:49:30+01:00")

    assert tasks[0]["job_period_start"] == "2025-03-29T23:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2025-03-30T22:00:00+00:00"
    assert timestamps(tasks) == ["2025-03-29T23:30"] + [f"2025-03-30T{hour:02d}:30" for hour in range(22)]


def test_day_ahead_run_before_autumn_dst_change_creates_25_tasks(generate):
    tasks = generate(select_runs("DayAheadCGM"), "2025-10-25T18:49:30+02:00")

    assert tasks[0]["job_period_start"] == "2025-10-25T22:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2025-10-26T23:00:00+00:00"
    assert timestamps(tasks) == ["2025-10-25T22:30", "2025-10-25T23:30"] + [f"2025-10-26T{hour:02d}:30" for hour in range(23)]


@pytest.mark.parametrize("now, expected_count, expected_last", [
    ("2025-03-29T23:04:30+01:00", 23, "2025-03-30T21:30"),
    ("2025-10-25T23:04:30+02:00", 25, "2025-10-26T22:30"),
], ids=["spring-dst", "autumn-dst"])
def test_intraday_h24_run_covers_the_whole_local_day_on_dst_days(generate, now, expected_count, expected_last):
    tasks = generate(select_runs("IntraDayCGM/1"), now)

    assert len(tasks) == expected_count
    assert tasks[-1]["task_properties"]["timestamp_utc"] == expected_last


def test_two_days_ahead_run_before_autumn_dst_covers_the_monday_after(generate):
    tasks = generate(select_runs("TwoDaysAheadCGM"), "2025-10-25T19:49:30+02:00")

    assert tasks[0]["job_period_start"] == "2025-10-26T23:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2025-10-27T23:00:00+00:00"
    assert len(tasks) == 24


@pytest.mark.parametrize("shift, expected_start, expected_end", [
    (None, "2025-10-24T22:00:00+00:00", "2025-11-02T23:00:00+00:00"),
    ("-P1D", "2025-10-24T22:00:00+00:00", "2025-11-02T23:00:00+00:00"),  # Tuesday, same week
    ("-P3D", "2025-10-17T22:00:00+00:00", "2025-10-26T23:00:00+00:00"),  # Sunday, previous week
    ("P7D", "2025-10-31T23:00:00+00:00", "2025-11-09T23:00:00+00:00"),
])
def test_process_time_shift_selects_the_week_of_a_week_ahead_run(generate, shift, expected_start, expected_end):
    tasks = generate(select_runs("WeekAheadRMM"), "2025-10-22T17:59:30+02:00", process_time_shift=shift)

    assert tasks[0]["job_period_start"] == expected_start
    assert tasks[0]["job_period_end"] == expected_end
    assert len(tasks) == 9  # Saturday to Sunday of the following week, 09:30Z each day


@pytest.mark.parametrize("now, expected_start, expected_end", [
    ("2025-09-17T22:59:30+02:00", "2025-09-30T22:00:00+00:00", "2025-10-31T23:00:00+00:00"),  # October
    ("2026-02-17T22:59:30+01:00", "2026-02-28T23:00:00+00:00", "2026-03-31T22:00:00+00:00"),  # March, after 28 days
], ids=["october", "march"])
def test_month_ahead_run_covers_the_next_calendar_month(generate, now, expected_start, expected_end):
    process_conf = select_runs("MonthAheadRMM")
    process_conf[0]["runs"][0]["run_at"] = "00 23 17 * *"

    tasks = generate(process_conf, now)

    assert tasks[0]["job_period_start"] == expected_start
    assert tasks[0]["job_period_end"] == expected_end
    assert len(tasks) == 31


def test_year_ahead_run_covers_the_next_calendar_year(generate):
    process_conf = select_runs("YearAheadRMM")
    process_conf[0]["runs"][0]["run_at"] = "0 4 1 9 *"

    tasks = generate(process_conf, "2027-09-01T03:59:30+02:00")

    assert tasks[0]["job_period_start"] == "2027-12-31T23:00:00+00:00"
    assert tasks[0]["job_period_end"] == "2028-12-31T23:00:00+00:00"
    assert len(tasks) == 8784  # 2028 is a leap year


def test_intraday_rmm_eod_does_not_fire_on_an_ordinary_wednesday(generate):
    assert generate(select_runs("IntraDayRMM/EOD"), "2025-06-04T14:59:30+02:00") == []


@pytest.mark.xfail(select_runs("IntraDayRMM/EOD")[0]["runs"][0]["run_at"] == "00 15 29 02 3", strict=True,
                   reason="croniter ORs day-of-month and day-of-week, so run_at '00 15 29 02 3' "
                          "(29 February on a Wednesday) fires every Wednesday in February")
def test_intraday_rmm_eod_does_not_fire_on_a_february_wednesday(generate):
    assert generate(select_runs("IntraDayRMM/EOD"), "2026-02-04T14:59:30+01:00") == []


@pytest.mark.parametrize("run_type", MANUAL_ONLY_RUN_TYPES)
def test_run_without_run_at_is_never_generated_automatically(generate, run_type):
    assert generate(select_runs(run_type), "2026-01-01T00:00:30+02:00", window_duration="P366D",
                    window_reference="currentDayStart") == []
