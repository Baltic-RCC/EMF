from unittest import mock

import pandas as pd
import pytest

from emf.task_generator import task_versioning
from emf.task_generator.task_versioning import set_task_version


def make_task(version):
    return {"task_properties": {"timestamp_utc": "2025-06-10T22:30", "time_horizon": "1D", "merge_type": "EU",
                                "version": version}}


def previous_tasks(*versions):
    return pd.DataFrame({"task_properties.version": list(versions)})


@pytest.fixture
def matching_tasks():
    with mock.patch("emf.task_generator.task_versioning._get_matching_tasks") as get_matching_tasks:
        yield get_matching_tasks


@pytest.mark.parametrize("version", ["", "auto", "AUTO", " Auto "])
def test_auto_version_is_001_when_no_previous_tasks(matching_tasks, version):
    matching_tasks.return_value = pd.DataFrame()
    task = make_task(version)

    set_task_version(task)

    assert task["task_properties"]["version"] == "001"


@pytest.mark.parametrize("version", ["", "auto"])
def test_auto_version_increments_latest_previous_version(matching_tasks, version):
    matching_tasks.return_value = previous_tasks("001", "003", "002")
    task = make_task(version)

    set_task_version(task)

    assert task["task_properties"]["version"] == "004"


@pytest.mark.parametrize("version, expected", [("1", "001"), ("002", "002"), ("15", "015")])
def test_provided_version_is_zero_padded_when_no_previous_tasks(matching_tasks, version, expected):
    matching_tasks.return_value = pd.DataFrame()
    task = make_task(version)

    set_task_version(task)

    assert task["task_properties"]["version"] == expected


@pytest.mark.parametrize("version, latest, expected", [
    ("5", "003", "005"),
    ("004", "003", "004"),
    ("003", "003", "004"),
    ("001", "003", "004"),
])
def test_provided_version_is_bumped_above_latest_previous_version(matching_tasks, version, latest, expected):
    matching_tasks.return_value = previous_tasks("001", latest)
    task = make_task(version)

    set_task_version(task)

    assert task["task_properties"]["version"] == expected


@pytest.mark.parametrize("version, expected", [("", None), ("auto", None), ("002", "002")])
def test_elastic_error_leaves_auto_version_unset_and_keeps_provided_version(matching_tasks, version, expected):
    matching_tasks.side_effect = ConnectionError("Elasticsearch unreachable")
    task = make_task(version)

    set_task_version(task)

    assert task["task_properties"]["version"] == expected


def test_matching_tasks_query_elastic_by_timestamp_time_horizon_and_merge_type():
    with mock.patch.object(task_versioning, "Elastic") as elastic:
        elastic.return_value.get_docs_by_query.return_value = previous_tasks("001")
        task_versioning._get_matching_tasks(make_task("auto"))

    query = elastic.return_value.get_docs_by_query.call_args.kwargs["query"]
    assert elastic.return_value.get_docs_by_query.call_args.kwargs["index"] == task_versioning.TASK_ELK_INDEX
    assert query == {"bool": {"must": [
        {"match": {"task_properties.timestamp_utc": "2025-06-10T22:30"}},
        {"term": {"task_properties.time_horizon.keyword": "1D"}},
        {"term": {"task_properties.merge_type.keyword": "EU"}},
    ]}}


def test_non_integer_versions_in_elastic_are_ignored():
    with mock.patch.object(task_versioning, "Elastic") as elastic:
        elastic.return_value.get_docs_by_query.return_value = previous_tasks("001", "abc", None, "2.5", "007", "003")
        task = make_task("auto")

        set_task_version(task)

    assert task["task_properties"]["version"] == "008"


def test_no_elastic_results_gives_first_version():
    with mock.patch.object(task_versioning, "Elastic") as elastic:
        elastic.return_value.get_docs_by_query.return_value = pd.DataFrame()
        task = make_task("")

        set_task_version(task)

    assert task["task_properties"]["version"] == "001"


def test_unparseable_provided_version_is_kept(matching_tasks):
    matching_tasks.return_value = previous_tasks("001")
    task = make_task("v2")

    set_task_version(task)

    assert task["task_properties"]["version"] == "v2"
