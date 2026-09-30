import json

import pandas as pd
import pytest

from emf.model_merger import merge_functions
from emf.model_merger.merge_functions import MergedModel, TaskConfig

REQUIRED_TASK_PROPERTIES = {
    "time_horizon": "1D", "timestamp_utc": "2025-07-02T09:30:00+00:00", "merge_type": "BA",
    "merging_entity": "BALTICRCC", "mas": "http://www.baltic-rsc.eu/OperationalPlanning", "version": "001",
    "replacement": True, "scaling": True, "outage_update": False, "force_outage_fix": False, "upload_to_opdm": False,
    "upload_to_minio": True, "send_merge_report": True, "post_temp_fixes": True, "lvl8_reporting": False,
}


@pytest.mark.parametrize("key", sorted(REQUIRED_TASK_PROPERTIES))
def test_task_config_requires_property(key):
    properties = {k: v for k, v in REQUIRED_TASK_PROPERTIES.items() if k != key}

    with pytest.raises(KeyError, match=key):
        TaskConfig.from_task({"task_properties": properties})


def test_task_config_defaults_for_optional_properties():
    task_config = TaskConfig.from_task({"task_properties": dict(REQUIRED_TASK_PROPERTIES)})

    assert (task_config.included_models, task_config.excluded_models, task_config.local_import_models,
            task_config.replace_tso) == ([], [], [], [])
    assert (task_config.schedule_start, task_config.schedule_end, task_config.schedule_time_horizon) == (None, None, None)
    assert task_config.task_creation_time == ""


TSOS = ["ELERING", "AST", "LITGRID", "PSE"]


@pytest.mark.parametrize("included, excluded, expected", [
    pytest.param(None, None, TSOS, id="nothing-configured"),
    pytest.param([], [], TSOS, id="empty-lists"),
    pytest.param(["PSE", "AST"], None, ["AST", "PSE"], id="included"),
    pytest.param("AST", None, ["AST"], id="included-string"),
    pytest.param(["AST"], ["AST", "PSE"], ["AST"], id="included-overrides-excluded"),
    pytest.param(None, ["AST"], ["ELERING", "LITGRID", "PSE"], id="excluded"),
    pytest.param(None, "AST", ["ELERING", "LITGRID", "PSE"], id="excluded-string"),
    pytest.param(["TERNA"], None, [], id="unknown-tso-not-added"),
])
def test_filter_models(included, excluded, expected):
    assert merge_functions.filter_models(TSOS, included_models=included, excluded_models=excluded) == expected


def acnp_model(tso, ac_net_position, sum_conform_load=10000.0):
    return {"pmd:TSO": tso, "ac_net_position": ac_net_position, "sum_conform_load": sum_conform_load}


@pytest.mark.parametrize("ac_net_position, sum_conform_load, scheduled, excluded_reason", [
    pytest.param(150.0, 1000.0, 100.0, None, id="within-deadband"),
    pytest.param(300.0, 2000.0, 100.0, None, id="deviation-equal-to-threshold"),
    pytest.param(-150.0, 1000.0, -100.0, None, id="import-within-deadband"),
    pytest.param(300.5, 5000.0, 100.0, "acnp-outside-schedule-deadband", id="deviation-above-threshold"),
    pytest.param(50.0, 5000.0, -200.0, "acnp-outside-schedule-deadband", id="opposite-direction"),
    pytest.param(150.0, 200.0, 100.0, "conform-load-outside-schedule-difference", id="conform-load-too-small"),
    pytest.param(150.0, 250.0, 100.0, "conform-load-outside-schedule-difference", id="conform-load-equal-to-deviation"),
])
def test_filter_models_by_acnp(ac_net_position, sum_conform_load, scheduled, excluded_reason):
    merged_model = MergedModel()
    model = acnp_model("AST", ac_net_position, sum_conform_load)

    # thresholds come as strings from merger.properties
    kept = merge_functions.filter_models_by_acnp([model], merged_model, {"AST": scheduled}, "200", "0.2")

    assert kept == ([] if excluded_reason else [model])
    assert merged_model.excluded == ([{"tso": "AST", "reason": excluded_reason}] if excluded_reason else [])


def test_filter_models_by_acnp_keeps_order_and_models_without_schedule():
    merged_model = MergedModel(excluded=[{"tso": "LITGRID", "reason": "missing-opdm"}])
    models = [acnp_model("ELERING", 0.0), acnp_model("PSE", 5000.0), acnp_model("AST", 900.0), acnp_model("TERNA", 1.0, 1.0)]

    kept = merge_functions.filter_models_by_acnp(models, merged_model, {"ELERING": 50, "AST": 1000, "TERNA": 900}, "200", "0.2")

    assert [model["pmd:TSO"] for model in kept] == ["ELERING", "PSE", "AST"]
    assert merged_model.excluded == [{"tso": "LITGRID", "reason": "missing-opdm"},
                                     {"tso": "TERNA", "reason": "acnp-outside-schedule-deadband"}]


@pytest.mark.xfail(strict=True, raises=(TypeError, KeyError),
                   reason="filter_models_by_acnp crashes when ac_net_position is None or missing (validator could not compute it)")
@pytest.mark.parametrize("model", [
    pytest.param({"pmd:TSO": "AST", "ac_net_position": None, "sum_conform_load": 1000.0}, id="none"),
    pytest.param({"pmd:TSO": "AST", "sum_conform_load": 1000.0}, id="missing"),
])
def test_filter_models_by_acnp_keeps_models_without_ac_net_position(model):
    merged_model = MergedModel()

    assert merge_functions.filter_models_by_acnp([model], merged_model, {"AST": 100.0}, "200", "0.2") == [model]
    assert merged_model.excluded == []


def test_filter_replacements_by_acnp():
    models = pd.DataFrame([
        {"opde:Id": "within", "pmd:TSO": "AST", "ac_net_position": 150.0, "sum_conform_load": 1000.0},
        {"opde:Id": "outside_deadband", "pmd:TSO": "AST", "ac_net_position": 400.0, "sum_conform_load": 5000.0},
        {"opde:Id": "small_conform_load", "pmd:TSO": "AST", "ac_net_position": 150.0, "sum_conform_load": 200.0},
        {"opde:Id": "no_schedule", "pmd:TSO": "PSE", "ac_net_position": 9999.0, "sum_conform_load": 1.0},
        {"opde:Id": "unusable_schedule", "pmd:TSO": "LITGRID", "ac_net_position": 9999.0, "sum_conform_load": 1.0},
    ])

    kept = merge_functions.filter_replacements_by_acnp(models, {"AST": 100, "LITGRID": "n/a"}, "200", "0.2")

    assert kept["opde:Id"].tolist() == ["within", "no_schedule", "unusable_schedule"]


@pytest.mark.parametrize("columns, acnp_dict, threshold, factor", [
    pytest.param(["pmd:TSO", "ac_net_position"], {"AST": 100}, "200", "0.2", id="missing-column"),
    pytest.param(None, None, "200", "0.2", id="no-acnp-dict"),
    pytest.param(None, {"AST": 100}, "not a number", "0.2", id="invalid-threshold"),
    pytest.param(None, {"AST": 100}, "200", None, id="missing-factor"),
])
def test_filter_replacements_by_acnp_returns_input_when_it_cannot_evaluate(columns, acnp_dict, threshold, factor):
    models = pd.DataFrame([{"pmd:TSO": "AST", "ac_net_position": 5000.0, "sum_conform_load": 1.0}])
    models = models[columns] if columns else models

    pd.testing.assert_frame_equal(merge_functions.filter_replacements_by_acnp(models, acnp_dict, threshold, factor), models)


BA_PROPERTIES = {"merge_type": "BA", "scaling": True, "replacement": True}


def flags(scaled=True, replaced=None, outages=None, acnp_schedule_replaced=None):
    return {"scaled": scaled, "replaced": replaced, "outages": outages, "acnp_schedule_replaced": acnp_schedule_replaced}


@pytest.mark.parametrize("report, properties, trustability, reason", [
    pytest.param(flags(), BA_PROPERTIES, "trusted", None, id="scaled-nothing-substituted"),
    pytest.param(flags(True, True, True, True), BA_PROPERTIES, "semi-trusted", None, id="all-substitutions-succeeded"),
    pytest.param(flags(False, True, True, True), BA_PROPERTIES, "untrusted", "scaling failed", id="scaling-failed"),
    pytest.param(flags(True, False, True, True), BA_PROPERTIES, "untrusted", "replacement failed", id="replacement-failed"),
    pytest.param(flags(True, True, False, True), BA_PROPERTIES, "untrusted", "outage fixing failed", id="outage-fix-failed"),
    pytest.param(flags(True, True, True, False), BA_PROPERTIES, "untrusted", "acnp schedule replacement failed",
                 id="acnp-schedule-replacement-failed"),
    pytest.param(flags(), {**BA_PROPERTIES, "scaling": False}, "untrusted", "config is disabled", id="scaling-disabled"),
    pytest.param(flags(True, True, True, True), {**BA_PROPERTIES, "replacement": False}, "untrusted", "config is disabled",
                 id="replacement-disabled"),
    pytest.param(flags(False, False), {**BA_PROPERTIES, "merge_type": "EU"}, "not_evaluated", None, id="not-baltic-merge"),
])
def test_evaluate_trustability(report, properties, trustability, reason):
    assert merge_functions.evaluate_trustability(report, properties) == {"trustability": trustability,
                                                                         "untrustability_reason": reason}


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="untrustability_reason is taken from the last falsy flag, so steps that never ran (None) are blamed")
@pytest.mark.parametrize("report, reason", [
    pytest.param(flags(scaled=False), "scaling failed", id="scaling-failed"),
    pytest.param(flags(replaced=False), "replacement failed", id="replacement-failed"),
])
def test_evaluate_trustability_reason_names_the_step_that_failed(report, reason):
    assert merge_functions.evaluate_trustability(report, BA_PROPERTIES) == {"trustability": "untrusted",
                                                                            "untrustability_reason": reason}


@pytest.mark.parametrize("scenario, created, expected", [
    pytest.param("2023-04-14T22:30:00+00:00", "2023-04-14T13:16:19.730930", "09", id="hours-ahead-floored"),
    pytest.param("2023-04-14T13:45:00+00:00", "2023-04-14T13:16:19", "01", id="less-than-an-hour-ahead"),
    pytest.param("2023-04-15T02:30:00+00:00", "2023-04-14T23:00:00", "03", id="across-midnight"),
    pytest.param("2023-04-15T23:30:00+00:00", "2023-04-14T13:00:00", "34", id="next-day"),
])
def test_set_intraday_time_horizon(scenario, created, expected):
    assert merge_functions.set_intraday_time_horizon(scenario, created) == expected


def test_generate_merge_report(merge_task):
    merge_task["task_properties"].update(merge_type="BA", scaling=True, replacement=True)
    merged_model = MergedModel(network=object(), network_buses_by_component={0: 12}, scaled=True, duration_s=float("nan"),
                               loadflow=[{"connected_component_num": 0, "status": "CONVERGED"},
                                         {"connected_component_num": 1, "status": "FAILED"}])

    report = merge_functions.generate_merge_report(merged_model, merge_task)

    assert "network" not in report and "network_buses_by_component" not in report
    assert [component["buses"] for component in report["loadflow"]] == [12, None]
    assert report["component_count"] == 2
    assert report["@task_id"] == merge_task["@id"]
    assert report["@scenario_timestamp"] == merge_task["task_properties"]["timestamp_utc"]
    assert report["@version"] == 1
    assert (report["merge_type"], report["merge_entity"]) == ("BA", "BALTICRCC")
    assert (report["trustability"], report["untrustability_reason"]) == ("trusted", None)
    assert report["duration_s"] is None
    json.dumps(report, allow_nan=False)
