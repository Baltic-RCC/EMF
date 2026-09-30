import datetime
from unittest import mock

import pytest
from isodate import parse_duration

from emf.common.helpers.opdm_objects import DataSource
from emf.model_merger import replacement
from emf.model_merger.merge_functions import MergedModel

# A Wednesday, weekday 2 in replacement_conf.json
SCENARIO = "2025-07-02T09:30:00Z"


def candidate(name, time_horizon, scenario_date, tso="ELERING", version="001", created="2025-06-01T00:00:00Z", **fields):
    """Model metadata as returned by the Elastic replacement query"""
    return {"opde:Id": name, "pmd:TSO": tso, "pmd:timeHorizon": time_horizon, "pmd:scenarioDate": scenario_date,
            "pmd:versionNumber": version, "pmd:creationDate": created, **fields}


def find_replacements(records, time_horizon="1D", scenario_date=SCENARIO, tso_list=None, **kwargs):
    by_tso = {}
    for record in records:
        by_tso.setdefault(record["pmd:TSO"], []).append(record)
    with mock.patch.object(replacement, "query_data",
                           side_effect=lambda query, query_filter: by_tso.get(query["pmd:TSO.keyword"][0], [])), \
            mock.patch.object(replacement, "get_content", side_effect=lambda metadata: {**metadata, "downloaded": True}):
        return replacement.find_replacement_models(tso_list=tso_list or list(by_tso), time_horizon=time_horizon,
                                                   scenario_date=scenario_date, **kwargs)


def ids(models):
    return [model["opde:Id"] for model in models]


@pytest.mark.parametrize("time_horizon, records, expected", [
    pytest.param("1D", [candidate("same_day_2D", "2D", "2025-07-02T09:30:00Z"),
                        candidate("same_day_1D_later_hour", "1D", "2025-07-02T11:30:00Z")],
                 "same_day_1D_later_hour", id="step1-same-horizon-same-day-beats-better-hour"),
    pytest.param("1D", [candidate("previous_day_1D", "1D", "2025-07-01T09:30:00Z"),
                        candidate("same_day_2D", "2D", "2025-07-02T09:30:00Z")],
                 "same_day_2D", id="step2-same-day-other-horizon-beats-other-day"),
    pytest.param("1D", [candidate("next_day_2D", "2D", "2025-07-03T09:30:00Z"),
                        candidate("two_days_ago_1D", "1D", "2025-06-30T09:30:00Z")],
                 "two_days_ago_1D", id="step3-same-horizon-other-day-beats-other-horizon"),
    pytest.param("2D", [candidate("previous_day_ID", "05", "2025-07-01T09:30:00Z"),
                        candidate("two_days_ago_1D", "1D", "2025-06-30T09:30:00Z")],
                 "two_days_ago_1D", id="step4-business-priority-first"),
])
def test_find_replacement_models_applies_steps_in_order(time_horizon, records, expected):
    assert ids(find_replacements(records, time_horizon=time_horizon)) == [expected]


@pytest.mark.parametrize("records, expected", [
    pytest.param([candidate("h10_30", "1D", "2025-07-02T10:30:00Z"), candidate("h09_30", "1D", "2025-07-02T09:30:00Z")],
                 "h09_30", id="scenario-hour-first"),
    pytest.param([candidate("h08_30", "1D", "2025-07-02T08:30:00Z"), candidate("h10_30", "1D", "2025-07-02T10:30:00Z")],
                 "h10_30", id="configured-hour-order"),
    pytest.param([candidate("next_day_h08_30", "1D", "2025-07-03T08:30:00Z"),
                  candidate("previous_day_h10_30", "1D", "2025-07-01T10:30:00Z")],
                 "previous_day_h10_30", id="hour-priority-before-day-priority"),
    # For a working day the configured day order puts the weekend (same day type rule) last
    pytest.param([candidate("saturday", "1D", "2025-06-28T09:30:00Z"), candidate("friday_before", "1D", "2025-06-27T09:30:00Z")],
                 "friday_before", id="configured-day-order"),
    pytest.param([candidate("v1", "1D", SCENARIO, version="001"), candidate("v3", "1D", SCENARIO, version="003"),
                  candidate("v2", "1D", SCENARIO, version="002")],
                 "v3", id="highest-version"),
    pytest.param([candidate("older", "1D", SCENARIO, created="2025-07-01T06:00:00Z"),
                  candidate("newer", "1D", SCENARIO, created="2025-07-01T07:00:00Z")],
                 "newer", id="latest-creation-date"),
    pytest.param([candidate("v1_newer", "1D", SCENARIO, version="001", created="2025-07-01T07:00:00Z"),
                  candidate("v2_older", "1D", SCENARIO, version="002", created="2025-07-01T06:00:00Z")],
                 "v2_older", id="version-before-creation-date"),
])
def test_find_replacement_models_tie_breaks(records, expected):
    assert ids(find_replacements(records)) == [expected]


@pytest.mark.parametrize("record", [
    pytest.param(candidate("unconfigured_hour", "1D", "2025-07-02T15:30:00Z"), id="hour-not-in-priority-list"),
    pytest.param(candidate("unrequested_business_type", "WK", SCENARIO), id="business-type-not-requested"),
    pytest.param(candidate("unconfigured_day", "1D", "2025-07-04T09:30:00Z"), id="day-not-in-priority-list"),
])
def test_find_replacement_models_ignores_models_outside_configured_priorities(record):
    assert find_replacements([record]) == []


@pytest.mark.parametrize("hours_ahead", ["01", "24"])
def test_find_replacement_models_treats_hours_ahead_horizons_as_intraday(hours_ahead):
    records = [candidate("day_ahead", "1D", SCENARIO), candidate("intraday", hours_ahead, "2025-07-02T10:30:00Z")]

    found = find_replacements(records, time_horizon="ID")

    assert ids(found) == ["intraday"]
    assert found[0]["pmd:timeHorizon"] == hours_ahead


def test_find_replacement_models_returns_original_documents_of_winners_only():
    records = [candidate("elering_best", "1D", SCENARIO, tso="ELERING", extra="kept"),
               candidate("elering_other", "1D", "2025-07-02T10:30:00Z", tso="ELERING"),
               candidate("ast_best", "2D", SCENARIO, tso="AST")]

    with mock.patch.object(replacement, "get_content", side_effect=lambda metadata: metadata) as get_content, \
            mock.patch.object(replacement, "query_data",
                              side_effect=lambda query, _: [r for r in records if r["pmd:TSO"] in query["pmd:TSO.keyword"]]):
        found = replacement.find_replacement_models(tso_list=["ELERING", "AST", "LITGRID"], time_horizon="1D",
                                                    scenario_date=SCENARIO)

    assert sorted(ids(found)) == ["ast_best", "elering_best"]
    assert get_content.call_count == 2
    assert next(model for model in found if model["opde:Id"] == "elering_best") == records[0]


@pytest.mark.parametrize("existing_horizon, expected", [
    pytest.param("1D", "neighbour_hour", id="same-model-excluded"),
    pytest.param("2D", "scenario_hour", id="other-horizon-not-excluded"),
])
def test_find_replacement_models_excludes_already_existing_models(existing_horizon, expected):
    records = [candidate("scenario_hour", "1D", SCENARIO), candidate("neighbour_hour", "1D", "2025-07-02T10:30:00Z")]
    existing = [{"pmd:TSO": "ELERING", "pmd:scenarioDate": "2025-07-02T09:30:00+00:00", "pmd:timeHorizon": existing_horizon}]

    assert ids(find_replacements(records, existing_models=existing)) == [expected]


@pytest.mark.parametrize("acnp_dict, expected", [
    pytest.param({"ELERING": 100}, ["within_schedule"], id="best-outside-deadband-dropped"),
    pytest.param({"AST": 100}, ["outside_schedule"], id="tso-without-schedule-not-filtered"),
    pytest.param({"ELERING": -2000}, [], id="all-dropped"),
])
def test_find_replacement_models_filters_candidates_by_acnp(acnp_dict, expected):
    records = [candidate("outside_schedule", "1D", SCENARIO, ac_net_position=400.0, sum_conform_load=5000.0),
               candidate("within_schedule", "1D", "2025-07-02T10:30:00Z", ac_net_position=150.0, sum_conform_load=1000.0)]

    assert ids(find_replacements(records, acnp_dict=acnp_dict, acnp_threshold="200", conform_load_factor="0.2")) == expected


def test_find_replacement_models_drops_documents_with_list_valued_fields():
    records = [candidate("multi_valued", ["1D", "2D"], SCENARIO), candidate("clean", "1D", "2025-07-02T10:30:00Z")]

    assert ids(find_replacements(records)) == ["clean"]


def test_find_replacement_models_month_ahead_uses_first_monday_of_previous_month():
    records = [candidate("month_ahead", "MO", "2025-07-16T21:30:00Z"),
               candidate("first_monday_08_30", "1D", "2025-06-02T08:30:00Z"),
               candidate("first_monday_09_30", "1D", "2025-06-02T09:30:00Z"),
               candidate("second_monday_09_30", "1D", "2025-06-09T09:30:00Z")]

    assert ids(find_replacements(records, time_horizon="MO", scenario_date="2025-07-16T21:30:00Z")) == ["first_monday_09_30"]


@pytest.mark.parametrize("time_horizon, lookback", [("1D", "now-2w"), ("ID", "now-2w"), ("MO", "now-1M"), ("YR", "now-4M")])
def test_find_replacement_models_queries_valid_models_of_source_within_lookback(time_horizon, lookback):
    with mock.patch.object(replacement, "query_data", return_value=[]) as query_data:
        assert replacement.find_replacement_models(["ELERING"], time_horizon, SCENARIO, data_source=DataSource.PDN) == []

    query_data.assert_called_once_with({"pmd:TSO.keyword": ["ELERING"], "valid": True, "data-source": DataSource.PDN}, lookback)


def test_find_replacement_models_continues_with_other_tsos_when_a_query_fails():
    def query_data(query, query_filter):
        if query["pmd:TSO.keyword"] == ["ELERING"]:
            raise ConnectionError("elastic down")
        return [candidate("ast", "1D", SCENARIO, tso="AST")]

    with mock.patch.object(replacement, "query_data", side_effect=query_data), \
            mock.patch.object(replacement, "get_content", side_effect=lambda metadata: metadata):
        assert ids(replacement.find_replacement_models(["ELERING", "AST"], "1D", SCENARIO)) == ["ast"]


@pytest.mark.parametrize("tso_list, scenario_date", [([], SCENARIO), (["ELERING"], "not a date")])
def test_find_replacement_models_returns_nothing_without_querying_for_invalid_input(tso_list, scenario_date):
    with mock.patch.object(replacement, "query_data") as query_data:
        assert replacement.find_replacement_models(tso_list, "1D", scenario_date) == []
    query_data.assert_not_called()


def test_make_lists_priority_applies_configured_offsets_in_order():
    conf = {
        "time_horizons": {"1D": {"request_list": ["1D", "2D"]}},
        "hours": [{"hour": "09:30", "priority": [{"1": "PT0H"}]},
                  {"hour": "23:30", "priority": [{"1": "PT0H"}, {"2": "PT1H"}, {"3": "-PT1H"}]}],
        "days": [{"day": 2, "priority": [{"1": "P0D"}, {"2": "-P7D"}, {"3": "P1D"}]}],
    }

    hours, days, business = replacement.make_lists_priority("2025-07-02T23:30:00Z", "1D", conf)

    assert hours == ["23:30", "00:30", "22:30"]
    assert days == ["2025-07-02", "2025-06-25", "2025-07-03"]
    assert business == ["1D", "2D"]


def test_make_lists_priority_month_ahead_special_handling():
    special = replacement.replacement_config["time_horizons"]["MO"]["special_handling"]

    hours, days, business = replacement.make_lists_priority("2025-07-16T21:30:00Z", "MO", replacement.replacement_config)

    assert hours == special["hours"]
    assert days == ["2025-06-02"]
    assert business == special["business_type"]


@pytest.mark.parametrize("timestamp, first_monday", [
    pytest.param("2025-03-15T09:30:00Z", datetime.date(2025, 2, 3), id="month-starting-on-saturday"),
    pytest.param("2025-01-10T00:30:00Z", datetime.date(2024, 12, 2), id="january-goes-to-previous-year"),
    pytest.param("2025-03-31T09:30:00Z", datetime.date(2025, 2, 3), id="day-missing-in-previous-month"),
    pytest.param("2025-10-05T09:30:00Z", datetime.date(2025, 9, 1), id="month-starting-on-monday"),
])
def test_get_first_monday_of_last_month(timestamp, first_monday):
    assert replacement.get_first_monday_of_last_month(timestamp).date() == first_monday


def test_shipped_replacement_config_covers_every_scenario_hour_and_weekday():
    conf = replacement.replacement_config

    assert sorted(hour["hour"] for hour in conf["hours"]) == [f"{h:02d}:30" for h in range(24)]
    assert sorted(day["day"] for day in conf["days"]) == list(range(7))
    for entry in conf["hours"] + conf["days"]:
        offsets = [parse_duration(value) for item in entry["priority"] for value in item.values()]
        assert offsets[0] == datetime.timedelta(0), entry
    for time_horizon, settings in conf["time_horizons"].items():
        assert settings["request_list"][0] == time_horizon
        assert settings["replacement_length"]


def igm(tso, source=DataSource.OPDM, name="current"):
    return {"opde:Id": f"{tso}-{source.value}-{name}", "pmd:TSO": tso, "data-source": source}


def run_replacement(igm_models, available=None, **kwargs):
    """Runs run_replacement with find_replacement_models answering from available {(tso, source): [models]}"""
    available = available or {}
    calls = []

    def find_replacement_models(tso_list, data_source, existing_models, **_):
        calls.append((tso_list[0], data_source, sorted(model["opde:Id"] for model in existing_models)))
        return [dict(model) for model in available.get((tso_list[0], data_source), [])]

    params = dict(model_replacement=True, local_import_models=[], missing_local_import=[], missing_models=[],
                  replace_tso=[], time_horizon="1D", scenario_datetime=SCENARIO)
    params.update(kwargs)
    merged_model = MergedModel()
    with mock.patch.object(replacement, "find_replacement_models", side_effect=find_replacement_models):
        result = replacement.run_replacement(igm_models=igm_models, merged_model=merged_model, **params)
    return result, merged_model, calls


def test_run_replacement_requests_forced_then_missing_opdm_then_missing_pdn():
    _, _, calls = run_replacement([], replace_tso=["ELERING", "AST"], local_import_models=["AST", "PSE"],
                                  missing_models=["LITGRID"], missing_local_import=["PSE"])

    assert [(tso, source) for tso, source, _ in calls] == [
        ("ELERING", DataSource.OPDM), ("AST", DataSource.PDN), ("LITGRID", DataSource.OPDM), ("PSE", DataSource.PDN)]


def test_run_replacement_forced_replaces_only_the_tsos_own_model_of_that_source():
    models = [igm("ELERING"), igm("ELERING", DataSource.PDN), igm("AST")]
    substitute = igm("ELERING", name="substitute")

    result, merged_model, calls = run_replacement(models, available={("ELERING", DataSource.OPDM): [substitute]},
                                                  replace_tso=["ELERING"])

    assert calls == [("ELERING", DataSource.OPDM, ["ELERING-OPDM-current"])]
    assert sorted(ids(result)) == ["AST-OPDM-current", "ELERING-OPDM-substitute", "ELERING-PDN-current"]
    assert merged_model.replaced is True
    assert [(entity["tso"], entity["quality_indicator"]) for entity in merged_model.replaced_entity] == [("ELERING", "Substituted")]


def test_run_replacement_forced_keeps_current_model_when_no_replacement_found():
    result, merged_model, _ = run_replacement([igm("ELERING")], replace_tso=["ELERING"])

    assert ids(result) == ["ELERING-OPDM-current"]
    assert merged_model.replaced is False
    assert merged_model.replaced_entity == []


def test_run_replacement_missing_model_excludes_all_current_models_of_its_source():
    models = [igm("ELERING"), igm("AST"), igm("PSE", DataSource.PDN)]

    result, _, calls = run_replacement(models, available={("LITGRID", DataSource.OPDM): [igm("LITGRID", name="old")]},
                                       missing_models=["LITGRID"])

    assert calls == [("LITGRID", DataSource.OPDM, ["AST-OPDM-current", "ELERING-OPDM-current"])]
    assert sorted(ids(result)) == ["AST-OPDM-current", "ELERING-OPDM-current", "LITGRID-OPDM-old", "PSE-PDN-current"]


@pytest.mark.parametrize("forced_found, expected_calls", [
    pytest.param(True, [("ELERING", DataSource.OPDM)], id="satisfied-by-forced-skipped"),
    pytest.param(False, [("ELERING", DataSource.OPDM)] * 2, id="retried-when-forced-found-nothing"),
])
def test_run_replacement_skips_tsos_already_replaced(forced_found, expected_calls):
    available = {("ELERING", DataSource.OPDM): [igm("ELERING", name="old")]} if forced_found else {}

    result, _, calls = run_replacement([], available=available, replace_tso=["ELERING"], missing_models=["ELERING"])

    assert [(tso, source) for tso, source, _ in calls] == expected_calls
    assert len(result) == int(forced_found)


@pytest.mark.parametrize("kwargs", [
    pytest.param(dict(model_replacement=False, missing_models=["LITGRID"], local_import_models=["PSE"],
                      missing_local_import=["PSE"]), id="replacement-disabled"),
    pytest.param(dict(missing_local_import=["PSE"]), id="no-local-import-configured"),
])
def test_run_replacement_ignores_missing_models_when_not_applicable(kwargs):
    result, merged_model, calls = run_replacement([igm("ELERING")], **kwargs)

    assert calls == []
    assert ids(result) == ["ELERING-OPDM-current"]
    assert merged_model.replaced is None


def test_run_replacement_disabled_still_runs_forced_replacement():
    _, merged_model, calls = run_replacement([], available={("AST", DataSource.OPDM): [igm("AST", name="old")]},
                                             model_replacement=False, replace_tso=["AST"], missing_models=["LITGRID"])

    assert [tso for tso, _, _ in calls] == ["AST"]
    assert merged_model.replaced is True


def test_run_replacement_continues_after_a_failing_request():
    def find_replacement_models(tso_list, **_):
        if tso_list == ["ELERING"]:
            raise RuntimeError("broken metadata")
        return [igm(tso_list[0], name="old")]

    merged_model = MergedModel()
    with mock.patch.object(replacement, "find_replacement_models", side_effect=find_replacement_models):
        result = replacement.run_replacement(
            igm_models=[], model_replacement=True, local_import_models=[], missing_local_import=[],
            missing_models=["ELERING", "AST"], replace_tso=[], time_horizon="1D", scenario_datetime=SCENARIO,
            merged_model=merged_model)

    assert ids(result) == ["AST-OPDM-old"]
    assert merged_model.replaced is True
