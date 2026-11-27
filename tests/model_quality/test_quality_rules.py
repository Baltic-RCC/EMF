from unittest import mock

import pandas as pd
import pytest

from emf.model_quality import quality_rules
from emf.model_quality.quality_rules import (check_crossborder_inconsistencies, check_generator_quality,
                                             check_line_impedance, check_line_limits, check_lt_pl_crossborder,
                                             check_outage_inconsistencies, check_reactive_power_limits,
                                             get_uap_outages_from_scenario_time)
from emf.model_quality.quality_functions import set_quality_flag

SCENARIO = {"pmd:timeHorizon": "1D", "pmd:scenarioDate": "2025-07-06T09:30:00Z"}


def cim_object(object_id, object_type, attributes):
    return [(object_id, "Type", object_type)] + [(object_id, key, str(value)) for key, value in attributes.items()]


def elastic_handler(docs_by_index):
    """Handler whose elastic_service.get_docs_by_query returns a DataFrame per index, or raises it if it's an exception"""
    def get_docs_by_query(index, **kwargs):
        result = docs_by_index[index]
        if isinstance(result, Exception):
            raise result
        return result.copy()

    handler = mock.MagicMock()
    handler.elastic_service.get_docs_by_query.side_effect = get_docs_by_query
    return handler


# --- Kruonis / Riga TEC generators

def generators(make_triplets, units):
    rows = []
    for number, (name, p) in enumerate(units):
        rows += cim_object(f"g{number}", "SynchronousMachine", {"IdentifiedObject.name": name, "RotatingMachine.p": p})
    return make_triplets(rows)


@pytest.mark.parametrize("p_values, running, check", [
    ([-200, -200, 0, 0], 2, True),
    ([-200, -200, -200, 0], 3, False),
    ([-200, -200, 150, 0], 3, False),  # pumping (p > 0 in load sign convention) counts as running
    ([0, 0, 0, 0], 0, True),
])
def test_kruonis_check_fails_when_three_or_more_units_run(make_triplets, p_values, running, check):
    units = [(f"KHAE_G{number}", p) for number, p in enumerate(p_values, start=1)] + [("OTHER_G1", -500)]

    report = check_generator_quality({}, generators(make_triplets, units))

    assert report["kruonis_generators"] == running
    assert report["kruonis_check"] is check
    assert report["kruonis_total_p"] == sum(abs(p) for p in p_values)


@pytest.mark.parametrize("p_values, running, check", [
    ([-100, -100, 0.000001], 2, True),
    ([-100, -100, -0.1], 3, False),
])
def test_rtec_check_ignores_units_with_negligible_output(make_triplets, p_values, running, check):
    units = [(f"RTEC{number}_G", p) for number, p in enumerate(p_values, start=1)]

    report = check_generator_quality({}, generators(make_triplets, units))

    assert report["rtec_generators"] == running
    assert report["rtec_check"] is check


def test_generator_checks_are_not_evaluated_without_kruonis_and_rtec(make_triplets):
    report = check_generator_quality({"existing": 1}, generators(make_triplets, [("OTHER_G1", -500)]))

    assert report == {"existing": 1, "kruonis_generators": None, "kruonis_check": None, "kruonis_total_p": None,
                      "rtec_generators": None, "rtec_check": None, "rtec_total_p": None}


# --- LT-PL cross-border flow

def lt_pl_tieflows(flow_1, flow_2):
    return pd.DataFrame({
        "cross_border": ["LT-PL", "LT-PL", "LT-PL", "LT-LV"],
        "IdentifiedObject.name_TieFlow": ["LT", "LT", "PL", "LT"],
        "IdentifiedObject.shortName_EquivalentInjection": ["XEL_AL11", "XEL_AL12", "XEL_AL11", "XEL_AL11"],
        "SvPowerFlow.p": [flow_1, flow_2, 9999.0, 9999.0],
    })


@pytest.mark.parametrize("flow_1, flow_2, expected_flow, check", [
    (200.0, 210.0, 205.0, True),
    (300.0, 280.0, 290.0, False),
    (-300.0, -280.0, -290.0, False),
    (0.0, 0.0, 0.0, True),
])
def test_lt_pl_flow_is_mean_of_lt_side_boundary_points_and_checked_against_border_limit(flow_1, flow_2,
                                                                                         expected_flow, check):
    report = check_lt_pl_crossborder({}, network=None, border_limit="250", tieflow_data=lt_pl_tieflows(flow_1, flow_2))

    assert report == {"lt_pl_flow": expected_flow, "lt_pl_xborder_check": check}


def test_lt_pl_check_is_not_evaluated_when_a_boundary_point_is_missing():
    tieflow_data = lt_pl_tieflows(100.0, 100.0).iloc[[0, 2, 3]]

    report = check_lt_pl_crossborder({}, network=None, border_limit="250", tieflow_data=tieflow_data)

    assert report == {"lt_pl_flow": None, "lt_pl_xborder_check": None}


@pytest.mark.parametrize("given_tieflow_data", [None, pd.DataFrame()])
def test_lt_pl_check_computes_tieflow_data_when_not_given(given_tieflow_data):
    network = mock.sentinel.network
    with mock.patch.object(quality_rules, "get_tieflow_data", return_value=lt_pl_tieflows(100.0, 120.0)) as get_tieflows:
        report = check_lt_pl_crossborder({}, network, border_limit="250", tieflow_data=given_tieflow_data)

    get_tieflows.assert_called_once_with(network)
    assert report["lt_pl_flow"] == 110.0


# --- cross-border connection status

def boundary_point(xnode, from_tso, to_tso, line_ends, boundary="true"):
    """X-node ConnectivityNode with one ACLineSegment end per TSO, line_ends: [(control area name, line name, connected)]"""
    rows = cim_object(f"cn_{xnode}", "ConnectivityNode", {
        "IdentifiedObject.name": xnode,
        "ConnectivityNode.boundaryPoint": boundary,
        "ConnectivityNode.fromEndNameTso": from_tso,
        "ConnectivityNode.toEndNameTso": to_tso,
    })
    for area, line_name, connected in line_ends:
        line = f"acl_{xnode}_{area}"
        rows += cim_object(line, "ACLineSegment", {"IdentifiedObject.name": line_name})
        rows += cim_object(f"t_{line}", "Terminal", {"IdentifiedObject.name": f"{line_name} T1",
                                                     "Terminal.ConductingEquipment": line,
                                                     "Terminal.ConnectivityNode": f"cn_{xnode}",
                                                     "ACDCTerminal.connected": str(connected).lower()})
        rows += cim_object(f"tf_{line}", "TieFlow", {"TieFlow.ControlArea": f"ca_{area}", "TieFlow.Terminal": f"t_{line}"})
    return rows


def control_areas(*names):
    return [row for name in names for row in cim_object(f"ca_{name}", "ControlArea", {"IdentifiedObject.name": name})]


def test_crossborder_line_connected_on_one_side_only_is_inconsistent(make_triplets):
    rows = control_areas("LT", "LV")
    rows += boundary_point("XLT_LV1", "Litgrid", "Augstsprieguma tikls", [("LT", "LT-LV 1", True), ("LV", "LV-LT 1", True)])
    rows += boundary_point("XLT_LV2", "Litgrid", "Augstsprieguma tikls", [("LT", "LT-LV 2", True), ("LV", "LV-LT 2", False)])

    report = check_crossborder_inconsistencies({}, make_triplets(rows))

    assert report["xborder_consistency_check"] is False
    assert report["xborder_inconsistencies"] == [{"xb_key": "XLT_LV2", "lines": [
        {"country": "LT", "name": "LT-LV 2", "connected": True},
        {"country": "LV", "name": "LV-LT 2", "connected": False},
    ]}]


@pytest.mark.parametrize("to_tso, boundary", [("Belenergo", "true"), ("Augstsprieguma tikls", "false")],
                         ids=["tso-not-in-check-list", "not-a-boundary-point"])
def test_crossborder_check_only_covers_boundary_points_between_listed_tsos(make_triplets, to_tso, boundary):
    rows = control_areas("LT", "BY")
    rows += boundary_point("XLT_BY1", "Litgrid", to_tso, [("LT", "LT-BY 1", True), ("BY", "BY-LT 1", False)],
                           boundary=boundary)

    report = check_crossborder_inconsistencies({}, make_triplets(rows))

    assert report == {"xborder_inconsistencies": [], "xborder_consistency_check": True}


def test_crossborder_check_is_not_evaluated_without_tieflows(make_triplets):
    rows = boundary_point("XLT_LV1", "Litgrid", "Augstsprieguma tikls", [("LT", "LT-LV 1", True)])
    data = make_triplets(rows).query("VALUE != 'TieFlow'")

    report = check_crossborder_inconsistencies({}, data)

    assert report == {"xborder_inconsistencies": None, "xborder_consistency_check": None}


# --- outages vs ELK

def outage(eic, name, start="2025-07-01T00:00:00Z", end="2025-07-10T00:00:00Z", outage_type="OUT",
           report_date="2025-07-05T08:00:00", last_change="2025-07-04T00:00:00"):
    return {"eic": eic, "name": name, "start_date": start, "end_date": end, "outage_type": outage_type,
            "reportParsedDate": report_date, "date_of_last_change": last_change, "Merge": "week"}


def eic_mrid_map(*eics):
    return pd.DataFrame({"eic": list(eics), "mrid": [f"_mrid_{eic}" for eic in eics]})


def outages_handler(outages):
    return elastic_handler({"opc-outages-baltics*": pd.DataFrame(outages)})


def test_outages_active_at_scenario_time_are_mapped_to_mrids():
    handler = outages_handler([
        outage("E1", "active"),
        outage("E2", "not started yet", start="2025-07-07T00:00:00Z"),
        outage("E3", "already ended", end="2025-07-06T09:00:00Z"),
        outage("E4", "no mrid"),
    ])

    result = get_uap_outages_from_scenario_time(handler, "1D", "2025-07-06T09:30:00Z", eic_mrid_map("E1", "E2", "E3"))

    mrids = result.set_index("eic")["mrid"]
    assert sorted(mrids.index) == ["E1", "E4"]
    assert mrids["E1"] == "_mrid_E1"
    assert pd.isna(mrids["E4"])


def test_outages_only_out_and_sss_types_from_latest_report_are_used():
    handler = outages_handler([
        outage("E1", "out"),
        outage("E2", "sss", outage_type="SSS"),
        outage("E3", "other type", outage_type="PLANNED"),
        outage("E4", "older report", report_date="2025-07-04T08:00:00"),
    ])

    result = get_uap_outages_from_scenario_time(handler, "1D", "2025-07-06T09:30:00Z", eic_mrid_map("E1", "E2", "E3", "E4"))

    assert sorted(result["eic"]) == ["E1", "E2"]


def test_outage_latest_change_wins_for_duplicated_element():
    handler = outages_handler([
        outage("E1", "old version", end="2025-07-05T00:00:00Z", last_change="2025-07-03T00:00:00"),
        outage("E1", "new version", end="2025-07-10T00:00:00Z", last_change="2025-07-04T12:00:00"),
        outage("E2", "new version ended", end="2025-07-05T00:00:00Z", last_change="2025-07-04T12:00:00"),
        outage("E2", "old version", end="2025-07-10T00:00:00Z", last_change="2025-07-03T00:00:00"),
    ])

    result = get_uap_outages_from_scenario_time(handler, "1D", "2025-07-06T09:30:00Z", eic_mrid_map("E1", "E2"))

    assert result[["eic", "name"]].to_dict("records") == [{"eic": "E1", "name": "new version"}]


def test_brell_lines_are_excluded_from_outages():
    brell_line = quality_rules.BRELL_LINES.split(",")[0]
    handler = outages_handler([outage("E1", "baltic line"), outage(brell_line, "BRELL line")])

    result = get_uap_outages_from_scenario_time(handler, "1D", "2025-07-06T09:30:00Z", eic_mrid_map("E1", brell_line))

    assert result["eic"].tolist() == ["E1"]


def test_no_outages_returns_empty_frame():
    handler = elastic_handler({"opc-outages-baltics*": pd.DataFrame()})

    result = get_uap_outages_from_scenario_time(handler, "1D", "2025-07-06T09:30:00Z", eic_mrid_map("E1"))

    assert result.empty
    assert {"eic", "name", "mrid"} <= set(result.columns)


@pytest.mark.parametrize("time_horizon, merge_types, lookback", [
    ("1D", ["week"], "now-2w"),
    ("2D", ["week"], "now-2w"),
    ("ID", ["week"], "now-2w"),
    ("09", ["week"], "now-2w"),
    ("WK", ["week"], "now-2w"),
    ("MO", ["week", "month"], "now-4w"),
    ("YR", ["year"], "now-4M"),
])
def test_outage_query_depends_on_time_horizon(time_horizon, merge_types, lookback):
    handler = elastic_handler({"opc-outages-baltics*": pd.DataFrame()})

    get_uap_outages_from_scenario_time(handler, time_horizon, "2025-07-06T09:30:00Z", eic_mrid_map("E1"))

    query = handler.elastic_service.get_docs_by_query.call_args.kwargs["query"]["bool"]
    assert {"terms": {"Merge": merge_types}} in query["must"]
    assert query["filter"][0]["range"]["reportParsedDate"]["gte"] == lookback


def test_outage_query_rejects_unknown_time_horizon():
    handler = elastic_handler({"opc-outages-baltics*": pd.DataFrame()})

    with pytest.raises(TypeError, match="Incorrect time horizon"):
        get_uap_outages_from_scenario_time(handler, "XX", "2025-07-06T09:30:00Z", eic_mrid_map("E1"))


def node_breaker_line(line, end_1_connected=True, end_2_connected=True):
    rows = cim_object(line, "ACLineSegment", {"IdentifiedObject.name": f"{line} name"})
    for end, connected in ((1, end_1_connected), (2, end_2_connected)):
        rows += cim_object(f"{line}_t{end}", "Terminal", {"IdentifiedObject.name": f"{line} T{end}",
                                                           "Terminal.ConductingEquipment": line,
                                                           "Terminal.ConnectivityNode": f"{line}_cn{end}",
                                                           "ACDCTerminal.connected": str(connected).lower()})
        rows += cim_object(f"{line}_cn{end}", "ConnectivityNode", {"IdentifiedObject.name": f"{line} N{end}"})
    return rows


@pytest.fixture
def outage_network(make_triplets):
    rows = node_breaker_line("L1")  # outage reported, but in service in the model
    rows += node_breaker_line("L2", end_2_connected=False)  # outage reported, open in the model
    rows += node_breaker_line("L3")  # no outage, in service
    rows += node_breaker_line("L4", end_2_connected=False)  # no outage, but open in the model
    rows += node_breaker_line("L5", False, False)  # not a critical element
    return make_triplets(rows)


def outage_check_handler(outages):
    critical_elements = pd.DataFrame({"eic": ["E1", "E2", "E3", "E4"], "mrid": ["_L1", "_L2", "_L3", "_L4"]})
    return elastic_handler({"config-network": critical_elements, "opc-outages-baltics*": pd.DataFrame(outages)})


def test_outage_inconsistencies_compare_model_status_with_reported_outages(outage_network):
    handler = outage_check_handler([outage("E1", "L1 outage"), outage("E2", "L2 outage")])

    report = check_outage_inconsistencies({}, outage_network, handler, SCENARIO)

    assert report["outage_check"] is False
    inconsistencies = {item["grid_id"]: item for item in report["outage_inconsistencies"]}
    assert inconsistencies.keys() == {"L1", "L4"}
    assert inconsistencies["L1"] == {"name": "L1 name", "grid_id": "L1",
                                     "line_end_1_connected": True, "line_end_2_connected": True}
    assert {inconsistencies["L4"]["line_end_1_connected"], inconsistencies["L4"]["line_end_2_connected"]} == {True, False}


def test_outage_check_passes_when_model_matches_outages(outage_network):
    handler = outage_check_handler([outage("E2", "L2 outage"), outage("E4", "L4 outage")])

    report = check_outage_inconsistencies({}, outage_network, handler, SCENARIO)

    assert report == {"outage_inconsistencies": [], "outage_check": True}


def test_outage_check_is_not_evaluated_when_elastic_fails(outage_network):
    handler = elastic_handler({"config-network": ConnectionError("elastic down")})

    report = check_outage_inconsistencies({}, outage_network, handler, SCENARIO)

    assert report == {"outage_inconsistencies": None, "outage_check": None}


# --- line impedance

def lines_network(make_triplets, lines, with_length=True):
    """lines: [(line id, r, x, nominal voltage)]"""
    rows = []
    for line, r, x, voltage in lines:
        attributes = {"IdentifiedObject.name": f"{line} name", "ACLineSegment.r": r, "ACLineSegment.x": x,
                      "ConductingEquipment.BaseVoltage": f"bv_{voltage}"}
        if with_length:
            attributes["Conductor.length"] = 10
        rows += cim_object(line, "ACLineSegment", attributes)
    for voltage in {line[3] for line in lines}:
        rows += cim_object(f"bv_{voltage}", "BaseVoltage", {"BaseVoltage.nominalVoltage": voltage})
    return make_triplets(rows)


@pytest.mark.parametrize("r, x, error, warning", [
    (1, 10, False, False),
    (1, 30, False, True),
    (1, 60, True, False),
    (1, 0.5, False, True),
    (1, 0.05, True, False),
    (0, 10, True, False),
])
def test_line_impedance_x_r_ratio_thresholds(make_triplets, r, x, error, warning):
    network = lines_network(make_triplets, [("L1", r, x, 330)])

    report = check_line_impedance({}, network)

    assert [item["grid_id"] for item in report["impedance_errors"]] == (["L1"] if error else [])
    assert [item["grid_id"] for item in report["impedance_warnings"]] == (["L1"] if warning else [])
    assert report["impedance_check"] is (not error)


def test_line_impedance_error_details(make_triplets):
    network = lines_network(make_triplets, [("L1", 1, 60, 330), ("L2", 1, 10, 330)])

    report = check_line_impedance({}, network)

    assert report["impedance_errors"] == [{"grid_id": "L1", "name": "L1 name", "type": "ACLineSegment",
                                           "r": 1, "x": 60, "x/r_ratio": 60}]


@pytest.mark.parametrize("voltage, checked", [(110, True), (330, True), (35, False)])
def test_line_impedance_only_checks_110kv_and_above(make_triplets, voltage, checked):
    network = lines_network(make_triplets, [("L1", 1, 60, voltage), ("L2", 1, 10, 330)])

    report = check_line_impedance({}, network)

    assert report["impedance_check"] is (not checked)


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="x/r check needs the optional Conductor.length although ACLineSegment r/x are totals, "
                          "so models without it are not checked")
def test_line_impedance_is_checked_without_conductor_length(make_triplets):
    network = lines_network(make_triplets, [("L1", 1, 60, 330)], with_length=False)

    report = check_line_impedance({}, network)

    assert report["impedance_check"] is False


def test_line_impedance_on_microgrid_be(microgrid_be_igm, microgrid_boundary):
    from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
    network = load_opdm_objects_to_triplets([microgrid_be_igm, microgrid_boundary])

    report = check_line_impedance({}, network)

    # BE-Line_1: r = 2.2, x = 68.2 ohm
    assert report["impedance_check"] is True
    assert [(item["name"], item["x/r_ratio"]) for item in report["impedance_warnings"]] == [("BE-Line_1", 31.0)]


# --- line limits vs ELK ratings

def limited_line(line, limit_end_1, limit_end_2):
    rows = cim_object(line, "ACLineSegment", {"IdentifiedObject.name": f"{line} name"})
    for end, limit in ((1, limit_end_1), (2, limit_end_2)):
        terminal, limit_set = f"{line}_t{end}", f"{line}_ols{end}"
        rows += cim_object(terminal, "Terminal", {"IdentifiedObject.name": f"{line} T{end}",
                                                  "Terminal.ConductingEquipment": line})
        rows += cim_object(limit_set, "OperationalLimitSet", {"IdentifiedObject.name": f"{line} limits {end}",
                                                              "OperationalLimitSet.Terminal": terminal})
        rows += cim_object(f"{line}_patl{end}", "CurrentLimit", {"IdentifiedObject.name": "PATL",
                                                                 "CurrentLimit.value": limit,
                                                                 "OperationalLimit.OperationalLimitSet": limit_set})
    return rows


def line_ratings_handler(ratings):
    return elastic_handler({"config-line-ratings": pd.DataFrame(ratings)})


@pytest.mark.parametrize("model_limit, mismatch", [(1000, False), (1009, False), (991, False), (1020, True), (900, True)])
def test_line_limit_must_match_rating_within_one_percent(make_triplets, model_limit, mismatch):
    network = make_triplets(limited_line("L1", model_limit, model_limit) + limited_line("L2", 500, 500))
    handler = line_ratings_handler({"grid_id": ["_L1", "_L2", "_L_unknown"], "25 C": [1000.0, 500.0, 700.0],
                                    "35 C": [1.0, 1.0, 1.0]})

    report = check_line_limits({}, network, handler, limit_temperature="25 C")

    expected = [{"IdentifiedObject.name_line": "L1 name", "grid_id": "L1", "set_limit": 1000.0,
                 "CurrentLimit.value1": model_limit}] if mismatch else []
    assert report == {"line_rating_mismatch": expected, "line_rating_check": not mismatch}


def test_line_limit_is_compared_with_rating_of_given_temperature(make_triplets):
    network = make_triplets(limited_line("L1", 1000, 1000))
    handler = line_ratings_handler({"grid_id": ["_L1"], "25 C": [1000.0], "35 C": [800.0]})

    report = check_line_limits({}, network, handler, limit_temperature="35 C")

    assert report["line_rating_check"] is False
    assert report["line_rating_mismatch"][0]["set_limit"] == 800.0


def test_line_limit_check_is_not_evaluated_when_elastic_fails(make_triplets):
    network = make_triplets(limited_line("L1", 1000, 1000))
    handler = elastic_handler({"config-line-ratings": ConnectionError("elastic down")})

    report = check_line_limits({}, network, handler)

    assert report == {"line_rating_mismatch": None, "line_rating_check": None}


# --- reactive power limits

def generator_grid(make_triplets, units):
    """units: [(name, area, nominal voltage, q, min_q, max_q, connected)], q in load sign convention (SSH)"""
    rows = []
    for area in {unit[1] for unit in units}:
        rows += cim_object(f"gr_{area}", "GeographicalRegion", {"IdentifiedObject.name": area})
        rows += cim_object(f"sgr_{area}", "SubGeographicalRegion", {"IdentifiedObject.name": f"{area} region",
                                                                    "SubGeographicalRegion.Region": f"gr_{area}"})
        rows += cim_object(f"ss_{area}", "Substation", {"IdentifiedObject.name": f"{area} substation",
                                                        "Substation.Region": f"sgr_{area}"})
    for voltage in {unit[2] for unit in units}:
        rows += cim_object(f"bv_{voltage}", "BaseVoltage", {"BaseVoltage.nominalVoltage": voltage})
    for name, area, voltage, q, min_q, max_q, connected in units:
        rows += cim_object(f"vl_{name}", "VoltageLevel", {"IdentifiedObject.name": f"{name} VL",
                                                          "VoltageLevel.Substation": f"ss_{area}",
                                                          "VoltageLevel.BaseVoltage": f"bv_{voltage}"})
        rows += cim_object(f"cn_{name}", "ConnectivityNode", {"IdentifiedObject.name": f"{name} node",
                                                              "ConnectivityNode.ConnectivityNodeContainer": f"vl_{name}"})
        rows += cim_object(f"sm_{name}", "SynchronousMachine", {"IdentifiedObject.name": name, "RotatingMachine.q": q,
                                                                "SynchronousMachine.minQ": min_q,
                                                                "SynchronousMachine.maxQ": max_q})
        rows += cim_object(f"t_{name}", "Terminal", {"IdentifiedObject.name": f"{name} T1",
                                                     "Terminal.ConductingEquipment": f"sm_{name}",
                                                     "Terminal.ConnectivityNode": f"cn_{name}",
                                                     "ACDCTerminal.connected": str(connected).lower()})
    return make_triplets(rows)


def test_reactive_power_violations_exclude_pl_low_voltage_and_disconnected_units(make_triplets):
    # symmetric limits and large deviations, so the result does not depend on the q sign convention
    network = generator_grid(make_triplets, [
        ("G_VIOLATION", "LT", 330, -150, -100, 100, True),
        ("G_OK", "LT", 330, -20, -100, 100, True),
        ("G_PL", "PL", 330, -500, -100, 100, True),
        ("G_10KV", "LT", 10, -150, -100, 100, True),
        ("G_OFF", "LT", 330, -500, -100, 100, False),
    ])

    report = check_reactive_power_limits({}, network)

    assert [item["name"] for item in report["q_limit_errors"]] == ["G_VIOLATION"]
    assert report["q_limit_errors"][0]["area"] == "LT"
    assert report["sum_min_q_limit"] == -300
    assert report["sum_max_q_limit"] == 300
    assert not report["reactive_power_check"]


def test_reactive_power_check_passes_within_limits(make_triplets):
    network = generator_grid(make_triplets, [("G1", "LT", 330, -20, -100, 100, True),
                                             ("G2", "LV", 330, 10, -100, 100, True)])

    report = check_reactive_power_limits({}, network)

    assert report["q_limit_errors"] == []
    assert report["reactive_power_check"]


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="RotatingMachine.q (load sign convention) is compared with minQ/maxQ (generator convention) "
                          "without negation, QoCDC GenReactivePowerInfeedLim")
@pytest.mark.parametrize("q, violation", [(-80, False), (50, True)],
                         ids=["producing-80-within-limits", "absorbing-50-below-min-q"])
def test_reactive_power_limits_use_generator_sign_convention(make_triplets, q, violation):
    network = generator_grid(make_triplets, [("G1", "LT", 330, q, -30, 100, True)])

    report = check_reactive_power_limits({}, network)

    assert [item["name"] for item in report["q_limit_errors"]] == (["G1"] if violation else [])
    assert bool(report["reactive_power_check"]) is (not violation)


def test_reactive_power_check_is_not_evaluated_without_regions(make_triplets):
    network = generator_grid(make_triplets, [("G1", "LT", 330, -20, -100, 100, True)])
    network = network[~network["ID"].str.startswith("gr_")]

    report = check_reactive_power_limits({}, network)

    assert report["reactive_power_check"] is None
    assert report["q_limit_errors"] is None


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="set_quality_flag checks flags with 'is True', the numpy bool of reactive_power_check rates bad")
def test_passing_reactive_power_check_rates_model_good(make_triplets):
    network = generator_grid(make_triplets, [("G1", "LT", 330, -20, -100, 100, True)])
    report = check_reactive_power_limits({}, network)

    report = set_quality_flag(report, "CGM", {"cgm_rule_set": ["reactive_power"]})

    assert report["quality"] == "good"
