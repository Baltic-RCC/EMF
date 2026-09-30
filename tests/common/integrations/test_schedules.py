import json
import math
import types
from unittest import mock

import pandas as pd
import pytest

import config
from emf.common.integrations.object_storage import schedules

AREAS = [
    {"area.eic": "10Y-AA", "area.code": "AA", "party.name": "TSO_A"},
    {"area.eic": "10Y-BB", "area.code": "BB", "party.name": "TSO_B"},
    {"area.eic": "10Y-CC", "area.code": "CC", "party.name": "TSO_C"},
]
AREA_EIC_MAP = {area["area.eic"]: area["area.code"] for area in AREAS}
AREA_NAME_MAP = {area["area.code"]: area["party.name"] for area in AREAS}
TIMESTAMP = "2024-05-02T10:30:00"
PREVIOUS_DAY = "2024-05-01T10:30:00"


def _schedule(out_area, in_area, value, mrid="doc", revision="1", reason="A88", **extra):
    return {"TimeSeries.out_Domain.mRID": f"10Y-{out_area}", "TimeSeries.in_Domain.mRID": f"10Y-{in_area}",
            "value": value, "mRID": mrid, "revisionNumber": revision, "Reason.code": reason, **extra}


class FakeElastic:
    """Serves schedules by (time horizon, docStatus.value, utc_start) and records every query"""

    def __init__(self, responses=None, hvdc_lines=None):
        self.responses = responses or {}
        self.hvdc_lines = hvdc_lines
        self.queries = []

    def query_schedules_from_elk(self, index, utc_start, utc_end, metadata, period_overlap=False):
        self.queries.append({"utc_start": utc_start, "utc_end": utc_end, "period_overlap": period_overlap, **metadata})
        rows = self.responses.get((metadata["@time_horizon"], metadata.get(schedules.DOC_STATUS_FIELD), utc_start))
        return pd.DataFrame(rows) if rows else None

    def get_docs_by_query(self, index, query, size=None):
        if self.hvdc_lines is None:
            raise RuntimeError("index not found")
        return pd.DataFrame(self.hvdc_lines)

    @property
    def steps(self):
        return [(query["@time_horizon"], query.get(schedules.DOC_STATUS_FIELD), query["utc_start"]) for query in self.queries]


@pytest.fixture
def areas_config(tmp_path):
    path = tmp_path / "config_areas_mapping.json"
    path.write_text(json.dumps(AREAS))
    with mock.patch.object(config.paths.cgm_worker, "config_areas_mapping", path):
        yield


def _merged_model():
    return types.SimpleNamespace(acnp_schedule_replaced=None, acnp_schedule_replaced_entity=[], acnp_schedule_missing=[])


def _parties(df):
    return sorted(zip(df["TimeSeries.out_Domain.party"], df["TimeSeries.in_Domain.party"], df["value"]))


@pytest.mark.parametrize("ac_schedules, expected", [
    ([{"value": 100, "TimeSeries.in_Domain.party": "TSO_B", "TimeSeries.out_Domain.party": "TSO_A"},
      {"value": 30, "TimeSeries.in_Domain.party": "TSO_A", "TimeSeries.out_Domain.party": "TSO_C"}],
     {"TSO_A": 70, "TSO_B": -100, "TSO_C": 30}),
    ([{"value": 50, "TimeSeries.in_Domain.party": None, "TimeSeries.out_Domain.party": "TSO_A"}], {"TSO_A": 50}),
    ([], None),
    (None, None),
])
def test_calculate_ac_net_position(ac_schedules, expected):
    assert schedules.calculate_ac_net_position(ac_schedules) == expected


def test_fetch_acnp_schedules_keeps_accepted_latest_revisions_and_maps_parties():
    service = FakeElastic({("1D", "A02", TIMESTAMP): [
        _schedule("AA", "BB", 100, mrid="doc1", revision="1"),
        _schedule("AA", "BB", 110, mrid="doc1", revision="2"),
        _schedule("CC", "AA", 5, mrid="doc2", reason="A30"),
        _schedule("BB", "CC", 20, mrid="doc3", reason=None),
    ]})

    result = schedules._fetch_acnp_schedules(service, "1D", TIMESTAMP, AREA_EIC_MAP, AREA_NAME_MAP,
                                             status_field=schedules.DOC_STATUS_FIELD, status_value="A02")

    assert _parties(result) == [("TSO_A", "TSO_B", 110), ("TSO_B", "TSO_C", 20)]
    assert set(result["in_domain"]) == {"BB", "CC"}
    assert service.queries == [{"utc_start": TIMESTAMP, "utc_end": "2024-05-02T10:45:00", "period_overlap": True,
                                "@time_horizon": "1D", "TimeSeries.businessType": "B64", schedules.DOC_STATUS_FIELD: "A02"}]


@pytest.mark.parametrize("rows", [None, [_schedule("AA", "BB", 1, reason="A30")]], ids=["no-schedules", "only-rejected"])
def test_fetch_acnp_schedules_returns_none_without_accepted_schedules(rows):
    service = FakeElastic({("ID", None, TIMESTAMP): rows})

    assert schedules._fetch_acnp_schedules(service, "ID", TIMESTAMP, AREA_EIC_MAP, AREA_NAME_MAP) is None
    assert schedules.DOC_STATUS_FIELD not in service.queries[0]


def _available(*flows):
    """Schedules already found for the main query, as (out TSO, in TSO) pairs"""
    return pd.DataFrame([{"value": 1, "TimeSeries.out_Domain.party": out_party, "TimeSeries.in_Domain.party": in_party}
                         for out_party, in_party in flows])


C_FLOWS = [_schedule("CC", "AA", 40, mrid="c1"), _schedule("AA", "CC", 10, mrid="c2")]


@pytest.mark.parametrize("time_horizon, responses, expected_steps, replaced_from", [
    ("1D", {("2D", "A02", TIMESTAMP): C_FLOWS},
     [("1D", "A01", TIMESTAMP), ("2D", "A02", TIMESTAMP)], ("2D", 0)),
    ("1D", {("1D", "A02", PREVIOUS_DAY): C_FLOWS},
     [("1D", "A01", TIMESTAMP), ("2D", "A02", TIMESTAMP), ("1D", "A02", PREVIOUS_DAY)], ("1D", -1)),
    ("1D", {("1D", "A01", TIMESTAMP): C_FLOWS, ("2D", "A02", TIMESTAMP): C_FLOWS},
     [("1D", "A01", TIMESTAMP)], ("1D", 0)),
    ("2D", {("1D", "A02", TIMESTAMP): C_FLOWS},
     [("2D", "A01", TIMESTAMP), ("1D", "A02", TIMESTAMP)], ("1D", 0)),
    ("ID", {("1D", "A02", TIMESTAMP): C_FLOWS},
     [("1D", "A02", TIMESTAMP)], ("1D", 0)),
], ids=["1D-cgma-final", "1D-previous-day", "1D-pevf-preliminary", "2D-pevf-final", "ID-pevf-final"])
def test_replace_missing_acnp_schedules_follows_replacement_chain(time_horizon, responses, expected_steps, replaced_from):
    service = FakeElastic(responses)
    merged_model = _merged_model()

    result = schedules.replace_missing_acnp_schedules(_available(("TSO_A", "TSO_B"), ("TSO_B", "TSO_A")), service,
                                                      time_horizon, TIMESTAMP, AREA_EIC_MAP, AREA_NAME_MAP,
                                                      merged_model=merged_model)

    assert service.steps == expected_steps
    assert ("TSO_C", "TSO_A", 40) in _parties(result) and ("TSO_A", "TSO_C", 10) in _parties(result)
    assert merged_model.acnp_schedule_replaced is True
    assert merged_model.acnp_schedule_missing == []
    assert [(e["tso"], e["time_horizon"], e["day_offset"]) for e in merged_model.acnp_schedule_replaced_entity] == [
        ("TSO_C", *replaced_from)]


def test_replace_missing_acnp_schedules_reports_tsos_without_replacement(caplog):
    merged_model = _merged_model()
    available = _available(("TSO_A", "TSO_B"), ("TSO_B", "TSO_A"))

    result = schedules.replace_missing_acnp_schedules(available, FakeElastic(), "1D", TIMESTAMP, AREA_EIC_MAP,
                                                      AREA_NAME_MAP, merged_model=merged_model)

    assert len(result) == 2
    assert merged_model.acnp_schedule_missing == ["TSO_C"]
    assert merged_model.acnp_schedule_replaced is False
    assert "No replacement ACNP schedules found for: ['TSO_C']" in caplog.text


@pytest.mark.parametrize("time_horizon, flows", [
    ("1D", [("TSO_A", "TSO_B"), ("TSO_B", "TSO_C"), ("TSO_C", "TSO_A")]),
    ("2D", [("TSO_A", "TSO_B"), ("TSO_C", "TSO_A")]),
], ids=["1D-all-directions", "2D-either-direction"])
def test_replace_missing_acnp_schedules_does_nothing_when_all_tsos_present(time_horizon, flows):
    service = FakeElastic()
    merged_model = _merged_model()

    result = schedules.replace_missing_acnp_schedules(_available(*flows), service, time_horizon, TIMESTAMP,
                                                      AREA_EIC_MAP, AREA_NAME_MAP, merged_model=merged_model)

    assert len(result) == len(flows)
    assert service.queries == []
    assert merged_model.acnp_schedule_replaced is None


def test_replace_missing_acnp_schedules_has_no_chain_for_other_horizons():
    service = FakeElastic()
    available = _available(("TSO_A", "TSO_B"))

    result = schedules.replace_missing_acnp_schedules(available, service, "WK", TIMESTAMP, AREA_EIC_MAP, AREA_NAME_MAP)

    assert result is available
    assert service.queries == []


@pytest.mark.parametrize("time_horizon, main_status", [("1D", "A02"), ("2D", "A02"), ("ID", None)])
def test_query_acnp_schedules_main_query_and_output(areas_config, time_horizon, main_status):
    service = FakeElastic({(time_horizon, main_status, TIMESTAMP): [
        _schedule("AA", "BB", 100), _schedule("BB", "CC", 20), _schedule("CC", "AA", 5)]})

    with mock.patch.object(schedules.elastic, "Elastic", return_value=service):
        result = schedules.query_acnp_schedules(time_horizon, TIMESTAMP)

    assert service.steps == [(time_horizon, main_status, TIMESTAMP)]
    assert sorted(result, key=lambda row: row["value"]) == [
        {"value": 5, "in_domain": "AA", "out_domain": "CC", "TimeSeries.in_Domain.party": "TSO_A", "TimeSeries.out_Domain.party": "TSO_C"},
        {"value": 20, "in_domain": "CC", "out_domain": "BB", "TimeSeries.in_Domain.party": "TSO_C", "TimeSeries.out_Domain.party": "TSO_B"},
        {"value": 100, "in_domain": "BB", "out_domain": "AA", "TimeSeries.in_Domain.party": "TSO_B", "TimeSeries.out_Domain.party": "TSO_A"},
    ]


def test_query_acnp_schedules_uses_replacements_when_main_query_is_empty(areas_config):
    service = FakeElastic({("1D", "A01", TIMESTAMP): [_schedule("AA", "BB", 100), _schedule("BB", "CC", 20),
                                                      _schedule("CC", "AA", 5)]})
    merged_model = _merged_model()

    with mock.patch.object(schedules.elastic, "Elastic", return_value=service):
        result = schedules.query_acnp_schedules("1D", TIMESTAMP, merged_model=merged_model)

    assert sorted(row["value"] for row in result) == [5, 20, 100]
    assert merged_model.acnp_schedule_replaced is True


def test_query_acnp_schedules_returns_none_without_any_schedules(areas_config):
    with mock.patch.object(schedules.elastic, "Elastic", return_value=FakeElastic()):
        assert schedules.query_acnp_schedules("ID", TIMESTAMP) is None


HVDC_LINES = [{"IdentifiedObject.energyIdentCodeEic": "10T-HVDC-1", "IdentifiedObject.description": "Link 1"}]


@pytest.mark.parametrize("time_horizon, business_type", [("1D", "B63"), ("ID", "B63"), ("2D", "B67")])
def test_query_hvdc_schedules_maps_areas_and_line_names(areas_config, time_horizon, business_type):
    rows = [_schedule("AA", "BB", 300, mrid="doc1", revision="1", **{"TimeSeries.connectingLine_RegisteredResource.mRID": "10T-HVDC-1"}),
            _schedule("AA", "BB", 350, mrid="doc1", revision="2", **{"TimeSeries.connectingLine_RegisteredResource.mRID": "10T-HVDC-1"})]
    service = FakeElastic({(time_horizon, None, TIMESTAMP): rows}, hvdc_lines=HVDC_LINES)

    with mock.patch.object(schedules.elastic, "Elastic", return_value=service):
        result = schedules.query_hvdc_schedules(time_horizon, TIMESTAMP)

    assert service.queries[0]["TimeSeries.businessType"] == business_type
    assert result == [{"value": 350, "in_domain": "BB", "out_domain": "AA", "registered_resource": "10T-HVDC-1",
                       "hvdc_name": "Link 1"}]


def test_query_hvdc_schedules_without_line_mapping_leaves_names_empty(areas_config, caplog):
    rows = [_schedule("AA", "BB", 300, **{"TimeSeries.connectingLine_RegisteredResource.mRID": "10T-HVDC-1"})]
    service = FakeElastic({("1D", None, TIMESTAMP): rows}, hvdc_lines=None)

    with mock.patch.object(schedules.elastic, "Elastic", return_value=service):
        result = schedules.query_hvdc_schedules("1D", TIMESTAMP)

    assert math.isnan(result[0]["hvdc_name"])
    assert "HVDC line mapping configuration retrieval failed" in caplog.text


def test_query_hvdc_schedules_returns_none_without_schedules(areas_config):
    with mock.patch.object(schedules.elastic, "Elastic", return_value=FakeElastic(hvdc_lines=HVDC_LINES)):
        assert schedules.query_hvdc_schedules("1D", TIMESTAMP) is None
