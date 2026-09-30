import json
from datetime import datetime, timezone

import pytest
from lxml import etree

from emf.common.converters import iec_schedule_to_ndjson

NAMESPACE = "urn:iec62325.351:tc57wg16:451-2:scheduledocument:5:0"
REASON = "<Reason><code>A95</code><text>Complementary information</text></Reason>"
TIME_FIELDS = {"value", "position", "utc_start", "utc_end"}


def as_utc(timestamp: str) -> datetime:
    parsed = datetime.fromisoformat(timestamp)
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def period(start: str, end: str, resolution: str, *points: tuple) -> str:
    point_xml = "".join(f"<Point><position>{position}</position><quantity>{quantity}</quantity></Point>"
                        for position, quantity in points)
    return (f"<Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval>"
            f"<resolution>{resolution}</resolution>{point_xml}</Period>")


def time_series(*periods: str, mrid: str = "TS-1", curve_type: str = "A01", reason: str = REASON, extra: str = "") -> str:
    return (f"<TimeSeries><mRID>{mrid}</mRID><businessType>B63</businessType><curveType>{curve_type}</curveType>"
            f"<measure_Unit.name>MAW</measure_Unit.name>{extra}{''.join(periods)}{reason}</TimeSeries>")


def schedule_document(*series: str, header_extra: str = "") -> bytes:
    """Minimal IEC 62325-451-2 Schedule_MarketDocument"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Schedule_MarketDocument xmlns="{NAMESPACE}">
  <mRID>SCHEDULE-1</mRID>
  <revisionNumber>3</revisionNumber>
  <type>A01</type>
  <process.processType>A17</process.processType>
  <sender_MarketParticipant.mRID codingScheme="A01">10X-TEST-SENDER</sender_MarketParticipant.mRID>
  <createdDateTime>2025-01-01T12:00:00Z</createdDateTime>
  <schedule_Time_Period.timeInterval><start>2025-01-01T23:00Z</start><end>2025-01-02T23:00Z</end></schedule_Time_Period.timeInterval>
  <docStatus><value>A02</value></docStatus>{header_extra}
  {''.join(series)}
</Schedule_MarketDocument>""".encode()


def one_period_document(resolution: str = "PT15M", *points: tuple, curve_type: str = "A01",
                        start: str = "2025-01-01T23:00Z", end: str = "2025-01-02T00:00Z") -> bytes:
    return schedule_document(time_series(period(start, end, resolution, *points), curve_type=curve_type))


def test_convert_returns_json_array_of_rows_and_content_type():
    document = one_period_document("PT15M", (1, 10), (2, 20.5))

    content, content_type = iec_schedule_to_ndjson.convert(document)

    assert content_type == "text/json"
    assert isinstance(content, bytes)
    assert json.loads(content) == iec_schedule_to_ndjson.parse_iec_xml(document)
    assert [row["value"] for row in json.loads(content)] == [10.0, 20.5]


@pytest.mark.parametrize("resolution, points, expected", [
    ("PT15M", [(1, 10), (2, 20), (3, 30)], [
        (1, "2025-01-01T23:00:00+00:00", "2025-01-01T23:15:00+00:00", 10.0),
        (2, "2025-01-01T23:15:00+00:00", "2025-01-01T23:30:00+00:00", 20.0),
        (3, "2025-01-01T23:30:00+00:00", "2025-01-01T23:45:00+00:00", 30.0),
    ]),
    ("PT30M", [(1, 5), (2, 6)], [
        (1, "2025-01-01T23:00:00+00:00", "2025-01-01T23:15:00+00:00", 5.0),
        (2, "2025-01-01T23:15:00+00:00", "2025-01-01T23:30:00+00:00", 5.0),
        (3, "2025-01-01T23:30:00+00:00", "2025-01-01T23:45:00+00:00", 6.0),
        (4, "2025-01-01T23:45:00+00:00", "2025-01-02T00:00:00+00:00", 6.0),
    ]),
    ("PT60M", [(1, 7.5)], [
        (1, "2025-01-01T23:00:00+00:00", "2025-01-01T23:15:00+00:00", 7.5),
        (2, "2025-01-01T23:15:00+00:00", "2025-01-01T23:30:00+00:00", 7.5),
        (3, "2025-01-01T23:30:00+00:00", "2025-01-01T23:45:00+00:00", 7.5),
        (4, "2025-01-01T23:45:00+00:00", "2025-01-02T00:00:00+00:00", 7.5),
    ]),
])
def test_parse_iec_xml_splits_points_into_15_minute_rows(resolution, points, expected):
    rows = iec_schedule_to_ndjson.parse_iec_xml(one_period_document(resolution, *points))

    assert [(row["position"], row["utc_start"], row["utc_end"], row["value"]) for row in rows] == expected


@pytest.mark.parametrize("resolution, expected", [
    ("PT15M", [(1, "2025-01-01T23:00:00", "2025-01-01T23:15:00"), (3, "2025-01-01T23:30:00", "2025-01-01T23:45:00")]),
    ("PT60M", [(1, "2025-01-01T23:00:00", "2025-01-02T00:00:00"), (3, "2025-01-02T01:00:00", "2025-01-02T02:00:00")]),
])
def test_parse_iec_xml_without_mtu_split_returns_one_row_per_point(resolution, expected):
    document = one_period_document(resolution, (1, 10), (3, 30), end="2025-01-02T02:00Z")

    rows = iec_schedule_to_ndjson.parse_iec_xml(document, return_values_per_mtu=False)

    assert [(row["position"], as_utc(row["utc_start"]), as_utc(row["utc_end"])) for row in rows] == [
        (position, as_utc(start), as_utc(end)) for position, start, end in expected]
    assert [row["value"] for row in rows] == [10.0, 30.0]


def test_parse_iec_xml_a03_curve_holds_value_until_next_point_or_period_end():
    document = one_period_document("PT60M", (1, 5), (3, 7), curve_type="A03", end="2025-01-02T03:00Z")

    per_point = iec_schedule_to_ndjson.parse_iec_xml(document, return_values_per_mtu=False)
    per_mtu = iec_schedule_to_ndjson.parse_iec_xml(document)

    assert [(row["position"], as_utc(row["utc_start"]), as_utc(row["utc_end"]), row["value"]) for row in per_point] == [
        (1, as_utc("2025-01-01T23:00:00"), as_utc("2025-01-02T01:00:00"), 5.0),
        (3, as_utc("2025-01-02T01:00:00"), as_utc("2025-01-02T03:00:00"), 7.0),
    ]
    assert [row["position"] for row in per_mtu] == list(range(1, 17))
    assert [row["value"] for row in per_mtu] == [5.0] * 8 + [7.0] * 8
    assert per_mtu[8]["utc_start"] == "2025-01-02T01:00:00+00:00"
    assert per_mtu[-1]["utc_end"] == "2025-01-02T03:00:00+00:00"


def test_parse_iec_xml_flattens_header_status_time_series_period_and_reason_onto_each_row():
    rows = iec_schedule_to_ndjson.parse_iec_xml(one_period_document("PT15M", (1, 10), (2, 20)))

    expected_metadata = {
        "root": "Schedule_MarketDocument",
        "namespace": NAMESPACE,
        "mRID": "SCHEDULE-1",
        "revisionNumber": "3",
        "type": "A01",
        "process.processType": "A17",
        "sender_MarketParticipant.mRID": "10X-TEST-SENDER",
        "createdDateTime": "2025-01-01T12:00:00Z",
        "docStatus.value": "A02",
        "TimeSeries.mRID": "TS-1",
        "TimeSeries.businessType": "B63",
        "TimeSeries.curveType": "A01",
        "TimeSeries.measure_Unit.name": "MAW",
        "Period.resolution": "PT15M",
        "Reason.code": "A95",
        "Reason.text": "Complementary information",
    }
    assert len(rows) == 2
    for row in rows:
        assert {key: value for key, value in row.items() if key not in TIME_FIELDS} == expected_metadata


def test_parse_iec_xml_takes_metadata_from_the_time_series_of_each_period():
    document = schedule_document(
        time_series(period("2025-01-01T23:00Z", "2025-01-01T23:15Z", "PT15M", (1, 1)), mrid="TS-A"),
        time_series(period("2025-01-01T23:00Z", "2025-01-01T23:15Z", "PT15M", (1, 2)), mrid="TS-B", reason=""),
    )

    rows = iec_schedule_to_ndjson.parse_iec_xml(document)

    assert [(row["TimeSeries.mRID"], row["value"]) for row in rows] == [("TS-A", 1.0), ("TS-B", 2.0)]
    assert "Reason.code" in rows[0]
    assert "Reason.code" not in rows[1]


@pytest.mark.parametrize("xml, kwargs, expected", [
    (b"<root xmlns='urn:x'><a>1</a><b v='2'/><nested><c>3</c></nested></root>", {},
     {"root": "root", "namespace": "urn:x", "a": "1", "b": "2"}),
    (b"<root><a>1</a></root>", {}, {"root": "root", "namespace": "", "a": "1"}),
    (b"<Period xmlns='urn:x'><resolution>PT15M</resolution></Period>", {"include_namespace": False, "prefix_root": True},
     {"Period.resolution": "PT15M"}),
])
def test_get_metadata_from_xml_reads_leaf_text_or_legacy_v_attribute(xml, kwargs, expected):
    assert iec_schedule_to_ndjson.get_metadata_from_xml(etree.fromstring(xml), **kwargs) == expected


def test_get_metadata_from_xml_returns_empty_dict_for_missing_element():
    assert iec_schedule_to_ndjson.get_metadata_from_xml(None) == {}


@pytest.mark.parametrize("flat, expected", [
    ({"a": 1}, {"a": 1}),
    ({"TimeSeries.mRID": "TS-1", "TimeSeries.curveType": "A01"}, {"TimeSeries": {"mRID": "TS-1", "curveType": "A01"}}),
    ({"a.b.c": 1, "a.d": 2, "e": 3}, {"a": {"b": {"c": 1}, "d": 2}, "e": 3}),
])
def test_expand_dotted_keys_nests_dotted_names(flat, expected):
    assert iec_schedule_to_ndjson.expand_dotted_keys(flat) == expected


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="timeInterval UTC offsets are dropped with replace(tzinfo=None), local times end up labelled as UTC")
def test_parse_iec_xml_converts_offset_times_to_utc():
    document = one_period_document("PT15M", (1, 10), (2, 20), start="2025-01-02T00:00+01:00", end="2025-01-02T00:30+01:00")

    rows = iec_schedule_to_ndjson.parse_iec_xml(document)

    assert [row["utc_start"] for row in rows] == ["2025-01-01T23:00:00+00:00", "2025-01-01T23:15:00+00:00"]


@pytest.mark.parametrize("header_extra, series_extra", [
    pytest.param("<!-- header comment -->", "", id="in-header"),
    pytest.param("", "<!-- time series comment -->", id="in-time-series"),
])
@pytest.mark.xfail(strict=True, raises=AttributeError,
                   reason="get_metadata_from_xml calls .split on the tag of comment nodes, any XML comment crashes the parser")
def test_parse_iec_xml_ignores_xml_comments(header_extra, series_extra):
    document = schedule_document(
        time_series(period("2025-01-01T23:00Z", "2025-01-01T23:15Z", "PT15M", (1, 10)), extra=series_extra),
        header_extra=header_extra,
    )

    rows = iec_schedule_to_ndjson.parse_iec_xml(document)

    assert [(row["mRID"], row["TimeSeries.mRID"], row["value"]) for row in rows] == [("SCHEDULE-1", "TS-1", 10.0)]


@pytest.mark.xfail(strict=True, reason="bare except in convert() swallows the parse error and returns None, callers then fail unpacking it")
def test_convert_raises_on_malformed_xml():
    with pytest.raises(Exception):
        iec_schedule_to_ndjson.convert(b"<Schedule_MarketDocument><mRID>")
