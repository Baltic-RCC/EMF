import datetime
from pathlib import Path

import pytest
from lxml import etree

from emf.model_validator.validator_functions import get_lvl8_report_igm

QAR_SCHEMA = etree.XMLSchema(etree.parse(str(Path(__file__).resolve().parents[2] / "emf/common/schemas/QAR_v2.12.0.xsd")))
NS = {"qa": "http://entsoe.eu/checks"}


def validation_report(status_text="Converged", timestamp="2025-07-06T09:41:27.123456"):
    """Validation report as HandlerModelsValidator builds it, '@timestamp' is added by Elastic.send_to_elastic"""
    return {
        "@timestamp": timestamp,
        "@scenario_timestamp": "2025-07-06T09:30:00Z",
        "@time_horizon": "1D",
        "@version": 3,
        "fullModel_ID": "3f6b5e3e-2a4c-4d8e-9d8b-0e8a7c6f1a21",
        "tso": "AST",
        "loadflow": {"status": 0 if status_text == "Converged" else 1, "status_text": status_text},
    }


def parse(report: bytes):
    return etree.fromstring(report)


@pytest.mark.parametrize("status_text", ["Converged", "Max iteration reached", "Failed"])
def test_lvl8_report_is_valid_against_qar_schema(status_text):
    report = get_lvl8_report_igm(validation_report(status_text))

    QAR_SCHEMA.assertValid(parse(report))


def test_lvl8_report_starts_with_xml_declaration():
    assert get_lvl8_report_igm(validation_report()).startswith(b"<?xml")


@pytest.mark.parametrize("status_text, quality_indicator", [
    ("Converged", "Valid"),
    ("Max iteration reached", "Invalid - inconsistent data"),
    ("Failed", "Invalid - inconsistent data"),
])
def test_lvl8_quality_indicator_follows_loadflow_convergence(status_text, quality_indicator):
    igm = parse(get_lvl8_report_igm(validation_report(status_text))).find("qa:IGM", NS)

    assert igm.get("qualityIndicator") == quality_indicator


def test_lvl8_converged_model_has_no_rule_violations():
    igm = parse(get_lvl8_report_igm(validation_report("Converged"))).find("qa:IGM", NS)

    assert igm.findall("qa:RuleViolation", NS) == []


def test_lvl8_non_converged_model_reports_igm_convergence_error():
    igm = parse(get_lvl8_report_igm(validation_report("Max iteration reached"))).find("qa:IGM", NS)

    violations = igm.findall("qa:RuleViolation", NS)
    assert [(v.get("ruleId"), v.get("validationLevel"), v.get("severity")) for v in violations] == [("IGMConvergence", "8", "ERROR")]
    assert violations[0].findtext("qa:Message", namespaces=NS)


def test_lvl8_report_identifies_the_model():
    root = parse(get_lvl8_report_igm(validation_report()))
    igm = root.find("qa:IGM", NS)

    assert root.get("serviceProvider") == "BALTICRCC"
    assert (igm.get("tso"), igm.get("version"), igm.get("processType")) == ("AST", "3", "1D")
    assert igm.findtext("qa:resource", namespaces=NS) == "3f6b5e3e-2a4c-4d8e-9d8b-0e8a7c6f1a21"


def test_lvl8_timestamps_are_utc_with_z_suffix():
    root = parse(get_lvl8_report_igm(validation_report()))
    igm = root.find("qa:IGM", NS)

    assert root.get("created") == "2025-07-06T09:41:27Z"
    assert igm.get("created") == "2025-07-06T09:41:27Z"
    assert igm.get("scenarioTime") == "2025-07-06T09:30:00Z"


@pytest.mark.xfail(strict=True, raises=ValueError,
                   reason="'@timestamp' is parsed with a mandatory '.%f', but isoformat() omits microseconds when they are 0")
def test_lvl8_report_for_timestamp_without_microseconds():
    timestamp = datetime.datetime(2025, 7, 6, 9, 41, 27).isoformat(sep="T")  # as Elastic.send_to_elastic stamps it

    root = parse(get_lvl8_report_igm(validation_report(timestamp=timestamp)))

    assert root.get("created") == "2025-07-06T09:41:27Z"
    QAR_SCHEMA.assertValid(root)
