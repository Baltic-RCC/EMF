import importlib.util
import json
from pathlib import Path
from unittest import mock

import pika
import pytest
from lxml import etree
from saxonche import PySaxonApiError

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORTS = REPO_ROOT / "emf" / "report_publisher"
OLD_XSL = REPORTS / "IGM_entsoeQAReport_Level_8_old.xsl"
QAR_XSD = REPO_ROOT / "emf" / "common" / "schemas" / "QAR_v2.12.0.xsd"
SAXONPY_API = REPO_ROOT / "emf" / "common" / "xslt_engine" / "saxonpy_api.py"
QAR = "{http://entsoe.eu/checks}"

# passed_igm_report.xml has a converged load flow, failed_igm_report.xml does not
PASSED_REPORT = REPORTS / "passed_igm_report.xml"
FAILED_REPORT = REPORTS / "failed_igm_report.xml"
PASSED_QA_REPORT = REPO_ROOT / "config" / "report_publisher" / "test_results.xml"
FAILED_QA_REPORT = REPO_ROOT / "config" / "report_publisher" / "test_results2.xml"
SAMPLES = [
    pytest.param(PASSED_REPORT, PASSED_QA_REPORT, id="passed"),
    pytest.param(FAILED_REPORT, FAILED_QA_REPORT, id="failed"),
]


@pytest.fixture(scope="module")
def saxonpy_api():
    """saxonpy_api connects to RabbitMQ at import, so it is loaded with BlockingClient patched and kept out of sys.modules"""
    spec = importlib.util.spec_from_file_location("emf.common.xslt_engine.saxonpy_api", SAXONPY_API)
    module = importlib.util.module_from_spec(spec)
    with mock.patch("emf.common.integrations.rabbit.BlockingClient"):
        spec.loader.exec_module(module)
    return module


def comparable(qa_report: bytes) -> bytes:
    """Canonical QAReport without the transformation timestamp. IGM tso is covered by its own test."""
    root = etree.fromstring(qa_report, etree.XMLParser(remove_blank_text=True))
    del root.attrib["created"]
    for igm in root.iter(f"{QAR}IGM"):
        del igm.attrib["tso"]
    return etree.tostring(root, method="c14n")


@pytest.mark.parametrize("sample, expected", SAMPLES)
def test_old_xslt_turns_sample_report_into_expected_qa_report(saxonpy_api, sample, expected):
    qa_report = saxonpy_api.xslt30_convert(str(sample), str(OLD_XSL))

    assert comparable(qa_report) == comparable(expected.read_bytes())


@pytest.mark.parametrize("sample", [PASSED_REPORT, FAILED_REPORT], ids=["passed", "failed"])
def test_old_xslt_output_is_valid_qar(saxonpy_api, sample):
    qa_report = saxonpy_api.xslt30_convert(str(sample), str(OLD_XSL))

    assert saxonpy_api.validate_xml(qa_report, QAR_XSD.read_bytes()) is True


@pytest.mark.parametrize("sample, quality_indicator, violations", [
    pytest.param(PASSED_REPORT, "Plausible", [], id="passed"),
    pytest.param(FAILED_REPORT, "Substituted",
                 [("IGMConvergence", "MSG 52. ERROR: IGM for http://www.apg.at/OperationalPlanning did not converge.")], id="failed"),
])
def test_old_xslt_quality_indicator_follows_load_flow_convergence(saxonpy_api, sample, quality_indicator, violations):
    igm = etree.fromstring(saxonpy_api.xslt30_convert(str(sample), str(OLD_XSL))).find(f"{QAR}IGM")

    assert igm.get("qualityIndicator") == quality_indicator
    assert [(v.get("ruleId"), v.findtext(f"{QAR}Message")) for v in igm.iter(f"{QAR}RuleViolation")] == violations


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="old XSLT tso choose is inverted: prints modelPartReference only when it is empty, else missing MetaData/TSO")
def test_old_xslt_fills_tso_from_model_part_reference(saxonpy_api):
    qa_report = saxonpy_api.xslt30_convert(str(FAILED_REPORT), str(OLD_XSL))

    assert etree.fromstring(qa_report).find(f"{QAR}IGM").get("tso") == "APG"


@pytest.mark.parametrize("source, stylesheet", [
    pytest.param(lambda path: str(path), lambda path: str(path), id="paths"),
    pytest.param(lambda path: path.read_bytes(), lambda path: path.read_bytes(), id="bytes"),
    pytest.param(lambda path: path.read_text(encoding="utf-8"), lambda path: path.read_text(encoding="utf-8"), id="text"),
    pytest.param(lambda path: path.read_bytes(), lambda path: path.read_text(encoding="utf-8"), id="bytes-and-text"),
])
def test_xslt30_convert_accepts_paths_text_and_bytes(saxonpy_api, source, stylesheet):
    qa_report = saxonpy_api.xslt30_convert(source(FAILED_REPORT), stylesheet(OLD_XSL))

    assert comparable(qa_report) == comparable(FAILED_QA_REPORT.read_bytes())


@pytest.mark.parametrize("source, stylesheet", [
    pytest.param(lambda path: path.read_text(encoding="utf-8"), lambda path: str(path), id="text-and-stylesheet-path"),
    pytest.param(lambda path: str(path), lambda path: path.read_text(encoding="utf-8"), id="path-and-stylesheet-text"),
])
@pytest.mark.xfail(strict=True, raises=PySaxonApiError,
                   reason="xslt30_convert checks os.path.isfile(source_file) to decide how to load the stylesheet")
def test_xslt30_convert_accepts_mixed_path_and_text(saxonpy_api, source, stylesheet):
    qa_report = saxonpy_api.xslt30_convert(source(FAILED_REPORT), stylesheet(OLD_XSL))

    assert etree.fromstring(qa_report).tag == f"{QAR}QAReport"


@pytest.mark.xfail(strict=True, raises=PySaxonApiError,
                   reason="bytes are decoded with utf-8 instead of utf-8-sig, the BOM in passed_igm_report.xml makes Saxon reject it")
def test_xslt30_convert_accepts_bytes_with_utf8_bom(saxonpy_api):
    sample = PASSED_REPORT.read_bytes()
    assert sample.startswith(b"\xef\xbb\xbf")

    qa_report = saxonpy_api.xslt30_convert(sample, OLD_XSL.read_bytes())

    assert etree.fromstring(qa_report).find(f"{QAR}IGM").get("qualityIndicator") == "Plausible"


def test_xslt30_convert_writes_output_file(saxonpy_api, tmp_path):
    output_file = tmp_path / "qa_report.xml"

    qa_report = saxonpy_api.xslt30_convert(str(FAILED_REPORT), str(OLD_XSL), output_file=str(output_file))

    assert comparable(output_file.read_bytes()) == comparable(qa_report)


@pytest.mark.parametrize("load", [lambda path: str(path), lambda path: path.read_bytes()], ids=["paths", "bytes"])
@pytest.mark.parametrize("expected", [PASSED_QA_REPORT, FAILED_QA_REPORT], ids=["passed", "failed"])
def test_validate_xml_accepts_expected_qa_reports(saxonpy_api, expected, load):
    assert saxonpy_api.validate_xml(load(expected), load(QAR_XSD)) is True


@pytest.mark.parametrize("break_igm", [
    pytest.param(lambda igm: igm.set("qualityIndicator", "Great"), id="unknown-quality-indicator"),
    pytest.param(lambda igm: igm.set("version", "first"), id="version-not-int"),
    pytest.param(lambda igm: igm.attrib.pop("tso"), id="tso-missing"),
])
def test_validate_xml_rejects_invalid_qa_report(saxonpy_api, break_igm):
    root = etree.fromstring(PASSED_QA_REPORT.read_bytes())
    break_igm(root.find(f"{QAR}IGM"))

    assert saxonpy_api.validate_xml(etree.tostring(root), QAR_XSD.read_bytes()) is False


@pytest.mark.parametrize("with_xsd, is_valid", [(True, "True"), (False, "None")])
def test_do_conversion_transforms_message_and_sets_headers(saxonpy_api, with_xsd, is_valid):
    message = {"XML": FAILED_REPORT.read_text(encoding="utf-8"), "XSL": OLD_XSL.read_text(encoding="utf-8")}
    if with_xsd:
        message["XSD"] = QAR_XSD.read_text(encoding="utf-8")
    channel, method = object(), object()

    result = saxonpy_api.do_conversion(channel, method, pika.BasicProperties(), json.dumps(message))

    out_channel, out_method, properties, body = result
    assert (out_channel, out_method) == (channel, method)
    assert properties.headers == {"file-type": "XML", "business-type": "QA-report", "is-valid": is_valid}
    assert comparable(body) == comparable(FAILED_QA_REPORT.read_bytes())


def test_run_service_shovels_queue_to_exchange_through_do_conversion(saxonpy_api):
    with mock.patch.object(saxonpy_api, "rabbit_service") as rabbit_service, \
            mock.patch.object(saxonpy_api, "RMQ_QUEUE", "xslt-queue"), \
            mock.patch.object(saxonpy_api, "RMQ_EXCHANGE", "xslt-exchange"):
        saxonpy_api.run_service()

    rabbit_service.shovel.assert_called_once_with("xslt-queue", "xslt-exchange", saxonpy_api.do_conversion)
