import io
import json
import zipfile
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

from emf.model_quality import model_quality, quality_functions
from emf.model_quality.quality_functions import (cache_tableviews, generate_quality_report, process_zipped_cgm,
                                                 set_common_metadata, set_quality_flag)
from emf.common.helpers.statistics import type_tableview_merge

RULE_SETS = {"igm_rule_set": ["impedance", "line_rating"], "cgm_rule_set": ["kruonis", "rtec", "outage", "lt_pl_xborder"]}

CHECK_RESULTS = {
    "check_generator_quality": {"kruonis_check": True, "rtec_check": True},
    "check_lt_pl_crossborder": {"lt_pl_xborder_check": True},
    "check_crossborder_inconsistencies": {"xborder_consistency_check": True},
    "check_outage_inconsistencies": {"outage_check": True},
    "check_reactive_power_limits": {"reactive_power_check": True},
    "check_line_limits": {"line_rating_check": True},
    "check_line_impedance": {"impedance_check": True},
}


@pytest.fixture
def fake_checks():
    """Replaces every quality rule with a stub that adds its passing flag to the report"""
    def stub(result):
        return lambda report, *args, **kwargs: {**report, **result}

    with ExitStack() as stack:
        yield {name: stack.enter_context(mock.patch.object(quality_functions, name, side_effect=stub(result)))
               for name, result in CHECK_RESULTS.items()}


def called_checks(fake_checks):
    return {name for name, check in fake_checks.items() if check.called}


# --- quality flag

@pytest.mark.parametrize("flags, quality", [
    ({"kruonis_check": True, "rtec_check": True, "outage_check": True, "lt_pl_xborder_check": True}, "good"),
    ({"kruonis_check": True, "rtec_check": False, "outage_check": True, "lt_pl_xborder_check": True}, "bad"),
    ({"kruonis_check": None, "rtec_check": False, "outage_check": True, "lt_pl_xborder_check": True}, "bad"),
    ({"kruonis_check": None, "rtec_check": True, "outage_check": True, "lt_pl_xborder_check": True}, "semi-good"),
    ({"kruonis_check": None, "rtec_check": None, "outage_check": None, "lt_pl_xborder_check": None}, "semi-good"),
])
def test_cgm_quality_aggregates_rule_flags(flags, quality):
    assert set_quality_flag(dict(flags), "CGM", RULE_SETS)["quality"] == quality


def test_quality_ignores_flags_outside_the_rule_set():
    report = {"kruonis_check": True, "reactive_power_check": False, "xborder_consistency_check": None}

    assert set_quality_flag(report, "CGM", RULE_SETS)["quality"] == "good"


def test_quality_ignores_rules_missing_from_report():
    report = {"kruonis_check": True, "rtec_check": None}

    assert set_quality_flag(report, "CGM", RULE_SETS)["quality"] == "semi-good"


@pytest.mark.parametrize("object_type, quality", [("IGM", "bad"), ("CGM", "good")])
def test_quality_uses_rule_set_of_object_type(object_type, quality):
    report = {"impedance_check": False, "line_rating_check": True, "kruonis_check": True}

    assert set_quality_flag(report, object_type, RULE_SETS)["quality"] == quality


# --- report generation

def test_baltic_cgm_runs_cgm_rules(fake_checks):
    tieflow_data = pd.DataFrame({"a": [1]})
    network = mock.sentinel.network

    report = generate_quality_report(mock.sentinel.handler, network, "CGM", {"pmd:Area": "BA"}, RULE_SETS,
                                     tieflow_data=tieflow_data)

    assert called_checks(fake_checks) == {"check_generator_quality", "check_lt_pl_crossborder",
                                          "check_crossborder_inconsistencies", "check_outage_inconsistencies",
                                          "check_reactive_power_limits"}
    lt_pl_call = fake_checks["check_lt_pl_crossborder"].call_args
    assert lt_pl_call.kwargs["tieflow_data"] is tieflow_data
    assert lt_pl_call.kwargs["border_limit"] == quality_functions.BORDER_LIMIT
    assert report["quality"] == "good"


def test_cgm_of_other_merge_area_gets_no_quality_status(fake_checks):
    with mock.patch.object(quality_functions, "CGM_TYPE", "BA"):
        report = generate_quality_report(None, None, "CGM", {"pmd:Area": "EU"}, RULE_SETS)

    assert report == {"quality": "no status"}
    assert not called_checks(fake_checks)


@pytest.mark.parametrize("tso", ["LITGRID", "AST", "ELERING"])
def test_igm_of_line_rating_tso_runs_line_limit_and_impedance_rules(fake_checks, tso):
    handler = mock.sentinel.handler

    report = generate_quality_report(handler, mock.sentinel.network, "IGM", {"pmd:TSO": tso}, RULE_SETS)

    assert called_checks(fake_checks) == {"check_line_limits", "check_line_impedance"}
    assert fake_checks["check_line_limits"].call_args.args[2] is handler
    assert fake_checks["check_line_limits"].call_args.kwargs["limit_temperature"] == quality_functions.LINE_LIMIT_TEMPERATURE
    assert report["quality"] == "good"


def test_igm_of_other_tso_skips_line_limit_rule(fake_checks):
    report = generate_quality_report(None, None, "IGM", {"pmd:TSO": "ELIA"}, RULE_SETS)

    assert called_checks(fake_checks) == {"check_line_impedance"}
    assert report["line_rating_check"] is None
    assert report["line_rating_mismatch"] is None


def test_other_object_types_get_no_quality_status(fake_checks):
    assert generate_quality_report(None, None, "BDS", {}, RULE_SETS) == {"quality": "no status"}
    assert not called_checks(fake_checks)


# --- metadata

IGM_METADATA = {"opde:Id": "igm-1", "pmd:TSO": "AST", "pmd:scenarioDate": "2025-07-06T09:30:00Z",
                "pmd:timeHorizon": "1D", "pmd:versionNumber": "002", "minio-bucket": "opdm-data",
                "pmd:content-reference": "CGMES/1D/AST/20250706/093000/SV/20250706T0930Z_1D_AST_SV_002.zip"}


def test_igm_common_metadata():
    metadata = set_common_metadata(IGM_METADATA, "IGM")

    assert metadata == {
        "object_type": "IGM",
        "@scenario_timestamp": "2025-07-06T09:30:00Z",
        "@time_horizon": "1D",
        "@version": 2,
        "content_reference": "CGMES/1D/AST/20250706/093000/SV/20250706T0930Z_1D_AST_SV_002.zip",
        "tso": "AST",
        "minio_bucket": "opdm-data",
    }


def test_cgm_common_metadata_uses_merge_area_as_merge_type():
    cgm = {"pmd:scenarioDate": "2025-07-06T09:30:00Z", "pmd:timeHorizon": "ID", "pmd:versionNumber": "012",
           "pmd:Area": "BA", "pmd:content-reference": "EMFOS/RMM/BA/file.zip"}

    metadata = set_common_metadata(cgm, "CGM")

    assert metadata == {"object_type": "CGM", "@scenario_timestamp": "2025-07-06T09:30:00Z", "@time_horizon": "ID",
                        "@version": 12, "merge_type": "BA", "content_reference": "EMFOS/RMM/BA/file.zip",
                        "minio_bucket": None}


def test_common_metadata_of_other_object_types_is_empty():
    assert set_common_metadata(IGM_METADATA, "BDS") == {}


# --- CGM unpacking and tableview cache

def zip_bytes(files: dict) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def test_zipped_cgm_is_flattened_to_named_xml_files():
    cgm = zip_bytes({
        "igm_1.zip": zip_bytes({"A_EQ.xml": b"<eq/>", "A_SSH.xml": b"<ssh/>"}),
        "nested.zip": zip_bytes({"deeper.zip": zip_bytes({"B_TP.xml": b"<tp/>"})}),
        "CGM_SV.xml": b"<sv/>",
        "readme.txt": b"ignored",
    })

    files = process_zipped_cgm(cgm)

    assert {file.name: file.getvalue() for file in files} == {"A_EQ.xml": b"<eq/>", "A_SSH.xml": b"<ssh/>",
                                                              "B_TP.xml": b"<tp/>", "CGM_SV.xml": b"<sv/>"}


def test_cached_tableviews_are_computed_once_per_type(make_triplets):
    network = make_triplets([("t1", "Type", "Terminal"), ("t1", "Terminal.ConnectivityNode", "cn1"),
                             ("cn1", "Type", "ConnectivityNode")])
    compute = mock.Mock(wraps=network.type_tableview)
    network.type_tableview = compute
    network = cache_tableviews(network)

    first = network.type_tableview("Terminal")
    second = network.type_tableview("Terminal")
    network.type_tableview("ConnectivityNode")

    assert compute.call_count == 2
    pd.testing.assert_frame_equal(first, second)


def test_cached_tableview_changes_do_not_leak_between_callers(make_triplets):
    network = cache_tableviews(make_triplets([("t1", "Type", "Terminal"), ("t1", "Terminal.ConnectivityNode", "cn1")]))

    network.type_tableview("Terminal")["Terminal.ConnectivityNode"] = "changed"

    assert network.type_tableview("Terminal").loc["t1", "Terminal.ConnectivityNode"] == "cn1"


def test_cached_tableviews_keep_merge_results_and_missing_types(make_triplets):
    rows = [("t1", "Type", "Terminal"), ("t1", "Terminal.ConnectivityNode", "cn1"), ("cn1", "Type", "ConnectivityNode")]
    expected = type_tableview_merge(make_triplets(rows), "Terminal->ConnectivityNode")

    network = cache_tableviews(make_triplets(rows))

    pd.testing.assert_frame_equal(type_tableview_merge(network, "Terminal->ConnectivityNode"), expected)
    assert network.type_tableview("TieFlow") is None


# --- HandlerModelQuality

@pytest.fixture
def handler():
    with mock.patch.object(model_quality.minio_api, "ObjectStorage"), mock.patch.object(model_quality.elastic, "Elastic"):
        yield model_quality.HandlerModelQuality()


def sent_reports(handler):
    return {call.kwargs["index"]: call.kwargs["json_message"] for call in handler.elastic_service.send_to_elastic.call_args_list}


def properties(object_type):
    return SimpleNamespace(headers={"opde:Object-Type": object_type})


def igm_message_metadata(opdm_object):
    """OPDM metadata as received from the queue, MicroGrid fixtures lack the storage keys of real OPDM objects"""
    metadata = {key: value for key, value in opdm_object.items() if key != "opde:Component"}
    return {**metadata, "pmd:content-reference": f"CGMES/1D/{opdm_object['pmd:TSO']}/model.zip", "minio-bucket": "opdm-data"}


def cgm_zip(opdm_object):
    return zip_bytes({component["opdm:Profile"]["pmd:fileName"]: component["opdm:Profile"]["DATA"]
                      for component in opdm_object["opde:Component"]})


def cgm_metadata(content_reference="EMFOS/RMM/BA/cgm.zip"):
    return {"pmd:scenarioDate": "2025-07-06T09:30:00Z", "pmd:timeHorizon": "1D", "pmd:versionNumber": "001",
            "pmd:Area": "BA", "pmd:content-reference": content_reference}


@pytest.fixture
def fake_report_generators():
    with mock.patch.object(model_quality, "generate_quality_report", return_value={"quality": "good"}) as quality, \
            mock.patch.object(model_quality, "get_system_metrics", return_value={"total_load": 100.0}) as statistics, \
            mock.patch.object(model_quality, "get_tieflow_data", return_value=pd.DataFrame()):
        yield SimpleNamespace(quality=quality, statistics=statistics)


def test_igm_quality_report_is_sent_with_common_metadata(handler, microgrid_be_igm, microgrid_boundary):
    message = json.dumps([igm_message_metadata(microgrid_be_igm)]).encode()
    with mock.patch.object(model_quality.models, "get_content", return_value=microgrid_be_igm) as get_content, \
            mock.patch.object(model_quality.models, "get_latest_boundary", return_value=microgrid_boundary):
        result = handler.handle(message, properties("IGM"))

    assert result[0] is message
    assert get_content.call_args.kwargs["metadata"]["opde:Id"] == microgrid_be_igm["opde:Id"]
    quality_report = sent_reports(handler)[model_quality.ELK_QUALITY_INDEX]
    assert quality_report["impedance_check"] is True
    assert quality_report["line_rating_check"] is None  # ELIA is not in LINE_RATING_TSO_LIST
    assert "quality" in quality_report
    assert quality_report["tso"] == "ELIA"
    assert quality_report["@version"] == 1


def test_igm_that_fails_to_load_is_skipped(handler, fake_report_generators, microgrid_be_igm, microgrid_nl_igm,
                                           microgrid_boundary):
    message = json.dumps([igm_message_metadata(microgrid_be_igm), igm_message_metadata(microgrid_nl_igm)]).encode()
    with mock.patch.object(model_quality.models, "get_content",
                           side_effect=[ConnectionError("minio down"), microgrid_nl_igm]), \
            mock.patch.object(model_quality.models, "get_latest_boundary", return_value=microgrid_boundary):
        handler.handle(message, properties("IGM"))

    assert fake_report_generators.quality.call_count == 1
    assert fake_report_generators.quality.call_args.kwargs["model_metadata"]["pmd:TSO"] == "TENNET"
    assert sent_reports(handler)[model_quality.ELK_QUALITY_INDEX]["tso"] == "TENNET"


def test_empty_igm_list_is_ignored(handler):
    with mock.patch.object(model_quality.models, "get_latest_boundary") as get_latest_boundary:
        handler.handle(b"[]", properties("IGM"))

    get_latest_boundary.assert_not_called()
    handler.elastic_service.send_to_elastic.assert_not_called()


def test_cgm_is_downloaded_unpacked_and_reported(handler, fake_report_generators, microgrid_be_igm):
    handler.minio_service.download_object.return_value = cgm_zip(microgrid_be_igm)

    with mock.patch.object(model_quality, "IGM_RULE_SET", "impedance"), mock.patch.object(model_quality, "CGM_RULE_SET", "kruonis,rtec"):
        handler.handle(json.dumps(cgm_metadata()).encode(), properties("CGM"))

    handler.minio_service.download_object.assert_called_once_with("opde-confidential-models", "EMFOS/RMM/BA/cgm.zip")
    quality_call = fake_report_generators.quality.call_args.kwargs
    assert quality_call["object_type"] == "CGM"
    assert quality_call["rule_sets"] == {"igm_rule_set": ["impedance"], "cgm_rule_set": ["kruonis", "rtec"]}
    assert quality_call["network"].query("KEY == 'Type' and VALUE == 'FullModel'").shape[0] == 4
    reports = sent_reports(handler)
    assert reports[model_quality.ELK_QUALITY_INDEX] == {"quality": "good", **set_common_metadata(cgm_metadata(), "CGM")}
    assert reports[model_quality.ELK_STATISTICS_INDEX]["total_load"] == 100.0
    assert reports[model_quality.ELK_STATISTICS_INDEX]["merge_type"] == "BA"


def test_cgm_is_downloaded_from_its_bucket(handler, fake_report_generators):
    handler.minio_service.download_object.return_value = zip_bytes({})
    metadata = {**cgm_metadata(), "minio-bucket": "cgm-bucket"}

    handler.handle(json.dumps(metadata).encode(), properties("CGM"))

    handler.minio_service.download_object.assert_called_once_with("cgm-bucket", "EMFOS/RMM/BA/cgm.zip")


def test_cgm_download_failure_sends_nothing(handler, fake_report_generators):
    handler.minio_service.download_object.side_effect = ConnectionError("minio down")
    message = json.dumps(cgm_metadata()).encode()
    props = properties("CGM")

    assert handler.handle(message, props) == (message, props)
    handler.elastic_service.send_to_elastic.assert_not_called()


def test_model_without_data_sends_nothing(handler, fake_report_generators):
    handler.minio_service.download_object.return_value = zip_bytes({})

    handler.handle(json.dumps(cgm_metadata()).encode(), properties("CGM"))

    fake_report_generators.quality.assert_not_called()
    handler.elastic_service.send_to_elastic.assert_not_called()


@pytest.mark.parametrize("failing, sent_index", [("quality", "ELK_STATISTICS_INDEX"), ("statistics", "ELK_QUALITY_INDEX")])
def test_one_failing_report_does_not_block_the_other(handler, fake_report_generators, microgrid_be_igm, failing,
                                                     sent_index):
    getattr(fake_report_generators, failing).side_effect = ValueError("broken")
    handler.minio_service.download_object.return_value = cgm_zip(microgrid_be_igm)

    handler.handle(json.dumps(cgm_metadata()).encode(), properties("CGM"))

    assert set(sent_reports(handler)) == {getattr(model_quality, sent_index)}


def test_unknown_object_type_is_not_processed(handler, fake_report_generators):
    message = json.dumps(cgm_metadata()).encode()
    props = properties("BDS")

    assert handler.handle(message, props) == (message, props)
    handler.minio_service.download_object.assert_not_called()
    handler.elastic_service.send_to_elastic.assert_not_called()
