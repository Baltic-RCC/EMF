import io
import json
import math
import zipfile
from types import SimpleNamespace
from unittest import mock

import polars as pl
import pypowsybl as pp
import pytest
from lxml import etree

from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.loadflow_tool import loadflow_settings
from emf.model_validator import model_validator
from emf.model_validator.model_validator import (
    HandlerModelsValidator,
    PostLFValidator,
    PreLFValidator,
    TemporaryPreMergeModifications,
)

Status = pp.loadflow.ComponentStatus
STATUS_TEXT = {Status.CONVERGED: "Converged", Status.MAX_ITERATION_REACHED: "Max iteration reached",
               Status.FAILED: "Failed", Status.NO_CALCULATION: "No calculation"}


def component_result(status=Status.CONVERGED):
    """Stand-in for pypowsybl ComponentResult with the same attributes"""
    return SimpleNamespace(connected_component_num=0, synchronous_component_num=0, status=status,
                           status_text=STATUS_TEXT[status], iteration_count=5, reference_bus_id="bus",
                           slack_bus_results=[], distributed_active_power=math.nan)


@pytest.fixture
def set_config(monkeypatch):
    """Sets module level config constants of model_validator, values are strings like in .properties"""
    def _set_config(**values):
        for name, value in values.items():
            monkeypatch.setattr(model_validator, name, value)
    return _set_config


def run_loadflow_validation(results):
    validator = PostLFValidator(network=mock.sentinel.network, network_triplets=None)
    with mock.patch.object(model_validator.pp.loadflow, "run_ac", side_effect=results) as run_ac:
        validator.validate_loadflow()
    used_parameters = [call.kwargs["parameters"] for call in run_ac.call_args_list]
    return validator.report, used_parameters


# ---------------------------------------------------------------- PostLFValidator

def test_dynamic_settings_relax_to_next_priority_after_divergence(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="True", VALIDATION_LOAD_FLOW_SETTINGS="IGM_VALIDATION",
               VALIDATION_LOAD_FLOW_SETTINGS_PRIORITY="IGM_VALIDATION, EU_DEFAULT, EU_RELAXED")

    report, used = run_loadflow_validation([[component_result(Status.MAX_ITERATION_REACHED)], [component_result()]])

    assert used == [loadflow_settings.IGM_VALIDATION, loadflow_settings.EU_DEFAULT]
    assert report["loadflow_parameters"] == "EU_DEFAULT"
    assert report["validations"]["loadflow"] is True
    assert report["loadflow"]["status"] == Status.CONVERGED.value


def test_dynamic_settings_start_from_configured_settings(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="True", VALIDATION_LOAD_FLOW_SETTINGS="EU_DEFAULT",
               VALIDATION_LOAD_FLOW_SETTINGS_PRIORITY="IGM_VALIDATION,EU_DEFAULT,EU_RELAXED")

    report, used = run_loadflow_validation([[component_result(Status.FAILED)], [component_result()]])

    assert used == [loadflow_settings.EU_DEFAULT, loadflow_settings.EU_RELAXED]
    assert report["loadflow_parameters"] == "EU_RELAXED"


def test_dynamic_settings_all_diverging_is_invalid_with_last_settings_reported(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="True", VALIDATION_LOAD_FLOW_SETTINGS="IGM_VALIDATION",
               VALIDATION_LOAD_FLOW_SETTINGS_PRIORITY="IGM_VALIDATION,EU_DEFAULT")

    report, used = run_loadflow_validation([[component_result(Status.FAILED)], [component_result(Status.MAX_ITERATION_REACHED)]])

    assert used == [loadflow_settings.IGM_VALIDATION, loadflow_settings.EU_DEFAULT]
    assert report["loadflow_parameters"] == "EU_DEFAULT"
    assert report["validations"]["loadflow"] is False
    assert report["loadflow"]["status"] == Status.MAX_ITERATION_REACHED.value
    assert report["loadflow"]["distributed_active_power"] == 0.0  # NaN is not valid JSON for Elastic


def test_without_dynamic_settings_only_configured_settings_are_tried(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="False", VALIDATION_LOAD_FLOW_SETTINGS="IGM_VALIDATION",
               VALIDATION_LOAD_FLOW_SETTINGS_PRIORITY="IGM_VALIDATION,EU_DEFAULT")

    report, used = run_loadflow_validation([[component_result(Status.FAILED)], [component_result()]])

    assert used == [loadflow_settings.IGM_VALIDATION]
    assert report["loadflow_parameters"] == "IGM_VALIDATION"
    assert report["validations"]["loadflow"] is False


@pytest.mark.parametrize("status, valid", [
    (Status.CONVERGED, True),
    (Status.MAX_ITERATION_REACHED, False),
    (Status.FAILED, False),
    (Status.NO_CALCULATION, False),
])
def test_loadflow_validation_valid_only_when_main_component_converged(set_config, status, valid):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="False")

    report, _ = run_loadflow_validation([[component_result(status)]])

    assert report["validations"]["loadflow"] is valid
    assert report["loadflow"]["status_text"] == STATUS_TEXT[status]


def test_loadflow_validation_counts_components(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="False")

    report, _ = run_loadflow_validation([[component_result(), component_result(Status.FAILED), component_result()]])

    assert (report["components"], report["converged_components"]) == (3, 2)


@pytest.mark.pypowsybl
def test_post_lf_validation_on_ieee14(ieee14_igm, set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="False", VALIDATION_LOAD_FLOW_SETTINGS="IGM_VALIDATION", CHECK_KIRCHHOFF_FIRST_LAW="False")
    validator = PostLFValidator(network=load_network_model([ieee14_igm]), network_triplets=load_opdm_objects_to_triplets([ieee14_igm]))

    validator.run_validation()

    assert validator.report["validations"] == {"loadflow": True}
    assert validator.report["loadflow_parameters"] == "IGM_VALIDATION"
    assert validator.report["loadflow"]["status_text"] == "Converged"
    assert (validator.report["components"], validator.report["converged_components"]) == (1, 1)
    assert validator.report["element_validation"]
    assert all(isinstance(value, bool) for value in validator.report["element_validation"].values())
    json.dumps(validator.report)  # goes to Elastic


@pytest.mark.pypowsybl
def test_post_lf_validation_checks_kirchhoff_first_law_when_enabled(ieee14_igm, set_config):
    set_config(CHECK_KIRCHHOFF_FIRST_LAW="True")
    validator = PostLFValidator(network=load_network_model([ieee14_igm]), network_triplets=load_opdm_objects_to_triplets([ieee14_igm]))

    validator.run_validation()

    assert isinstance(validator.report["validations"]["kirchhoff_first_law"], bool)


# ---------------------------------------------------------------- PreLFValidator

def switch_rows(switch_id, node_1, node_2, retained="false", is_open="false"):
    rows = [(switch_id, "Type", "Breaker"), (switch_id, "Switch.retained", retained), (switch_id, "Switch.open", is_open)]
    for number, node in enumerate([node_1, node_2]):
        terminal = f"{switch_id}_t{number}"
        rows += [(terminal, "Type", "Terminal"), (terminal, "Terminal.TopologicalNode", node),
                 (terminal, "Terminal.ConductingEquipment", switch_id)]
    return rows


@pytest.mark.parametrize("rows, valid", [
    (switch_rows("sw", "tn1", "tn2"), False),
    (switch_rows("sw", "tn1", "tn1"), True),
    (switch_rows("sw", "tn1", "tn2", retained="true"), True),
], ids=["between_nodes", "same_node", "retained"])
def test_pre_lf_validation_of_non_retained_switches(make_triplets, set_config, rows, valid):
    set_config(CHECK_NON_RETAINED_SWITCHES="True")
    validator = PreLFValidator(network=make_triplets(rows))

    validator.run_validation()

    assert validator.report == {"pre_validations": {"non_retained_switches": valid}}


def test_pre_lf_validation_disabled_by_config(make_triplets, set_config):
    set_config(CHECK_NON_RETAINED_SWITCHES="False")
    validator = PreLFValidator(network=make_triplets(switch_rows("sw", "tn1", "tn2")))

    validator.run_validation()

    assert validator.report == {"pre_validations": {}}


# ---------------------------------------------------------------- TemporaryPreMergeModifications

DK2_EIC = "10YDK-2--------M"
DK_REGIONS = [
    ("dk", "Type", "GeographicalRegion"), ("dk", "IdentifiedObject.name", "DK"),
    ("dk_eic", "Type", "GeographicalRegion"), ("dk_eic", "IdentifiedObject.name", "DK2"),
    ("dk_eic", "IdentifiedObject.energyIdentCodeEic", DK2_EIC),
    ("sjaelland", "Type", "SubGeographicalRegion"), ("sjaelland", "IdentifiedObject.name", "Sjaelland"),
    ("sjaelland", "SubGeographicalRegion.Region", "dk"),
    ("ca", "Type", "ControlArea"), ("ca", "ControlArea.type", "ControlAreaTypeKind.Interchange"),
    ("ca", "IdentifiedObject.energyIdentCodeEic", DK2_EIC),
]


def model_file(file_name, tso="AST"):
    return [("distribution", "Type", "Distribution"), ("distribution", "label", file_name.format(tso=tso)),
            ("header", "Type", "FullModel"), ("header", "Model.scenarioTime", "2025-07-06T09:30:00Z")]


def values(data, key):
    return dict(data.query("KEY == @key")[["ID", "VALUE"]].itertuples(index=False))


@pytest.mark.parametrize("file_name, sanitized", [
    ("20250706T0930Z_1D_{tso}_SSH_001.XML", True),
    ("20250706T0930Z_1D_{tso}_SSH_001.xml", False),
])
def test_pre_merge_modification_fixes_uppercase_xml_extension(make_triplets, file_name, sanitized):
    modifications = TemporaryPreMergeModifications(network=make_triplets(model_file(file_name)), tso="AST")

    data = modifications.run_pre_process_modifications()

    assert values(data, "label") == {"distribution": "20250706T0930Z_1D_AST_SSH_001.xml"}
    assert modifications.report["modification"]["sanitize_file_name"] is sanitized


def test_pre_merge_modification_fills_header_from_file_name(make_triplets):
    modifications = TemporaryPreMergeModifications(network=make_triplets(model_file("20250706T0930Z_1D_{tso}_SSH_002.xml")), tso="AST")

    data = modifications.run_pre_process_modifications()

    assert values(data, "Model.version") == {"header": "002"}
    assert values(data, "Model.modelingEntity") == {"header": "AST"}
    assert modifications.report["modification"]["header_from_file_name"] is True


@pytest.mark.parametrize("tso, modify_dk_regions, modified", [
    ("DKE", "True", True),
    ("DKW", "True", True),
    ("DKE", "False", False),
    ("AST", "True", False),
])
def test_pre_merge_modification_dk_region_fix_only_for_danish_tsos(make_triplets, set_config, tso, modify_dk_regions, modified):
    set_config(MODIFY_DK_REGIONS=modify_dk_regions)
    modifications = TemporaryPreMergeModifications(
        network=make_triplets(model_file("20250706T0930Z_1D_{tso}_EQ_001.xml", tso) + DK_REGIONS), tso=tso)

    data = modifications.run_pre_process_modifications()

    assert values(data, "SubGeographicalRegion.Region") == {"sjaelland": "dk_eic" if modified else "dk"}
    assert modifications.report["modification"].get("update_region_names", False) is modified


@pytest.mark.parametrize("open_non_retained_switches", ["True", "False"])
def test_pre_merge_modification_opens_non_retained_switches_when_configured(make_triplets, set_config, open_non_retained_switches):
    set_config(OPEN_NON_RETAINED_SWITCHES=open_non_retained_switches)
    modifications = TemporaryPreMergeModifications(
        network=make_triplets(model_file("20250706T0930Z_1D_{tso}_EQ_001.xml") + switch_rows("sw", "tn1", "tn2")), tso="AST")

    data = modifications.run_pre_process_modifications()

    if open_non_retained_switches == "True":
        assert values(data, "Switch.open") == {"sw": "true"}
        assert modifications.report["modification"]["open_non_retained_switches"] is True
    else:
        assert values(data, "Switch.open") == {"sw": "false"}
        assert "open_non_retained_switches" not in modifications.report["modification"]


# ---------------------------------------------------------------- HandlerModelsValidator

LVL8_TIMESTAMP = "2025-07-06T09:41:27.123456"


def _stamp_timestamp(index, json_message, **kwargs):
    # Elastic.send_to_elastic adds '@timestamp' to the message in place, the level 8 report reads it from there
    json_message["@timestamp"] = LVL8_TIMESTAMP


@pytest.fixture
def services(set_config):
    set_config(ENABLE_DYNAMIC_VALIDATION_SETTINGS="False", VALIDATION_LOAD_FLOW_SETTINGS="IGM_VALIDATION",
               CHECK_NON_RETAINED_SWITCHES="False", CHECK_KIRCHHOFF_FIRST_LAW="False", ENABLE_LVL8_REPORTS="False")
    with mock.patch.object(model_validator.minio_api, "ObjectStorage") as minio, \
            mock.patch.object(model_validator.elastic, "Elastic") as elastic, \
            mock.patch.object(model_validator.edx, "EDX") as edx:
        elastic.return_value.send_to_elastic.side_effect = _stamp_timestamp
        yield SimpleNamespace(minio=minio.return_value, elastic=elastic.return_value, edx_class=edx, edx=edx.return_value)


def _metadata_only(opdm_object):
    """Message content as received from RabbitMQ: OPDM metadata without the model DATA"""
    components = [{"opdm:Profile": {k: v for k, v in c["opdm:Profile"].items() if k != "DATA"}} for c in opdm_object["opde:Component"]]
    return {**opdm_object, "opde:Component": components}


def handle(opdm_objects, boundary=None):
    boundary = boundary or {"opde:Object-Type": "BDS", "opde:Component": []}
    contents = {opdm_object["opde:Id"]: opdm_object for opdm_object in opdm_objects}
    message = json.dumps([_metadata_only(opdm_object) for opdm_object in opdm_objects]).encode()
    properties = SimpleNamespace(headers={})
    with mock.patch("emf.model_validator.model_validator.models.get_content",
                    side_effect=lambda metadata: contents[metadata["opde:Id"]]), \
            mock.patch("emf.model_validator.model_validator.models.get_latest_boundary", return_value=boundary) as latest_boundary:
        result = HandlerModelsValidator().handle(message, properties)
    return SimpleNamespace(result=result, message=message, properties=properties, latest_boundary=latest_boundary)


def sent_reports(services):
    return [call.kwargs["json_message"] for call in services.elastic.send_to_elastic.call_args_list]


def sent_metadata(services):
    return [message for call in services.elastic.send_to_elastic_bulk.call_args_list for message in call.kwargs["json_message_list"]]


def stored_in_minio(opdm_object, bucket="opdm-data"):
    """Adds the MinIO location keys that OPDM objects retrieved by EMFOS carry"""
    opdm_object["minio-bucket"] = bucket
    for component in opdm_object["opde:Component"]:
        profile = component["opdm:Profile"]
        profile["pmd:content-reference"] = f"CGMES/1D/{opdm_object['pmd:TSO']}/20140601/103000/{profile['pmd:cgmesProfile']}/{profile['pmd:fileName']}"
    opdm_object["pmd:content-reference"] = opdm_object["opde:Component"][-1]["opdm:Profile"]["pmd:content-reference"]
    return opdm_object


def test_handler_skips_messages_with_only_boundary_sets(services, microgrid_boundary):
    run = handle([microgrid_boundary])

    assert run.result == (run.message, run.properties)
    run.latest_boundary.assert_not_called()
    services.elastic.send_to_elastic.assert_not_called()
    services.elastic.send_to_elastic_bulk.assert_not_called()
    services.minio.upload_object.assert_not_called()


@pytest.mark.pypowsybl
def test_handler_validates_microgrid_igm_with_boundary(services, microgrid_be_igm, microgrid_boundary):
    igm = stored_in_minio(microgrid_be_igm)
    content_references = {c["opdm:Profile"]["pmd:content-reference"] for c in igm["opde:Component"]}

    run = handle([igm], boundary=microgrid_boundary)

    assert run.result == (run.message, run.properties)
    run.latest_boundary.assert_called_once()
    [report] = sent_reports(services)
    assert services.elastic.send_to_elastic.call_args.kwargs["index"] == model_validator.VALIDATION_ELK_INDEX
    assert report["validations"] == {"loadflow": True}
    assert report["@scenario_timestamp"] == microgrid_be_igm["pmd:scenarioDate"]
    assert report["@time_horizon"] == "1D"
    assert report["fullModel_ID"] == microgrid_be_igm["pmd:fullModel_ID"]
    assert report["@version"] == 1
    assert report["tso"] == "ELIA"
    assert report["minio_bucket"] == "opdm-data"
    assert {"pre_validations", "modification", "loadflow", "loadflow_parameters"} <= report.keys()

    [metadata] = sent_metadata(services)
    assert services.elastic.send_to_elastic_bulk.call_args.kwargs["index"] == model_validator.METADATA_ELK_INDEX
    assert metadata["opde:Id"] == microgrid_be_igm["opde:Id"]
    assert metadata["valid"] is True
    assert {"ac_net_position", "sum_conform_load"} <= metadata.keys()
    assert all(c["opdm:Profile"]["DATA"] is None for c in metadata["opde:Component"])

    uploads = [call.kwargs for call in services.minio.upload_object.call_args_list]
    assert {upload["file_path_or_file_object"].name for upload in uploads} == content_references
    assert all(upload["bucket_name"] == "opdm-data" and upload["tags"] == {"state": "modified"} for upload in uploads)
    for upload in uploads:
        with zipfile.ZipFile(io.BytesIO(upload["file_path_or_file_object"].getvalue())) as modified:
            assert [name.endswith(".xml") for name in modified.namelist()] == [True]

    services.edx_class.assert_not_called()


@pytest.mark.pypowsybl
def test_handler_continues_with_next_model_after_a_failing_one(services, igm_factory):
    broken = igm_factory("create_ieee14", tso="BROKEN")
    for component in broken["opde:Component"]:
        component["opdm:Profile"]["DATA"] = b"not a zip"
    good = igm_factory("create_ieee14", tso="GOOD")

    handle([broken, good])

    assert [report["tso"] for report in sent_reports(services)] == ["GOOD"]
    assert [metadata["pmd:TSO"] for metadata in sent_metadata(services)] == ["GOOD"]


@pytest.mark.pypowsybl
def test_handler_marks_model_invalid_when_loadflow_diverges(services, ieee14_igm):
    with mock.patch.object(model_validator.pp.loadflow, "run_ac", return_value=[component_result(Status.MAX_ITERATION_REACHED)]):
        handle([ieee14_igm])

    [report] = sent_reports(services)
    [metadata] = sent_metadata(services)
    assert report["validations"]["loadflow"] is False
    assert metadata["valid"] is False


@pytest.mark.pypowsybl
def test_handler_adds_net_position_and_conform_load_to_metadata(services, ieee14_igm):
    with mock.patch.object(model_validator, "get_ac_net_position", return_value=-123.45) as net_position, \
            mock.patch.object(model_validator, "get_sum_of_loads", return_value=678.9) as sum_of_loads:
        handle([ieee14_igm])

    [metadata] = sent_metadata(services)
    assert (metadata["ac_net_position"], metadata["sum_conform_load"]) == (-123.45, 678.9)
    assert sum_of_loads.call_args.kwargs["parameter_name"] == "ConformLoad"
    network_triplets = net_position.call_args.kwargs["models_as_triplets"]
    assert isinstance(network_triplets, pl.DataFrame) and not network_triplets.is_empty()


@pytest.mark.pypowsybl
def test_handler_sends_metadata_even_when_net_position_fails(services, ieee14_igm):
    with mock.patch.object(model_validator, "get_ac_net_position", side_effect=KeyError("ControlArea")):
        handle([ieee14_igm])

    [metadata] = sent_metadata(services)
    assert metadata["valid"] is True
    assert len(sent_reports(services)) == 1


@pytest.mark.pypowsybl
@pytest.mark.parametrize("enabled", ["True", "False"])
def test_handler_sends_lvl8_report_via_edx_only_when_enabled(services, ieee14_igm, set_config, enabled):
    set_config(ENABLE_LVL8_REPORTS=enabled, QAS_EIC="10V000000000011X", QAS_MSG_TYPE="QAS-LVL8")

    handle([ieee14_igm])

    if enabled == "True":
        services.edx.send_message.assert_called_once()
        call = services.edx.send_message.call_args.kwargs
        assert (call["receiver_EIC"], call["business_type"]) == ("10V000000000011X", "QAS-LVL8")
        igm = etree.fromstring(call["content"]).find("{http://entsoe.eu/checks}IGM")
        assert (igm.get("tso"), igm.get("qualityIndicator")) == ("TSO", "Valid")
    else:
        services.edx_class.assert_not_called()
        services.edx.send_message.assert_not_called()
