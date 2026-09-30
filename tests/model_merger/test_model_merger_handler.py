import io
import json
import zipfile
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

from emf.common.helpers.opdm_objects import DataSource
from emf.common.loadflow_tool.settings_manager import LoadflowSettingsManager
from emf.model_merger import model_merger
from emf.model_merger.model_merger import HandlerMergeModels

CGM_FILES = ["CGM_SV.zip", "CGM_SSH_ELIA.zip", "CGM_SSH_TENNET.zip"]


def fake_model(tso, source=DataSource.OPDM, **fields):
    """OPDM object with just enough content for the handler when the pypowsybl steps are mocked"""
    profiles = ("EQ_BD", "TP_BD") if tso == "ENTSOE" else ("EQ", "SSH", "TP", "SV")
    return {"opde:Id": f"{tso}-{source.value}", "pmd:TSO": tso, "data-source": source, **fields,
            "opde:Component": [{"opdm:Profile": {"pmd:cgmesProfile": profile, "pmd:fileName": f"{tso}_{profile}.zip",
                                                 "DATA": f"{tso} {profile}".encode()}} for profile in profiles]}


def exported_files(_):
    files = []
    for name in CGM_FILES:
        item = io.BytesIO(name.encode())
        item.name = name
        files.append(item)
    return files


@pytest.fixture
def services():
    """Mocks every external service the handler talks to"""
    with ExitStack() as stack:
        patch = lambda name, **kwargs: stack.enter_context(mock.patch.object(model_merger, name, **kwargs))
        mocks = SimpleNamespace(
            update_task_status=patch("update_task_status"),
            query_acnp_schedules=patch("query_acnp_schedules", return_value=None),
            query_hvdc_schedules=patch("query_hvdc_schedules", return_value=None),
            calculate_ac_net_position=patch("calculate_ac_net_position", return_value=None),
            get_latest_models_and_download=patch("get_latest_models_and_download", return_value=[]),
            get_latest_boundary=patch("get_latest_boundary", return_value=fake_model("ENTSOE")),
            get_tsos_available_in_storage=patch("get_tsos_available_in_storage", return_value=[]),
            async_call=patch("async_call"),
            elastic=stack.enter_context(mock.patch.object(model_merger.elastic, "Elastic")),
            opdm=stack.enter_context(mock.patch.object(model_merger.opdm, "OPDM")),
            edx=stack.enter_context(mock.patch.object(model_merger.edx, "EDX")),
        )
        stack.enter_context(mock.patch.object(LoadflowSettingsManager, "_get_defaults_from_elastic",
                                              side_effect=ConnectionError("no Elastic in unit tests")))
        mocks.handler = HandlerMergeModels()
        mocks.minio = mocks.handler.minio_service = mock.MagicMock()
        yield mocks


@pytest.fixture
def merge_steps(services):
    """Replaces model loading, load flow, scaling, export and post-processing; set loadflow_status/scaled per test"""
    steps = SimpleNamespace(loadflow_status="CONVERGED", scaled=True)

    def run_loadflow(merged_model):
        merged_model.loadflow = [{"connected_component_num": 0, "status": steps.loadflow_status, "reference_bus_id": "bus"}]
        merged_model.loadflow_status = steps.loadflow_status
        merged_model.loadflow_settings = "EU_DEFAULT"
        return merged_model, mock.sentinel.loadflow_parameters

    def scale_balance(model, **_):
        model.scaled = steps.scaled
        return model

    with ExitStack() as stack:
        patch = lambda target, name, **kwargs: stack.enter_context(mock.patch.object(target, name, **kwargs))
        patch(HandlerMergeModels, "apply_pre_loadflow_corrections", side_effect=lambda merged_model: merged_model)
        patch(HandlerMergeModels, "run_loadflow", side_effect=run_loadflow)
        patch(model_merger.scaler, "scale_balance", side_effect=scale_balance)
        patch(model_merger.merge_functions, "export_merged_model", return_value=io.BytesIO(b"exported SV"))
        patch(model_merger.post_processing, "run_post_merge_processing",
              side_effect=lambda input_models, exported_model, opdm_object_meta, additional_processing: (None, None, opdm_object_meta))
        patch(model_merger, "attr_to_dict", return_value={"id": "urn:uuid:5a1b1f1c-0000-4000-8000-000000000001"})
        patch(model_merger, "get_network_elements", return_value=pd.DataFrame({"connected_component": [0, 0]}))
        patch(model_merger, "export_to_cgmes_zip", side_effect=exported_files)
        steps.load_network_model = patch(model_merger, "load_network_model")
        steps.run_replacement = patch(model_merger, "run_replacement", side_effect=lambda igm_models, **_: igm_models)
        yield steps


def handle(services, task, **task_properties):
    task["task_properties"].update(task_properties)
    return services.handler.handle(task, SimpleNamespace(headers={}))


def sent_to_elastic(services, index):
    return [c.kwargs["json_message"] for c in services.elastic.send_to_elastic.call_args_list if c.kwargs["index"] == index]


def merge_report(services):
    [report] = sent_to_elastic(services, model_merger.MERGE_REPORT_ELK_INDEX)
    return report


def with_schedules(services):
    services.query_acnp_schedules.return_value = [{"ac schedule": 1}]
    services.query_hvdc_schedules.return_value = [{"dc schedule": 1}]


def test_handle_without_igms_returns_task_as_unsuccessful(services, merge_steps, merge_task):
    body, properties = handle(services, merge_task)

    assert body is merge_task
    assert properties.headers["success"] is False
    merge_steps.load_network_model.assert_not_called()
    services.minio.upload_object.assert_not_called()
    services.opdm.assert_not_called()


@pytest.mark.parametrize("merge_type, model_type", [("EU", "CGM"), ("BA", "RMM")])
def test_handle_returns_merged_model_metadata(services, merge_steps, merge_task, merge_type, model_type):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA"), fake_model("TENNET")]

    body, properties = handle(services, merge_task, merge_type=merge_type)

    metadata = json.loads(body)
    assert metadata["opde:Object-Type"] == "CGM"
    assert metadata["pmd:Area"] == merge_type
    # ID is resolved to the hours between task creation (13:16) and scenario time (22:30)
    assert metadata["pmd:timeHorizon"] == "09"
    assert metadata["pmd:content-reference"].startswith(
        f"{model_merger.OUTPUT_MINIO_FOLDER}/ID/{model_type}_09_001_20230414T2230Z_{merge_type}_")
    assert properties.headers == {key: value for key, value in metadata.items() if isinstance(value, str)}
    assert sent_to_elastic(services, model_merger.OPDE_MODELS_ELK_INDEX) == [metadata]
    assert services.update_task_status.call_args.args[1] == "finished"


def test_handle_uploads_cgm_with_original_eq_and_tp_to_minio(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA"), fake_model("TENNET")]

    body, _ = handle(services, merge_task)

    upload = services.minio.upload_object.call_args.kwargs
    assert upload["bucket_name"] == model_merger.OUTPUT_MINIO_BUCKET
    assert upload["metadata"] == {"trustability": "not_evaluated", "untrustability_reason": None}
    assert upload["file_path_or_file_object"].name == json.loads(body)["pmd:content-reference"]
    with zipfile.ZipFile(upload["file_path_or_file_object"]) as merged_zip:
        assert merged_zip.namelist() == CGM_FILES + ["ELIA_EQ.zip", "ELIA_TP.zip", "TENNET_EQ.zip", "TENNET_TP.zip",
                                                     "ENTSOE_EQ_BD.zip", "ENTSOE_TP_BD.zip"]
    assert merge_report(services)["uploaded_to_minio"] is True


def test_handle_uploads_secondary_minio_copy_when_configured(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA")]
    uploaded_names = []
    services.minio.upload_object.side_effect = lambda file_path_or_file_object, **_: uploaded_names.append(
        file_path_or_file_object.name)

    with mock.patch.object(model_merger, "OUTPUT_MINIO_COPY_FOLDER", "EMFOS/COPY"):
        body, _ = handle(services, merge_task)

    content_reference = json.loads(body)["pmd:content-reference"]
    assert uploaded_names == [content_reference, content_reference.replace(model_merger.OUTPUT_MINIO_FOLDER, "EMFOS/COPY", 1)]


@pytest.mark.parametrize("upload_to_opdm, loadflow_status, scaling, scaled, uploaded", [
    pytest.param("True", "CONVERGED", "True", True, True, id="converged-and-scaled"),
    pytest.param("True", "CONVERGED", "True", False, False, id="scaling-failed"),
    pytest.param("True", "CONVERGED", "False", None, False, id="scaling-disabled"),
    pytest.param("True", "FAILED", "True", True, False, id="loadflow-failed"),
    pytest.param("False", "CONVERGED", "True", True, False, id="upload-disabled"),
])
def test_handle_uploads_to_opdm_only_when_converged_and_scaled(services, merge_steps, merge_task, upload_to_opdm,
                                                               loadflow_status, scaling, scaled, uploaded):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA"), fake_model("TENNET")]
    with_schedules(services)
    merge_steps.loadflow_status, merge_steps.scaled = loadflow_status, scaled

    with mock.patch.object(model_merger, "SEND_TYPE", "FS"):
        handle(services, merge_task, upload_to_opdm=upload_to_opdm, scaling=scaling)

    put_file = services.opdm.return_value.put_file
    assert [c.kwargs["file_id"] for c in put_file.call_args_list] == (CGM_FILES if uploaded else [])
    assert merge_report(services)["uploaded_to_opde"] is uploaded


def test_handle_soap_send_type_publishes_each_file_asynchronously(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA"), fake_model("TENNET")]
    with_schedules(services)

    with mock.patch.object(model_merger, "SEND_TYPE", "SOAP"):
        handle(services, merge_task, scaling="True")

    calls = services.async_call.call_args_list
    assert [c.kwargs["file_path_or_file_object"].name for c in calls] == CGM_FILES
    assert all(c.kwargs["function"] is services.opdm.return_value.publication_request for c in calls)
    services.opdm.return_value.put_file.assert_not_called()
    assert merge_report(services)["uploaded_to_opde"] is True


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason='shipped SEND_TYPE "FS/SOAP" matches neither upload branch, yet uploaded_to_opde is set True')
def test_handle_with_shipped_send_type_uploads_or_reports_not_uploaded(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA"), fake_model("TENNET")]
    with_schedules(services)

    handle(services, merge_task, scaling="True")

    sent = services.opdm.return_value.put_file.called or services.async_call.called
    assert sent or merge_report(services)["uploaded_to_opde"] is False


def test_handle_hands_included_tso_outside_acnp_deadband_to_replacement(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [
        fake_model("ELIA", ac_net_position=120.0, sum_conform_load=1000.0),
        fake_model("TENNET", ac_net_position=900.0, sum_conform_load=1000.0)]
    services.calculate_ac_net_position.return_value = {"ELIA": 100.0, "TENNET": 100.0}

    handle(services, merge_task, included=["ELIA", "TENNET"], replacement="True")

    replacement_args = merge_steps.run_replacement.call_args.kwargs
    assert replacement_args["missing_models"] == ["TENNET"]
    assert [model["pmd:TSO"] for model in replacement_args["igm_models"]] == ["ELIA"]
    assert replacement_args["acnp_dict"] == {"ELIA": 100.0, "TENNET": 100.0}
    assert merge_report(services)["excluded"] == [{"tso": "TENNET", "reason": "acnp-outside-schedule-deadband"}]


def test_handle_baltic_merge_takes_missing_opdm_model_from_pdn(services, merge_steps, merge_task):
    available = {DataSource.OPDM: [fake_model("ELIA")], DataSource.PDN: [fake_model("TENNET", DataSource.PDN)]}
    services.get_latest_models_and_download.side_effect = lambda tso, data_source, **_: [
        model for model in available[data_source] if model["pmd:TSO"] in tso]

    handle(services, merge_task, merge_type="BA", included=["ELIA", "TENNET"], replacement="True")

    pdn_query = services.get_latest_models_and_download.call_args_list[-1].kwargs
    assert (pdn_query["data_source"], pdn_query["tso"]) == (DataSource.PDN, ["TENNET"])
    merged = merge_steps.load_network_model.call_args.kwargs["opdm_objects"]
    assert [(model["pmd:TSO"], model["data-source"]) for model in merged] == [
        ("ELIA", DataSource.OPDM), ("TENNET", DataSource.PDN), ("ENTSOE", DataSource.OPDM)]
    assert merge_steps.run_replacement.call_args.kwargs["missing_models"] == []


def test_handle_does_not_replace_tsos_excluded_by_task(services, merge_steps, merge_task):
    services.get_latest_models_and_download.return_value = [fake_model("ELIA")]
    services.get_tsos_available_in_storage.return_value = ["ELIA", "TENNET", "RTE"]

    handle(services, merge_task, excluded=["RTE"], replacement="True")

    assert merge_steps.run_replacement.call_args.kwargs["missing_models"] == ["TENNET"]


@pytest.mark.pypowsybl
def test_handle_merges_microgrid(services, merge_task, microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    services.get_latest_models_and_download.return_value = [microgrid_be_igm, microgrid_nl_igm]
    services.get_latest_boundary.return_value = microgrid_boundary

    # MicroGrid has no ControlArea, which check_net_interchanges doesn't handle
    with mock.patch.object(model_merger.post_processing, "check_net_interchanges",
                           side_effect=lambda cgm_sv_data, cgm_ssh_data, original_models: cgm_ssh_data):
        body, _ = handle(services, merge_task, timestamp_utc="2014-06-01T10:30:00+00:00", time_horizon="1D",
                         lvl8_reporting="False", post_temp_fixes="False")

    metadata = json.loads(body)
    assert merge_report(services)["loadflow"][0]["status"] == "CONVERGED"
    assert metadata["pmd:scenarioDate"] == "2014-06-01T10:30:00Z"
    merged_zip = services.minio.upload_object.call_args.kwargs["file_path_or_file_object"]
    assert merged_zip.name == metadata["pmd:content-reference"]
    with zipfile.ZipFile(merged_zip) as archive:
        profiles = sorted(name.split("_")[-2] for name in archive.namelist())
    assert profiles == ["EQ", "EQ", "EQBD", "SSH", "SSH", "SV", "TP", "TP", "TPBD"]
    services.opdm.assert_not_called()
