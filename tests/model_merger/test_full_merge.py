import io
import uuid
import zipfile
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pypowsybl as pp
import pytest
from lxml import etree

from emf.common.helpers.cgmes import export_to_cgmes_zip, get_metadata_from_rdfxml
from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.time import parse_datetime
from emf.common.helpers.utils import attr_to_dict
from emf.common.loadflow_tool import settings_manager
from emf.model_merger import merge_functions, post_processing
from emf.model_merger.model_merger import HandlerMergeModels

pytestmark = pytest.mark.pypowsybl

SCENARIO_DATE = "2014-06-01T10:30:00Z"
MAS = "http://www.baltic-rsc.eu/OperationalPlanning"
SV_FILE = "20140601T1030Z_1D_BALTICRCC-EU_SV_001"
SSH_FILES = {"ELIA": "20140601T1030Z_1D_BALTICRCC-EU-ELIA_SSH_001", "TENNET": "20140601T1030Z_1D_BALTICRCC-EU-TENNET_SSH_001"}

# MicroGrid BaseCase header ids
ORIGINAL_SSH = {"ELIA": "52b712d1-f3b0-4a59-9191-79f2fb1e4c4e", "TENNET": "66085ffe-dddf-4fc8-805c-2c7aa2097b90"}
ORIGINAL_EQ = {"ELIA": "d400c631-75a0-4c30-8aed-832b0d282e73", "TENNET": "77b55f87-fc1e-4046-9599-6c6b4f991a86"}
ORIGINAL_TP = {"f2f43818-09c8-4252-9611-7af80c398d20", "5d32d257-1646-4906-a1f6-4d7ce3f91569"}
BOUNDARY_TP = "2399cbd1-9a39-11e0-aa80-0800200c9a66"


def _run_full_merge(input_models: list) -> SimpleNamespace:
    """Merge sequence of the merger (as in the emfos-smoke-run smoke_merge.py), without scaling and replacement"""
    # MicroGrid has no ControlArea, check_net_interchanges is replaced like smoke_merge.py does, the bug itself
    # is covered by an xfail in test_export_and_post_processing.py
    with mock.patch.object(settings_manager.LoadflowSettingsManager, "_get_defaults_from_elastic",
                           side_effect=ConnectionError("Elastic is not available in unit tests")), \
            mock.patch.object(post_processing, "check_net_interchanges",
                              new=lambda cgm_sv_data, cgm_ssh_data, original_models: cgm_ssh_data):
        merged_model = merge_functions.MergedModel()
        merged_model.network = load_network_model(opdm_objects=input_models)
        merged_model.network_meta = attr_to_dict(instance=merged_model.network, sanitize_to_strings=True)
        merged_model = HandlerMergeModels.apply_pre_loadflow_corrections(merged_model=merged_model)
        merged_model, loadflow_parameters = HandlerMergeModels.run_loadflow(merged_model=merged_model)

        opdm_object_meta = merge_functions.create_merged_model_opdm_object(
            object_id=merged_model.network_meta["id"].split("uuid:")[-1], time_horizon="1D",
            merging_entity="BALTICRCC", merging_area="EU", scenario_date=SCENARIO_DATE, mas=MAS, version="001")
        exported_model = merge_functions.export_merged_model(network=merged_model.network,
                                                             opdm_object_meta=opdm_object_meta,
                                                             profiles=["SV"], cgm_convention=False)
        sv_data, ssh_data, opdm_object_meta = post_processing.run_post_merge_processing(
            input_models=input_models, exported_model=exported_model, opdm_object_meta=opdm_object_meta,
            additional_processing=True)
        serialized = export_to_cgmes_zip([ssh_data, sv_data])

    zips = {item.name: item.getvalue() for item in serialized}
    xml_files = {}
    for name, content in zips.items():
        with zipfile.ZipFile(io.BytesIO(content)) as export:
            xml_files[name] = {xml_name: export.read(xml_name) for xml_name in export.namelist()}
    return SimpleNamespace(
        input_models=input_models,
        loadflow=merged_model.loadflow,
        loadflow_status=merged_model.loadflow_status,
        loadflow_settings=merged_model.loadflow_settings,
        loadflow_parameters=loadflow_parameters,
        bus_voltages=merged_model.network.get_bus_breaker_view_buses()[["v_mag", "v_angle"]],
        opdm_object_meta=opdm_object_meta,
        zips=zips,
        xml_files=xml_files,
    )


@pytest.fixture(scope="module")
def _merge_cache():
    return {}


@pytest.fixture
def full_merge(_merge_cache, microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    """MicroGrid BE + NL + boundary merged end to end, computed once per module, tests only read it"""
    if not _merge_cache:
        _merge_cache["result"] = _run_full_merge([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])
    return _merge_cache["result"]


def _stripped(reference: str) -> str:
    return reference.split(":")[-1]


def _headers(full_merge) -> dict:
    """{file name without extension: md:FullModel metadata}, references without the urn:uuid: prefix"""
    headers = {}
    for files in full_merge.xml_files.values():
        for xml_name, xml in files.items():
            metadata = get_metadata_from_rdfxml(etree.parse(io.BytesIO(xml)))
            for key in ("Model.DependentOn", "Model.Supersedes"):
                values = metadata.get(key, [])
                metadata[key] = [_stripped(v) for v in ([values] if isinstance(values, str) else values)]
            headers[xml_name.removesuffix(".xml")] = metadata
    return headers


def test_full_merge_solves_main_island_with_first_priority_settings(full_merge):
    assert full_merge.loadflow_status == "CONVERGED"
    assert full_merge.loadflow[0]["status"] == "CONVERGED"
    assert full_merge.loadflow_settings == "EU_DEFAULT"


def test_full_merge_exports_one_sv_and_an_updated_ssh_per_igm_named_by_mask(full_merge):
    expected = [SV_FILE, *SSH_FILES.values()]

    assert sorted(full_merge.zips) == sorted(f"{name}.zip" for name in expected)
    assert {zip_name: list(files) for zip_name, files in full_merge.xml_files.items()} == {
        f"{name}.zip": [f"{name}.xml"] for name in expected}


def test_full_merge_updated_ssh_supersede_their_original_ssh_with_new_mrid(full_merge):
    headers = _headers(full_merge)

    for tso, file_name in SSH_FILES.items():
        header = headers[file_name]
        assert header["Model.profile"] == "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1", tso
        assert header["Model.Supersedes"] == [ORIGINAL_SSH[tso]], tso
        assert header["Model.mRID"] != ORIGINAL_SSH[tso], tso
        assert str(uuid.UUID(header["Model.mRID"])) == header["Model.mRID"], tso
        assert header["Model.DependentOn"] == [ORIGINAL_EQ[tso]], tso


def test_full_merge_sv_depends_on_updated_ssh_and_all_tp_including_boundary(full_merge):
    headers = _headers(full_merge)
    sv = headers[SV_FILE]
    updated_ssh = {headers[file_name]["Model.mRID"] for file_name in SSH_FILES.values()}

    assert sv["Model.profile"] == "http://entsoe.eu/CIM/StateVariables/4/1"
    assert str(uuid.UUID(sv["Model.mRID"])) == sv["Model.mRID"]
    assert sorted(sv["Model.DependentOn"]) == sorted(ORIGINAL_TP | {BOUNDARY_TP} | updated_ssh)


def test_full_merge_keeps_scenario_time_and_writes_merged_model_version(full_merge):
    igm_scenario_times = {parse_datetime(model["pmd:scenarioDate"])
                          for model in full_merge.input_models if model["opde:Object-Type"] == "IGM"}

    headers = _headers(full_merge)

    assert len(headers) == 3
    assert {parse_datetime(header["Model.scenarioTime"]) for header in headers.values()} == igm_scenario_times
    assert {header["Model.version"] for header in headers.values()} == {"001"}


def test_full_merge_cgm_reassembles_from_igm_eq_tp_and_solves_to_the_merged_state(full_merge):
    cgm = io.BytesIO()
    with zipfile.ZipFile(cgm, "w") as cgm_zip:
        for model in full_merge.input_models:
            for component in model["opde:Component"]:
                profile = component["opdm:Profile"]
                if profile["pmd:cgmesProfile"] in ("EQ", "TP", "EQ_BD", "TP_BD"):
                    with zipfile.ZipFile(io.BytesIO(profile["DATA"])) as profile_zip:
                        for name in profile_zip.namelist():
                            cgm_zip.writestr(name, profile_zip.read(name))
        for files in full_merge.xml_files.values():
            for name, xml in files.items():
                cgm_zip.writestr(name, xml)
    cgm.seek(0)

    network = pp.network.load_from_binary_buffer(cgm, parameters={"iidm.import.cgmes.import-node-breaker-as-bus-breaker": "true"})
    results = pp.loadflow.run_ac(network, parameters=full_merge.loadflow_parameters)

    assert results[0].status == pp.loadflow.ComponentStatus.CONVERGED
    voltages = network.get_bus_breaker_view_buses()[["v_mag", "v_angle"]]
    pd.testing.assert_frame_equal(voltages.loc[full_merge.bus_voltages.index], full_merge.bus_voltages, atol=0.01, rtol=0)


@pytest.mark.xfail(strict=True, reason="pmd:fullModel_ID of the merged model comes from the network id (an IGM EQ mRID) "
                                       "and is never updated to the mRID of the exported SV")
def test_full_merge_opdm_metadata_identifies_the_exported_sv(full_merge):
    sv = _headers(full_merge)[SV_FILE]

    assert full_merge.opdm_object_meta["pmd:fullModel_ID"] == sv["Model.mRID"]
