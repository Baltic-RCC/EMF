import copy
import io
import uuid
import zipfile
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import polars as pl
import pytest

from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.helpers.utils import attr_to_dict
from emf.common.loadflow_tool import settings_manager
from emf.model_merger import merge_functions, post_processing
from emf.model_merger.model_merger import HandlerMergeModels

MAS = "http://www.baltic-rsc.eu/OperationalPlanning"
CREATION_DATE = "2024-05-06T07:08:09.123456Z"
SSH_PROFILE = "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1"
TP_PROFILE = "http://entsoe.eu/CIM/Topology/4/1"
TP_BD_PROFILE = "http://entsoe.eu/CIM/TopologyBoundary/3/1"

# MicroGrid BaseCase header ids
MICROGRID_ORIGINAL_SSH = {"ELIA": "52b712d1-f3b0-4a59-9191-79f2fb1e4c4e", "TENNET": "66085ffe-dddf-4fc8-805c-2c7aa2097b90"}
MICROGRID_TP = {"f2f43818-09c8-4252-9611-7af80c398d20", "5d32d257-1646-4906-a1f6-4d7ce3f91569"}
MICROGRID_TP_BD = "2399cbd1-9a39-11e0-aa80-0800200c9a66"


def _merged_model_meta(version: str = "001") -> dict:
    meta = merge_functions.create_merged_model_opdm_object(
        object_id=str(uuid.uuid4()), time_horizon="1D", merging_entity="BALTICRCC", merging_area="EU",
        scenario_date="2014-06-01T10:30:00Z", mas=MAS, version=version)
    return {**meta, "pmd:creationDate": CREATION_DATE}


def _values(data, key: str, object_id: str | None = None) -> list:
    data = data.to_pandas() if isinstance(data, pl.DataFrame) else data
    rows = data[data.KEY.astype(str) == key]
    if object_id is not None:
        rows = rows[rows.ID.astype(str) == object_id]
    return rows.VALUE.astype(str).tolist()


def _header(data) -> dict:
    """mRID and Model.* values of the single FullModel in data, Model.DependentOn as a list"""
    data = data.to_pandas() if isinstance(data, pl.DataFrame) else data
    full_model = data[(data.KEY.astype(str) == "Type") & (data.VALUE.astype(str) == "FullModel")].ID.iloc[0]
    rows = data[(data.ID == full_model) & data.KEY.astype(str).str.startswith("Model.")]
    header = {"mRID": full_model}
    for key, value in zip(rows.KEY.astype(str), rows.VALUE.astype(str)):
        if key == "Model.DependentOn":
            header.setdefault(key, []).append(value)
        else:
            header[key] = value
    return header


@pytest.fixture
def pl_triplets(make_triplets):
    def _build(rows, instance_id: str = "test-instance"):
        return pl.from_pandas(make_triplets(rows, instance_id))
    return _build


# ---------------------------------------------------------------- merged model metadata and SV export

@pytest.mark.parametrize("version, expected", [("1", "001"), ("12", "012"), ("003", "003")])
def test_create_merged_model_opdm_object_formats_version_and_scenario(version, expected):
    meta = merge_functions.create_merged_model_opdm_object(
        object_id="id", time_horizon="1D", merging_entity="BALTICRCC", merging_area="EU",
        scenario_date="2014-06-01T10:30:00Z", mas=MAS, version=version)

    assert meta["pmd:versionNumber"] == expected
    assert meta["pmd:validFrom"] == "20140601T1030Z"
    assert meta["pmd:scenarioDate"] == "2014-06-01T10:30:00Z"
    assert meta["pmd:modelPartReference"] == "BALTICRCC-EU"
    assert meta["pmd:modelingAuthoritySet"] == MAS


def test_update_header_from_opdm_object_overwrites_full_model_header(make_triplets):
    data = make_triplets([
        ("fm", "Type", "FullModel"),
        ("fm", "Model.version", "7"),
        ("fm", "Model.created", "2020-01-01T00:00:00Z"),
        ("fm", "Model.scenarioTime", "2020-01-01T00:30:00Z"),
        ("fm", "Model.processType", "YR"),
        ("fm", "Model.description", "original"),
        ("fm", "Model.mergingEntity", "OTHER"),
        ("fm", "Model.domain", "BA"),
    ])
    meta = _merged_model_meta(version="2")

    result = merge_functions.update_header_from_opdm_object(data, meta)

    assert _header(result) == {
        "mRID": "fm",
        "Model.version": "002",
        "Model.created": CREATION_DATE,
        "Model.scenarioTime": "2014-06-01T10:30:00Z",
        "Model.processType": "1D",
        "Model.description": meta["pmd:description"],
        "Model.mergingEntity": "BALTICRCC",
        "Model.domain": "EU",
    }


SV_XML = """<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:cim="http://iec.ch/TC57/2013/CIM-schema-cim16#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">
  <md:FullModel rdf:about="urn:uuid:{full_model_id}">
    <md:Model.created>2024-01-01T00:00:00Z</md:Model.created>
    <md:Model.scenarioTime>2014-06-01T10:30:00Z</md:Model.scenarioTime>
    <md:Model.version>1</md:Model.version>
    <md:Model.description>SV Model</md:Model.description>
    <md:Model.profile>http://entsoe.eu/CIM/StateVariables/4/1</md:Model.profile>
    <md:Model.modelingAuthoritySet>http://www.baltic-rsc.eu/OperationalPlanning</md:Model.modelingAuthoritySet>
  </md:FullModel>
  <cim:SvVoltage rdf:ID="_5f0a4c1e-7a7e-4d52-9d7a-6a7cf1f0e001">
    <cim:SvVoltage.v>400.0</cim:SvVoltage.v>
    <cim:SvVoltage.TopologicalNode rdf:resource="#_5f0a4c1e-7a7e-4d52-9d7a-6a7cf1f0e002"/>
  </cim:SvVoltage>
</rdf:RDF>"""


def _exported_sv(full_model_id: str) -> io.BytesIO:
    """SV-only zip as pypowsybl exports it"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as export:
        export.writestr("20140601T1030Z_1D_BALTICRCC-EU_SV_001_SV.xml", SV_XML.format(full_model_id=full_model_id))
    buffer.name = "20140601T1030Z_1D_BALTICRCC-EU_SV_001_1234.zip"
    buffer.seek(0)
    return buffer


@pytest.mark.parametrize("version, expected_label", [
    ("1", "20140601T1030Z_1D_BALTICRCC-EU_SV_001.xml"),
    ("12", "20140601T1030Z_1D_BALTICRCC-EU_SV_012.xml"),
])
def test_update_merged_model_sv_names_file_by_mask_and_writes_header_from_opdm_object(version, expected_label):
    meta = _merged_model_meta(version=version)

    sv_data = merge_functions.update_merged_model_sv(_exported_sv(str(uuid.uuid4())), meta)

    assert _values(sv_data, "label") == [expected_label]
    header = _header(sv_data)
    assert {k: header[k] for k in ("Model.version", "Model.created", "Model.scenarioTime", "Model.processType",
                                   "Model.mergingEntity", "Model.domain", "Model.messageType", "Model.description")} == {
        "Model.version": f"{int(version):03d}",
        "Model.created": CREATION_DATE,
        "Model.scenarioTime": "2014-06-01T10:30:00Z",
        "Model.processType": "1D",
        "Model.mergingEntity": "BALTICRCC",
        "Model.domain": "EU",
        "Model.messageType": "SV",
        "Model.description": meta["pmd:description"],
    }


@pytest.mark.parametrize("full_model_id, keeps_id", [
    ("77b55f87-fc1e-4046-9599-6c6b4f991a86+d400c631_N_STATE_VARIABLES_2014-06-01T10:30:00Z_1_1D__FM", False),
    ("0c7e3f5a-3b0e-4b8e-9a53-0d6f8f3b1f11", True),
])
def test_update_merged_model_sv_gives_the_sv_a_valid_uuid_mrid(full_model_id, keeps_id):
    sv_data = merge_functions.update_merged_model_sv(_exported_sv(full_model_id), _merged_model_meta())

    mrid = _header(sv_data)["mRID"]
    assert str(uuid.UUID(mrid)) == mrid
    assert (mrid == full_model_id) is keeps_id


def _updated_ssh_inputs(make_triplets):
    ssh = make_triplets([
        ("dist", "Type", "Distribution"),
        ("dist", "label", "20140601T1030Z_1D_ELIA_SSH_001.xml"),
        ("ssh-old", "Type", "FullModel"),
        ("ssh-old", "Model.scenarioTime", "2014-06-01T10:30:00Z"),
        ("ssh-old", "Model.version", "1"),
        ("ssh-old", "Model.profile", SSH_PROFILE),
        ("ec", "Type", "EnergyConsumer"),
        ("ec", "EnergyConsumer.p", "10"),
        ("ec", "EnergyConsumer.q", "2"),
        ("tc", "Type", "RatioTapChanger"),
        ("tc", "TapChanger.step", "3"),
        ("sh", "Type", "LinearShuntCompensator"),
        ("sh", "ShuntCompensator.sections", "1"),
    ], instance_id="ssh-instance")
    eq = make_triplets([
        ("dist-eq", "Type", "Distribution"),
        ("dist-eq", "label", "20140601T1030Z_1D_ELIA_EQ_001.xml"),
        ("t-ec", "Type", "Terminal"),
        ("t-ec", "Terminal.ConductingEquipment", "ec"),
    ], instance_id="eq-instance")
    sv = make_triplets([
        ("sv", "Type", "FullModel"),
        ("sv", "Model.DependentOn", "ssh-old"),
        ("pf", "Type", "SvPowerFlow"),
        ("pf", "SvPowerFlow.Terminal", "t-ec"),
        ("pf", "SvPowerFlow.p", "12.5"),
        ("pf", "SvPowerFlow.q", "3.5"),
        ("st", "Type", "SvTapStep"),
        ("st", "SvTapStep.TapChanger", "tc"),
        ("st", "SvTapStep.position", "4"),
        ("ss", "Type", "SvShuntCompensatorSections"),
        ("ss", "SvShuntCompensatorSections.ShuntCompensator", "sh"),
        ("ss", "SvShuntCompensatorSections.sections", "0"),
    ], instance_id="sv-instance")
    return pd.concat([ssh, eq], ignore_index=True), sv


def test_create_updated_ssh_supersedes_original_ssh_with_new_mrid(make_triplets):
    models, sv = _updated_ssh_inputs(make_triplets)

    sv_data, ssh_data, _ = merge_functions.create_updated_ssh(models, sv, _merged_model_meta())

    header = _header(ssh_data)
    assert header["mRID"] != "ssh-old"
    assert str(uuid.UUID(header["mRID"])) == header["mRID"]
    assert header["Model.Supersedes"] == "ssh-old"
    assert _values(sv_data, "Model.DependentOn") == [header["mRID"]]
    assert set(ssh_data.INSTANCE_ID.astype(str)) == {"ssh-instance"}


def test_create_updated_ssh_takes_solved_state_from_sv(make_triplets):
    models, sv = _updated_ssh_inputs(make_triplets)

    _, ssh_data, _ = merge_functions.create_updated_ssh(models, sv, _merged_model_meta())

    assert float(_values(ssh_data, "EnergyConsumer.p", "ec")[0]) == 12.5
    assert float(_values(ssh_data, "EnergyConsumer.q", "ec")[0]) == 3.5
    assert float(_values(ssh_data, "TapChanger.step", "tc")[0]) == 4
    assert float(_values(ssh_data, "ShuntCompensator.sections", "sh")[0]) == 0


def test_create_updated_ssh_writes_merged_header_and_file_name(make_triplets):
    models, sv = _updated_ssh_inputs(make_triplets)

    _, ssh_data, _ = merge_functions.create_updated_ssh(models, sv, _merged_model_meta(version="4"))

    assert _values(ssh_data, "label") == ["20140601T1030Z_1D_BALTICRCC-EU-ELIA_SSH_004.xml"]
    header = _header(ssh_data)
    assert (header["Model.scenarioTime"], header["Model.version"], header["Model.mergingEntity"]) == (
        "2014-06-01T10:30:00Z", "004", "BALTICRCC")


# ---------------------------------------------------------------- SV clean-up steps

@pytest.mark.parametrize("limit, remaining_islands", [(1, {"small", "big"}), (2, {"big"}), (3, set())])
def test_remove_small_islands_drops_islands_up_to_the_size_limit(pl_triplets, limit, remaining_islands):
    sv = pl_triplets([
        ("small", "Type", "TopologicalIsland"),
        ("small", "TopologicalIsland.TopologicalNodes", "tn1"),
        ("small", "TopologicalIsland.TopologicalNodes", "tn2"),
        ("small", "TopologicalIsland.AngleRefTopologicalNode", "tn1"),
        ("big", "Type", "TopologicalIsland"),
        ("big", "TopologicalIsland.TopologicalNodes", "tn3"),
        ("big", "TopologicalIsland.TopologicalNodes", "tn4"),
        ("big", "TopologicalIsland.TopologicalNodes", "tn5"),
        ("v1", "Type", "SvVoltage"),
    ])

    result = post_processing.remove_small_islands(sv, island_size_limit=limit)

    assert set(result.filter(pl.col("VALUE") == "TopologicalIsland")["ID"]) == remaining_islands
    assert set(result["ID"]) == remaining_islands | {"v1"}


def test_remove_equivalent_shunt_section_drops_sections_only_for_equivalent_shunts(pl_triplets):
    models = pl_triplets([("eqs", "Type", "EquivalentShunt"), ("lsc", "Type", "LinearShuntCompensator")])
    sv = pl_triplets([
        ("s1", "Type", "SvShuntCompensatorSections"),
        ("s1", "SvShuntCompensatorSections.ShuntCompensator", "eqs"),
        ("s1", "SvShuntCompensatorSections.sections", "1"),
        ("s2", "Type", "SvShuntCompensatorSections"),
        ("s2", "SvShuntCompensatorSections.ShuntCompensator", "lsc"),
        ("s2", "SvShuntCompensatorSections.sections", "2"),
    ])

    result = post_processing.remove_equivalent_shunt_section(sv, models)

    assert set(result["ID"]) == {"s2"}
    assert result.height == 3


def test_add_missing_sv_tap_steps_adds_step_from_ssh_for_tap_changers_without_one(pl_triplets):
    ssh = pl_triplets([("tc1", "TapChanger.step", "5"), ("tc2", "TapChanger.step", "7")], instance_id="ssh")
    sv = pl_triplets([
        ("st1", "Type", "SvTapStep"),
        ("st1", "SvTapStep.TapChanger", "tc1"),
        ("st1", "SvTapStep.position", "6"),
    ], instance_id="sv")

    result = post_processing.add_missing_sv_tap_steps(sv, ssh)

    new = result.filter(~pl.col("ID").is_in(["st1"]))
    new_id = new["ID"].unique().to_list()
    assert len(new_id) == 1
    assert dict(zip(new["KEY"], new["VALUE"])) == {"Type": "SvTapStep", "SvTapStep.TapChanger": "tc2",
                                                   "SvTapStep.position": "7"}
    assert set(new["INSTANCE_ID"]) == {"sv"}
    assert _values(result, "SvTapStep.position", "st1") == ["6"]


def test_add_missing_sv_tap_steps_keeps_sv_when_nothing_is_missing(pl_triplets):
    ssh = pl_triplets([("tc1", "TapChanger.step", "5")])
    sv = pl_triplets([("st1", "Type", "SvTapStep"), ("st1", "SvTapStep.TapChanger", "tc1")])

    assert post_processing.add_missing_sv_tap_steps(sv, ssh).equals(sv)


def test_check_and_fix_dependencies_points_sv_to_all_tp_and_new_ssh(pl_triplets):
    original = pl.concat([
        pl_triplets([("tp-be", "Type", "FullModel"), ("tp-be", "Model.profile", TP_PROFILE)], "tp"),
        pl_triplets([("tp-bd", "Type", "FullModel"), ("tp-bd", "Model.profile", TP_BD_PROFILE)], "tp-bd"),
        pl_triplets([("eq-be", "Type", "FullModel"), ("eq-be", "Model.profile", "http://entsoe.eu/CIM/EquipmentCore/3/1")], "eq"),
        pl_triplets([("ssh-old", "Type", "FullModel"), ("ssh-old", "Model.profile", SSH_PROFILE)], "ssh-old"),
    ])
    ssh = pl.concat([
        pl_triplets([("ssh-be", "Type", "FullModel"), ("ssh-be", "Model.profile", SSH_PROFILE)], "ssh-be"),
        pl_triplets([("ssh-nl", "Type", "FullModel"), ("ssh-nl", "Model.profile", SSH_PROFILE)], "ssh-nl"),
    ])
    sv = pl_triplets([("sv", "Type", "FullModel"), ("sv", "Model.DependentOn", "pypowsybl-tp")], instance_id="sv")

    result = post_processing.check_and_fix_dependencies(cgm_sv_data=sv, cgm_ssh_data=ssh, original_data=original)

    dependencies = result.filter(pl.col("KEY") == "Model.DependentOn")
    assert set(dependencies["VALUE"]) == {"tp-be", "tp-bd", "ssh-be", "ssh-nl"}
    assert set(dependencies["ID"]) == {"sv"}
    assert set(dependencies["INSTANCE_ID"]) == {"sv"}


BOUNDARY_NODE_ROWS = [
    ("tn-b", "Type", "TopologicalNode"),
    ("tn-b", "TopologicalNode.boundaryPoint", "true"),
    ("tn-i", "Type", "TopologicalNode"),
    ("tn-i", "TopologicalNode.boundaryPoint", "false"),
]


@pytest.mark.parametrize("first_v, second_v, kept", [("0", "400.1", "v2"), ("400.1", "0", "v1"), ("401", "400", "v1")])
def test_remove_duplicate_sv_voltages_keeps_one_non_zero_voltage_per_shared_boundary_node(
        pl_triplets, first_v, second_v, kept):
    # both IGM SV files hold a voltage for the shared boundary node
    original = pl_triplets(BOUNDARY_NODE_ROWS + [
        ("o1", "SvVoltage.TopologicalNode", "tn-b"),
        ("o2", "SvVoltage.TopologicalNode", "tn-b"),
        ("o3", "SvVoltage.TopologicalNode", "tn-i"),
    ])
    sv = pl_triplets([
        ("v1", "Type", "SvVoltage"), ("v1", "SvVoltage.TopologicalNode", "tn-b"), ("v1", "SvVoltage.v", first_v),
        ("v2", "Type", "SvVoltage"), ("v2", "SvVoltage.TopologicalNode", "tn-b"), ("v2", "SvVoltage.v", second_v),
        ("v3", "Type", "SvVoltage"), ("v3", "SvVoltage.TopologicalNode", "tn-i"), ("v3", "SvVoltage.v", "110"),
    ])

    result = post_processing.remove_duplicate_sv_voltages(cgm_sv_data=sv, original_data=original)

    assert set(result["ID"]) == {kept, "v3"}
    assert result.height == 6


# ---------------------------------------------------------------- SSH consistency and injection fixes

def test_set_paired_boundary_injections_to_zero_only_touches_paired_injections(pl_triplets):
    original = pl_triplets([
        ("tn-p", "TopologicalNode.boundaryPoint", "true"),
        ("tn-u", "TopologicalNode.boundaryPoint", "true"),
        ("t1", "Type", "Terminal"), ("t1", "Terminal.ConductingEquipment", "ei1"), ("t1", "Terminal.TopologicalNode", "tn-p"),
        ("t2", "Type", "Terminal"), ("t2", "Terminal.ConductingEquipment", "ei2"), ("t2", "Terminal.TopologicalNode", "tn-p"),
        ("t3", "Type", "Terminal"), ("t3", "Terminal.ConductingEquipment", "ei3"), ("t3", "Terminal.TopologicalNode", "tn-u"),
    ])
    ssh_rows = [("t1", "ACDCTerminal.connected", "false"), ("t2", "ACDCTerminal.connected", "true"),
                ("t3", "ACDCTerminal.connected", "false")]
    for ei, p in (("ei1", "10"), ("ei2", "-10"), ("ei3", "5")):
        ssh_rows += [(ei, "Type", "EquivalentInjection"), (ei, "EquivalentInjection.p", p),
                     (ei, "EquivalentInjection.q", p), (ei, "EquivalentInjection.regulationStatus", "true")]
    ssh = pl_triplets(ssh_rows)

    result = post_processing.set_paired_boundary_injections_to_zero(original_models=original, cgm_ssh_data=ssh)

    for ei, terminal in (("ei1", "t1"), ("ei2", "t2")):
        assert float(_values(result, "EquivalentInjection.p", ei)[0]) == 0
        assert float(_values(result, "EquivalentInjection.q", ei)[0]) == 0
        assert _values(result, "EquivalentInjection.regulationStatus", ei) == ["false"]
        assert _values(result, "ACDCTerminal.connected", terminal) == ["true"]
    assert _values(result, "EquivalentInjection.p", "ei3") == ["5"]
    assert _values(result, "EquivalentInjection.regulationStatus", "ei3") == ["true"]
    assert _values(result, "ACDCTerminal.connected", "t3") == ["false"]
    assert result.height == ssh.height


def _injection_case(pl_triplets, sv_p: str, injection: str = "ExternalNetworkInjection", key: str = "p"):
    original = pl_triplets([
        ("inj", "Type", injection), ("inj", f"{injection}.{key}", "50"),
        ("t", "Type", "Terminal"), ("t", "Terminal.ConductingEquipment", "inj"),
    ])
    ssh = pl_triplets([("inj", "Type", injection), ("inj", f"{injection}.{key}", "50")])
    sv = pl_triplets([("pf", "Type", "SvPowerFlow"), ("pf", "SvPowerFlow.Terminal", "t"), ("pf", "SvPowerFlow.p", sv_p)])
    return sv, ssh, original


@pytest.mark.parametrize("sv_p, threshold, fix_errors, expected", [
    ("60", 0.1, True, 60.0),
    ("50.05", 0.1, True, 50.0),
    ("60", 0.1, False, 50.0),
    ("60", 20.0, True, 50.0),
])
@pytest.mark.parametrize("injection, key", [("ExternalNetworkInjection", "p"), ("EnergySource", "activePower")])
def test_check_all_kind_of_injections_copies_solved_flow_to_ssh_above_threshold_when_fixing(
        pl_triplets, injection, key, sv_p, threshold, fix_errors, expected):
    sv, ssh, original = _injection_case(pl_triplets, sv_p, injection, key)

    result = post_processing.check_all_kind_of_injections(
        cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original, injection_name=injection,
        fields_to_check={"SvPowerFlow.p": f"{injection}.{key}"}, threshold=threshold, fix_errors=fix_errors)

    assert float(_values(result, f"{injection}.{key}", "inj")[0]) == expected


def test_check_all_kind_of_injections_without_injections_of_that_type_returns_ssh_unchanged(pl_triplets):
    sv, ssh, original = _injection_case(pl_triplets, "60")

    result = post_processing.check_all_kind_of_injections(
        cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original, injection_name="EnergySource",
        fields_to_check={"SvPowerFlow.p": "EnergySource.activePower"}, fix_errors=True)

    assert result.equals(ssh)


def test_check_non_boundary_equivalent_injections_fixes_only_injections_off_the_boundary(pl_triplets):
    original = pl_triplets([
        ("tn-b", "TopologicalNode.boundaryPoint", "true"),
        ("tb", "Type", "Terminal"), ("tb", "Terminal.ConductingEquipment", "ei-b"), ("tb", "Terminal.TopologicalNode", "tn-b"),
        ("ti", "Type", "Terminal"), ("ti", "Terminal.ConductingEquipment", "ei-i"), ("ti", "Terminal.TopologicalNode", "tn-i"),
        ("ei-b", "Type", "EquivalentInjection"), ("ei-b", "EquivalentInjection.p", "10"),
        ("ei-i", "Type", "EquivalentInjection"), ("ei-i", "EquivalentInjection.p", "20"),
    ])
    ssh = pl_triplets([
        ("ei-b", "Type", "EquivalentInjection"), ("ei-b", "EquivalentInjection.p", "10"),
        ("ei-i", "Type", "EquivalentInjection"), ("ei-i", "EquivalentInjection.p", "20"),
    ])
    sv = pl_triplets([
        ("pf-b", "Type", "SvPowerFlow"), ("pf-b", "SvPowerFlow.Terminal", "tb"), ("pf-b", "SvPowerFlow.p", "99"),
        ("pf-i", "Type", "SvPowerFlow"), ("pf-i", "SvPowerFlow.Terminal", "ti"), ("pf-i", "SvPowerFlow.p", "25"),
    ])

    result = post_processing.check_non_boundary_equivalent_injections(
        cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original, threshold=0.1, fix_errors=True)

    assert float(_values(result, "EquivalentInjection.p", "ei-i")[0]) == 25
    assert float(_values(result, "EquivalentInjection.p", "ei-b")[0]) == 10


def _boundary_injection_case(pl_triplets, solved_v: str, terminal_connected: str = "true"):
    original = pl_triplets([
        ("tn-b", "TopologicalNode.boundaryPoint", "true"),
        ("tb", "Type", "Terminal"), ("tb", "Terminal.ConductingEquipment", "ei"), ("tb", "Terminal.TopologicalNode", "tn-b"),
        ("tb", "ACDCTerminal.connected", terminal_connected),
        ("ei", "Type", "EquivalentInjection"), ("ei", "EquivalentInjection.p", "10"), ("ei", "EquivalentInjection.q", "5"),
        ("ov", "Type", "SvVoltage"), ("ov", "SvVoltage.TopologicalNode", "tn-b"), ("ov", "SvVoltage.v", "400"),
        ("ov", "SvVoltage.angle", "1"),
        ("opf", "Type", "SvPowerFlow"), ("opf", "SvPowerFlow.Terminal", "tb"), ("opf", "SvPowerFlow.p", "10"),
        ("opf", "SvPowerFlow.q", "5"),
    ])
    ssh = pl_triplets([
        ("tb", "ACDCTerminal.connected", terminal_connected),
        ("ei", "Type", "EquivalentInjection"), ("ei", "EquivalentInjection.p", "10"), ("ei", "EquivalentInjection.q", "5"),
    ])
    sv = pl_triplets([
        ("nv", "Type", "SvVoltage"), ("nv", "SvVoltage.TopologicalNode", "tn-b"), ("nv", "SvVoltage.v", solved_v),
        ("nv", "SvVoltage.angle", "0"),
        ("npf", "Type", "SvPowerFlow"), ("npf", "SvPowerFlow.Terminal", "tb"), ("npf", "SvPowerFlow.p", "10"),
        ("npf", "SvPowerFlow.q", "5"),
    ])
    return sv, ssh, original


@pytest.mark.parametrize("solved_v, fix_errors, expected_p", [("0", True, 0.0), ("0", False, 10.0), ("400", True, 10.0)])
def test_check_energized_boundary_nodes_zeroes_injections_on_dead_boundary_nodes(pl_triplets, solved_v, fix_errors,
                                                                                 expected_p):
    sv, ssh, original = _boundary_injection_case(pl_triplets, solved_v)

    result = post_processing.check_energized_boundary_nodes(cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original,
                                                            fix_errors=fix_errors)

    assert float(_values(result, "EquivalentInjection.p", "ei")[0]) == expected_p
    assert float(_values(result, "EquivalentInjection.q", "ei")[0]) == expected_p / 2


def test_check_for_disconnected_terminals_connects_energized_boundary_injections_and_zeroes_other_flows(pl_triplets):
    sv, ssh, original = _boundary_injection_case(pl_triplets, solved_v="400", terminal_connected="false")
    original = pl.concat([original, pl_triplets([
        ("tl", "Type", "Terminal"), ("tl", "Terminal.ConductingEquipment", "line"), ("tl", "Terminal.TopologicalNode", "tn-i"),
        ("tl", "ACDCTerminal.connected", "false"),
    ])])
    ssh = pl.concat([ssh, pl_triplets([("tl", "ACDCTerminal.connected", "false")])])
    sv = pl.concat([sv, pl_triplets([
        ("lpf", "Type", "SvPowerFlow"), ("lpf", "SvPowerFlow.Terminal", "tl"), ("lpf", "SvPowerFlow.p", "5"),
        ("lpf", "SvPowerFlow.q", "1"),
    ])])

    sv_result, ssh_result = post_processing.check_for_disconnected_terminals(
        cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original, fix_errors=True)

    assert _values(ssh_result, "ACDCTerminal.connected", "tb") == ["true"]
    assert _values(ssh_result, "ACDCTerminal.connected", "tl") == ["false"]
    assert float(_values(sv_result, "SvPowerFlow.p", "npf")[0]) == 10
    assert [float(v) for v in _values(sv_result, "SvPowerFlow.p", "lpf") + _values(sv_result, "SvPowerFlow.q", "lpf")] == [0, 0]


def test_check_non_regulating_rotating_machine_q_restores_q_without_enabled_control(pl_triplets):
    original = pl_triplets([
        ("rc-on", "RegulatingControl.enabled", "true"),
        ("rc-off", "RegulatingControl.enabled", "false"),
        ("sm1", "RotatingMachine.q", "10"), ("sm1", "RegulatingCondEq.controlEnabled", "true"),
        ("sm1", "RegulatingCondEq.RegulatingControl", "rc-on"),
        ("sm2", "RotatingMachine.q", "20"), ("sm2", "RegulatingCondEq.controlEnabled", "false"),
        ("sm2", "RegulatingCondEq.RegulatingControl", "rc-on"),
        ("sm3", "RotatingMachine.q", "30"), ("sm3", "RegulatingCondEq.controlEnabled", "true"),
        ("sm3", "RegulatingCondEq.RegulatingControl", "rc-off"),
    ])
    ssh = pl_triplets([("sm1", "RotatingMachine.q", "11"), ("sm2", "RotatingMachine.q", "21"), ("sm3", "RotatingMachine.q", "31")])

    result = post_processing.check_non_regulating_rotating_machine_q(cgm_ssh_data=ssh, original_models=original,
                                                                     fix_errors=True)

    assert {m: _values(result, "RotatingMachine.q", m)[0] for m in ("sm1", "sm2", "sm3")} == {
        "sm1": "11", "sm2": "20", "sm3": "30"}


def test_check_rotating_machine_q_outside_p_limits_restores_q_when_generation_is_outside_limits(pl_triplets):
    original = pl_triplets([
        ("gu", "GeneratingUnit.minOperatingP", "0"), ("gu", "GeneratingUnit.maxOperatingP", "100"),
        ("curve", "Type", "ReactiveCapabilityCurve"),
        ("cd1", "CurveData.Curve", "curve"), ("cd1", "CurveData.xvalue", "60"),
        ("cd2", "CurveData.Curve", "curve"), ("cd2", "CurveData.xvalue", "80"),
        # load sign convention: generating 50 MW is p = -50
        ("inside", "RotatingMachine.p", "-50"), ("inside", "RotatingMachine.q", "10"),
        ("inside", "RotatingMachine.GeneratingUnit", "gu"),
        ("above", "RotatingMachine.p", "-150"), ("above", "RotatingMachine.q", "20"),
        ("above", "RotatingMachine.GeneratingUnit", "gu"),
        ("curve-first", "RotatingMachine.p", "-50"), ("curve-first", "RotatingMachine.q", "30"),
        ("curve-first", "RotatingMachine.GeneratingUnit", "gu"),
        ("curve-first", "SynchronousMachine.InitialReactiveCapabilityCurve", "curve"),
    ])
    ssh = pl_triplets([("inside", "RotatingMachine.q", "11"), ("above", "RotatingMachine.q", "21"),
                       ("curve-first", "RotatingMachine.q", "31")])

    result = post_processing.check_rotating_machine_q_outside_p_limits(cgm_ssh_data=ssh, original_models=original,
                                                                       fix_errors=True)

    assert {m: _values(result, "RotatingMachine.q", m)[0] for m in ("inside", "above", "curve-first")} == {
        "inside": "11", "above": "20", "curve-first": "30"}


def test_check_non_ltc_tap_changer_step_restores_ssh_step_and_sv_position(pl_triplets):
    original = pl_triplets([
        ("tcc", "RegulatingControl.enabled", "true"),
        ("ltc", "TapChanger.step", "5"), ("ltc", "TapChanger.ltcFlag", "true"), ("ltc", "TapChanger.controlEnabled", "true"),
        ("ltc", "TapChanger.TapChangerControl", "tcc"),
        ("fixed", "TapChanger.step", "3"), ("fixed", "TapChanger.ltcFlag", "false"),
        ("fixed", "TapChanger.controlEnabled", "true"), ("fixed", "TapChanger.TapChangerControl", "tcc"),
    ])
    ssh = pl_triplets([("ltc", "TapChanger.step", "6"), ("fixed", "TapChanger.step", "4")])
    sv = pl_triplets([
        ("st-ltc", "SvTapStep.TapChanger", "ltc"), ("st-ltc", "SvTapStep.position", "6"),
        ("st-fixed", "SvTapStep.TapChanger", "fixed"), ("st-fixed", "SvTapStep.position", "4"),
    ])

    ssh_result, sv_result = post_processing.check_non_ltc_tap_changer_step(
        cgm_ssh_data=ssh, cgm_sv_data=sv, original_models=original, fix_errors=True)

    assert (_values(ssh_result, "TapChanger.step", "ltc"), _values(ssh_result, "TapChanger.step", "fixed")) == (["6"], ["3"])
    assert (_values(sv_result, "SvTapStep.position", "st-ltc"), _values(sv_result, "SvTapStep.position", "st-fixed")) == (
        ["6"], ["3"])


def test_check_net_interchanges_updates_control_area_interchange_to_solved_tie_flows(pl_triplets):
    original = pl_triplets([
        ("ca", "Type", "ControlArea"), ("ca", "ControlArea.netInterchange", "100"), ("ca", "ControlArea.pTolerance", "10"),
        ("ca", "IdentifiedObject.name", "BE"),
        ("tf1", "Type", "TieFlow"), ("tf1", "TieFlow.ControlArea", "ca"), ("tf1", "TieFlow.Terminal", "t1"),
        ("tf1", "TieFlow.positiveFlowIn", "true"),
        ("tf2", "Type", "TieFlow"), ("tf2", "TieFlow.ControlArea", "ca"), ("tf2", "TieFlow.Terminal", "t2"),
        ("tf2", "TieFlow.positiveFlowIn", "true"),
        ("t1", "Type", "Terminal"), ("t1", "ACDCTerminal.connected", "true"),
        ("t2", "Type", "Terminal"), ("t2", "ACDCTerminal.connected", "true"),
        ("opf1", "Type", "SvPowerFlow"), ("opf1", "SvPowerFlow.Terminal", "t1"), ("opf1", "SvPowerFlow.p", "60"),
        ("opf2", "Type", "SvPowerFlow"), ("opf2", "SvPowerFlow.Terminal", "t2"), ("opf2", "SvPowerFlow.p", "40"),
    ])
    ssh = pl_triplets([("ca", "Type", "ControlArea"), ("ca", "ControlArea.netInterchange", "100"),
                       ("ca", "ControlArea.pTolerance", "10")])
    sv = pl_triplets([
        ("npf1", "Type", "SvPowerFlow"), ("npf1", "SvPowerFlow.Terminal", "t1"), ("npf1", "SvPowerFlow.p", "70"),
        ("npf2", "Type", "SvPowerFlow"), ("npf2", "SvPowerFlow.Terminal", "t2"), ("npf2", "SvPowerFlow.p", "50"),
    ])

    result = post_processing.check_net_interchanges(cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original)

    assert float(_values(result, "ControlArea.netInterchange", "ca")[0]) == 120
    assert _values(result, "ControlArea.pTolerance", "ca") == ["10"]


# ---------------------------------------------------------------- run_post_merge_processing on MicroGrid

@pytest.fixture(scope="module")
def _microgrid_cache():
    return {}


@pytest.fixture
def solved_microgrid(_microgrid_cache, microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    """MicroGrid BE + NL solved and exported to SV like the merger does, computed once per module"""
    if not _microgrid_cache:
        input_models = [microgrid_be_igm, microgrid_nl_igm, microgrid_boundary]
        model = merge_functions.MergedModel()
        model.network = load_network_model(opdm_objects=input_models)
        model.network_meta = attr_to_dict(instance=model.network, sanitize_to_strings=True)
        model = HandlerMergeModels.apply_pre_loadflow_corrections(merged_model=model)
        with mock.patch.object(settings_manager.LoadflowSettingsManager, "_get_defaults_from_elastic",
                               side_effect=ConnectionError("Elastic is not available in unit tests")):
            model, _ = HandlerMergeModels.run_loadflow(merged_model=model)
        meta = _merged_model_meta()
        exported = merge_functions.export_merged_model(network=model.network, opdm_object_meta=meta, profiles=["SV"],
                                                       cgm_convention=False)
        _microgrid_cache.update(input_models=input_models, meta=meta, exported_name=exported.name,
                                exported=exported.getvalue(), loadflow_status=model.loadflow_status)
    return SimpleNamespace(**_microgrid_cache)


def _exported_model(solved) -> io.BytesIO:
    exported = io.BytesIO(solved.exported)
    exported.name = solved.exported_name
    return exported


def _run_post_processing(solved, additional_processing: bool = True):
    with mock.patch.object(post_processing, "check_net_interchanges",
                           side_effect=lambda cgm_sv_data, cgm_ssh_data, original_models: cgm_ssh_data):
        return post_processing.run_post_merge_processing(
            input_models=solved.input_models, exported_model=_exported_model(solved),
            opdm_object_meta=copy.deepcopy(solved.meta), additional_processing=additional_processing)


@pytest.fixture
def post_processed_microgrid(_microgrid_cache, solved_microgrid):
    if "post_processed" not in _microgrid_cache:
        _microgrid_cache["post_processed"] = _run_post_processing(solved_microgrid)
    sv_data, ssh_data, meta = _microgrid_cache["post_processed"]
    return SimpleNamespace(sv=sv_data.copy(), ssh=ssh_data.copy(), meta=copy.deepcopy(meta),
                           originals=load_opdm_objects_to_triplets(solved_microgrid.input_models))


@pytest.mark.pypowsybl
def test_export_merged_model_exports_only_sv_named_after_merged_model(solved_microgrid):
    exported = _exported_model(solved_microgrid)

    assert solved_microgrid.loadflow_status == "CONVERGED"
    assert exported.name.startswith("20140601T1030Z_1D_BALTICRCC-EU_SV_001_")
    with zipfile.ZipFile(exported) as export:
        assert export.namelist() == ["20140601T1030Z_1D_BALTICRCC-EU_SV_001_SV.xml"]
        assert f"<md:Model.modelingAuthoritySet>{MAS}</md:Model.modelingAuthoritySet>" in export.read(
            export.namelist()[0]).decode("utf-8")


@pytest.mark.pypowsybl
def test_post_processing_gives_sv_a_valid_mrid_and_merged_model_name(post_processed_microgrid):
    header = _header(post_processed_microgrid.sv)

    assert str(uuid.UUID(header["mRID"])) == header["mRID"]
    assert _values(post_processed_microgrid.sv, "label") == ["20140601T1030Z_1D_BALTICRCC-EU_SV_001.xml"]
    assert (header["Model.scenarioTime"], header["Model.version"]) == ("2014-06-01T10:30:00Z", "001")


@pytest.mark.pypowsybl
def test_post_processing_sv_depends_on_all_tp_including_boundary_and_on_updated_ssh(post_processed_microgrid):
    ssh_ids = set(post_processed_microgrid.ssh.query("KEY == 'Type' and VALUE == 'FullModel'").ID.astype(str))

    dependencies = set(_header(post_processed_microgrid.sv)["Model.DependentOn"])

    assert len(ssh_ids) == 2
    assert dependencies == MICROGRID_TP | {MICROGRID_TP_BD} | ssh_ids


@pytest.mark.pypowsybl
def test_post_processing_updated_ssh_supersede_original_ssh_with_new_mrid(post_processed_microgrid):
    ssh = post_processed_microgrid.ssh
    full_models = ssh.query("KEY == 'Type' and VALUE == 'FullModel'").ID.astype(str)

    supersedes = {_values(ssh, "Model.forEntity", fm)[0]: (fm, _values(ssh, "Model.Supersedes", fm)) for fm in full_models}

    assert {tso: superseded for tso, (_, superseded) in supersedes.items()} == {
        tso: [ssh_id] for tso, ssh_id in MICROGRID_ORIGINAL_SSH.items()}
    assert not {fm for fm, _ in supersedes.values()} & set(MICROGRID_ORIGINAL_SSH.values())
    assert set(_values(ssh, "label")) == {"20140601T1030Z_1D_BALTICRCC-EU-ELIA_SSH_001.xml",
                                          "20140601T1030Z_1D_BALTICRCC-EU-TENNET_SSH_001.xml"}
    assert set(_values(ssh, "Model.scenarioTime")) == {"2014-06-01T10:30:00Z"}


@pytest.mark.pypowsybl
def test_post_processing_updated_ssh_holds_solved_consumption(post_processed_microgrid):
    sv, ssh, originals = post_processed_microgrid.sv, post_processed_microgrid.ssh, post_processed_microgrid.originals
    terminals = originals.type_tableview("Terminal")[["Terminal.ConductingEquipment"]]
    flows = sv.type_tableview("SvPowerFlow").merge(terminals, left_on="SvPowerFlow.Terminal", right_index=True)
    solved_p = flows.set_index("Terminal.ConductingEquipment")["SvPowerFlow.p"]

    consumers = ssh.type_tableview("EnergyConsumer")["EnergyConsumer.p"]

    assert len(consumers) == 6
    assert consumers.to_dict() == pytest.approx(solved_p[consumers.index].to_dict())


@pytest.mark.pypowsybl
def test_post_processing_zeroes_paired_boundary_injections(post_processed_microgrid):
    injections = post_processed_microgrid.ssh.type_tableview("EquivalentInjection")

    # all 10 MicroGrid boundary injections are paired BE-NL
    assert len(injections) == 10
    assert (injections[["EquivalentInjection.p", "EquivalentInjection.q"]] == 0).all().all()
    assert set(injections["EquivalentInjection.regulationStatus"]) == {"false"}


@pytest.mark.pypowsybl
def test_post_processing_keeps_one_voltage_per_topological_node(post_processed_microgrid):
    originals = post_processed_microgrid.originals
    voltages = post_processed_microgrid.sv.type_tableview("SvVoltage")
    boundary_nodes = set(originals.query("KEY == 'TopologicalNode.boundaryPoint' and VALUE == 'true'").ID.astype(str))
    connected_boundary_nodes = boundary_nodes & set(_values(originals, "Terminal.TopologicalNode"))

    assert voltages["SvVoltage.TopologicalNode"].is_unique
    assert len(connected_boundary_nodes) == 5
    assert connected_boundary_nodes <= set(voltages["SvVoltage.TopologicalNode"])


@pytest.mark.pypowsybl
def test_post_processing_keeps_main_island_and_a_tap_step_for_every_tap_changer(post_processed_microgrid):
    sv, ssh = post_processed_microgrid.sv, post_processed_microgrid.ssh

    tap_changers = set(ssh.query("KEY == 'TapChanger.step'").ID.astype(str))

    assert len(_values(sv, "TopologicalIsland.TopologicalNodes")) > int(post_processing.SMALL_ISLAND_SIZE)
    assert tap_changers
    assert tap_changers <= set(_values(sv, "SvTapStep.TapChanger"))


@pytest.mark.pypowsybl
@pytest.mark.parametrize("additional_processing, fix_injection_errors, injection_threshold", [
    (True, "True", "0.1"),
    (False, "False", "0.5"),
])
def test_post_processing_steps_follow_configuration(solved_microgrid, additional_processing, fix_injection_errors,
                                                    injection_threshold):
    additional_steps = ["check_for_disconnected_terminals", "check_energized_boundary_nodes",
                        "check_non_regulating_rotating_machine_q", "check_rotating_machine_q_outside_p_limits",
                        "check_non_ltc_tap_changer_step"]
    spies = {name: mock.Mock(wraps=getattr(post_processing, name))
             for name in additional_steps + ["check_all_kind_of_injections", "check_non_boundary_equivalent_injections"]}

    with mock.patch.multiple(post_processing, **spies), \
            mock.patch.object(post_processing, "FIX_INJECTION_ERRORS", fix_injection_errors), \
            mock.patch.object(post_processing, "INJECTION_THRESHOLD", injection_threshold):
        _run_post_processing(solved_microgrid, additional_processing=additional_processing)

    for name in additional_steps:
        assert spies[name].called is additional_processing, name
        assert all(call.kwargs["fix_errors"] is True for call in spies[name].call_args_list), name
    expected = (float(injection_threshold), fix_injection_errors == "True")
    non_boundary_check = spies["check_non_boundary_equivalent_injections"]
    assert non_boundary_check.call_count == 1
    assert (non_boundary_check.call_args.kwargs["threshold"], non_boundary_check.call_args.kwargs["fix_errors"]) == expected
    # EquivalentInjection is the call delegated by check_non_boundary_equivalent_injections
    assert {call.kwargs["injection_name"]: (call.kwargs["threshold"], call.kwargs["fix_errors"])
            for call in spies["check_all_kind_of_injections"].call_args_list} == {
        "EnergySource": expected, "ExternalNetworkInjection": expected, "EquivalentInjection": expected}


@pytest.mark.pypowsybl
@pytest.mark.xfail(strict=True, raises=AttributeError,
                   reason="check_net_interchanges calls .rename on type_tableview() == None when models have no "
                          "ControlArea, the caller only catches KeyError/ColumnNotFoundError")
def test_post_processing_skips_net_interchange_check_for_models_without_control_area(solved_microgrid):
    sv_data, ssh_data, _ = post_processing.run_post_merge_processing(
        input_models=solved_microgrid.input_models, exported_model=_exported_model(solved_microgrid),
        opdm_object_meta=copy.deepcopy(solved_microgrid.meta), additional_processing=True)

    assert not sv_data.empty and not ssh_data.empty
