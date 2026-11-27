import copy
import io
import logging
import zipfile

import pandas as pd
import pytest
from lxml import etree

from emf.common.helpers.opdm_objects import (clean_data_from_opdm_objects, clean_profile_data_from_opdm_objects,
                                             create_opdm_objects, filename_from_opdm_metadata,
                                             generate_opdm_object_content_reference_from_filename,
                                             get_metadata_from_file_name, get_opdm_data_from_models,
                                             get_opdm_metadata_from_rdfxml, load_opdm_objects_to_triplets)

FULL_MODEL = b"""<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">
  <md:FullModel rdf:about="urn:uuid:11111111-2222-3333-4444-555555555555">
    <md:Model.scenarioTime>2025-07-06T09:30:00Z</md:Model.scenarioTime>
    <md:Model.created>2025-07-05T12:00:00Z</md:Model.created>
    <md:Model.description>AST day ahead</md:Model.description>
    <md:Model.version>3</md:Model.version>
    <md:Model.DependentOn rdf:resource="urn:uuid:eq-model"/>
    <md:Model.DependentOn rdf:resource="urn:uuid:tp-model"/>
    <md:Model.profile>http://entsoe.eu/CIM/StateVariables/4/1</md:Model.profile>
    <md:Model.modelingAuthoritySet>http://www.ast.lv/OperationalPlanning</md:Model.modelingAuthoritySet>
    <md:Model.processType>unmapped</md:Model.processType>
  </md:FullModel>
</rdf:RDF>"""


@pytest.mark.parametrize("file_name, expected", [
    ("20250706T0930Z_1D_AST_SSH_001.xml", {
        "file_type": "xml", "pmd:validFrom": "20250706T0930Z", "pmd:timeHorizon": "1D", "pmd:cgmesProfile": "SSH",
        "pmd:versionNumber": "001", "pmd:modelPartReference": "AST", "pmd:TSO": "AST", "pmd:sourcingActor": "AST"}),
    ("20250706T0000Z_ENTSOE_EQBD_002.zip", {
        "file_type": "zip", "pmd:validFrom": "20250706T0000Z", "pmd:timeHorizon": "", "pmd:cgmesProfile": "EQBD",
        "pmd:versionNumber": "002", "pmd:modelPartReference": "ENTSOE", "pmd:TSO": "ENTSOE",
        "pmd:sourcingActor": "ENTSOE"}),
    ("20250706T0930Z__ENTSOE_TPBD_001.zip", {
        "file_type": "zip", "pmd:validFrom": "20250706T0930Z", "pmd:timeHorizon": "", "pmd:cgmesProfile": "TPBD",
        "pmd:versionNumber": "001", "pmd:modelPartReference": "ENTSOE", "pmd:TSO": "ENTSOE",
        "pmd:sourcingActor": "ENTSOE"}),
    ("20250706T0930Z_1D_BALTICRSC-BA_SV_004.zip", {
        "file_type": "zip", "pmd:validFrom": "20250706T0930Z", "pmd:timeHorizon": "1D", "pmd:cgmesProfile": "SV",
        "pmd:versionNumber": "004", "pmd:mergingEntity": "BALTICRSC", "pmd:mergingArea": "BA"}),
    ("20250706T0930Z_WK_BALTICRSC-BA-LITGRID_SSH_001.xml", {
        "file_type": "xml", "pmd:validFrom": "20250706T0930Z", "pmd:timeHorizon": "WK", "pmd:cgmesProfile": "SSH",
        "pmd:versionNumber": "001", "pmd:mergingEntity": "BALTICRSC", "pmd:mergingArea": "BA",
        "pmd:modelPartReference": "LITGRID", "pmd:TSO": "LITGRID", "pmd:sourcingActor": "LITGRID"}),
])
def test_metadata_from_file_name(file_name, expected):
    assert get_metadata_from_file_name(file_name) == expected


def test_metadata_from_file_name_with_custom_separator():
    metadata = get_metadata_from_file_name("20250706T0930Z;1D;AST;SSH;001.xml", meta_separator=";")

    assert metadata["pmd:TSO"] == "AST"
    assert metadata["pmd:cgmesProfile"] == "SSH"


@pytest.mark.parametrize("file_name", ["20250706T0930Z_AST_SSH.xml", "20250706T0930Z_1D_AST_SSH_001_extra.xml"])
def test_file_name_with_unexpected_part_count_returns_partial_metadata(file_name, caplog):
    with caplog.at_level(logging.WARNING):
        metadata = get_metadata_from_file_name(file_name)

    assert metadata == {"file_type": "xml"}
    assert "Parsing error" in caplog.text


def test_model_authority_with_too_many_parts_has_no_authority_metadata(caplog):
    metadata = get_metadata_from_file_name("20250706T0930Z_1D_A-B-C-D_SSH_001.xml")

    assert metadata["pmd:cgmesProfile"] == "SSH"
    assert not {"pmd:TSO", "pmd:mergingEntity", "pmd:modelPartReference"} & metadata.keys()
    assert "Parsing error A-B-C-D" in caplog.text


def test_file_name_without_extension_is_rejected():
    with pytest.raises(ValueError):
        get_metadata_from_file_name("20250706T0930Z_1D_AST_SSH_001")


def test_file_name_must_be_a_string():
    with pytest.raises(AssertionError):
        get_metadata_from_file_name(None)


@pytest.mark.parametrize("file_name, content_reference", [
    ("20250706T0930Z_1D_AST_SSH_001.zip", "CGMES/1D/AST/20250706/093000/SSH/20250706T0930Z_1D_AST_SSH_001.zip"),
    ("20250706T2330Z_2D_LITGRID_SV_002.xml", "CGMES/2D/LITGRID/20250706/233000/SV/20250706T2330Z_2D_LITGRID_SV_002.xml"),
])
def test_content_reference_from_file_name(file_name, content_reference):
    assert generate_opdm_object_content_reference_from_filename(file_name) == content_reference


def test_content_reference_uses_given_object_type():
    reference = generate_opdm_object_content_reference_from_filename("20250706T0930Z_1D_AST_SSH_001.zip", "IGM")

    assert reference.startswith("IGM/1D/AST/")


def test_cgm_file_name_from_metadata():
    metadata = {"pmd:validFrom": "20250706T0930Z", "pmd:timeHorizon": "1D", "pmd:mergingEntity": "BALTICRSC",
                "pmd:Area": "BA", "pmd:cgmesProfile": "SV", "pmd:versionNumber": "003"}

    assert filename_from_opdm_metadata(metadata) == "20250706T0930Z_1D_BALTICRSC-BA_SV_003"
    assert filename_from_opdm_metadata(metadata, file_type="zip") == "20250706T0930Z_1D_BALTICRSC-BA_SV_003.zip"


def test_cgm_file_name_round_trips_through_parser():
    metadata = get_metadata_from_file_name("20250706T0930Z_1D_BALTICRSC-BA_SV_003.zip")
    # the parser names the area pmd:mergingArea, CGM metadata uses pmd:Area
    metadata["pmd:Area"] = metadata.pop("pmd:mergingArea")

    assert filename_from_opdm_metadata(metadata, file_type=metadata["file_type"]) == "20250706T0930Z_1D_BALTICRSC-BA_SV_003.zip"


def test_clean_data_removes_all_profile_data(microgrid_be_igm, microgrid_nl_igm):
    opdm_objects = [microgrid_be_igm, microgrid_nl_igm]

    result = clean_data_from_opdm_objects(opdm_objects)

    assert result is opdm_objects
    assert all(component["opdm:Profile"]["DATA"] is None
               for opdm_object in result for component in opdm_object["opde:Component"])


def test_clean_profile_data_keeps_other_profiles(microgrid_be_igm):
    original = copy.deepcopy(microgrid_be_igm)

    clean_profile_data_from_opdm_objects([microgrid_be_igm], {"SSH", "SV"})

    for component, original_component in zip(microgrid_be_igm["opde:Component"], original["opde:Component"]):
        profile = component["opdm:Profile"]
        if profile["pmd:cgmesProfile"] in {"SSH", "SV"}:
            assert profile["DATA"] is None
        else:
            assert profile["DATA"] == original_component["opdm:Profile"]["DATA"]


def test_opdm_metadata_from_rdfxml_maps_header_to_opdm_keys():
    metadata = get_opdm_metadata_from_rdfxml(etree.parse(io.BytesIO(FULL_MODEL)))

    model_id = "11111111-2222-3333-4444-555555555555"
    assert metadata == {
        "opde:Id": model_id, "pmd:modelid": model_id, "pmd:fullModel_ID": model_id,
        "pmd:scenarioDate": "2025-07-06T09:30:00Z",
        "pmd:creationDate": "2025-07-05T12:00:00Z",
        "pmd:description": "AST day ahead",
        "pmd:version": "3",
        "opde:DependsOn": ["urn:uuid:eq-model", "urn:uuid:tp-model"],
        "pmd:modelProfile": "http://entsoe.eu/CIM/StateVariables/4/1",
        "pmd:modelingAuthoritySet": "http://www.ast.lv/OperationalPlanning",
    }


def profile_files(opdm_object):
    files = []
    for component in opdm_object["opde:Component"]:
        file = io.BytesIO(component["opdm:Profile"]["DATA"])
        file.name = component["opdm:Profile"]["pmd:fileName"]
        files.append(file)
    return files


def test_create_opdm_objects_builds_igm_with_key_profile_metadata(microgrid_be_igm):
    opdm_object, = create_opdm_objects([profile_files(microgrid_be_igm)])

    assert opdm_object["opde:Object-Type"] == "IGM"
    assert opdm_object["pmd:cgmesProfile"] == "SV"
    assert opdm_object["pmd:TSO"] == "ELIA"
    assert opdm_object["pmd:fileName"] == "20140601T1030Z_1D_ELIA_SV_001.zip"
    assert opdm_object["pmd:content-reference"] == "CGMES/1D/ELIA/20140601/103000/SV/20140601T1030Z_1D_ELIA_SV_001.zip"
    assert opdm_object["pmd:fullModel_ID"] == microgrid_be_igm["pmd:fullModel_ID"]
    assert opdm_object["pmd:modelProfile"] == "http://entsoe.eu/CIM/StateVariables/4/1"
    assert "DATA" not in opdm_object
    profiles = {component["opdm:Profile"]["pmd:cgmesProfile"]: component["opdm:Profile"]
                for component in opdm_object["opde:Component"]}
    assert profiles.keys() == {"EQ", "SSH", "TP", "SV"}
    assert profiles["EQ"]["pmd:modelProfile"] == ["http://entsoe.eu/CIM/EquipmentCore/3/1",
                                                  "http://entsoe.eu/CIM/EquipmentShortCircuit/3/1"]
    original = {component["opdm:Profile"]["pmd:cgmesProfile"]: component["opdm:Profile"]["DATA"]
                for component in microgrid_be_igm["opde:Component"]}
    assert {profile: data["DATA"] for profile, data in profiles.items()} == original


def test_create_opdm_objects_skips_non_igm_profiles(microgrid_be_igm, microgrid_boundary):
    opdm_object, = create_opdm_objects([profile_files(microgrid_be_igm) + profile_files(microgrid_boundary)])

    assert sorted(component["opdm:Profile"]["pmd:cgmesProfile"] for component in opdm_object["opde:Component"]) == \
        ["EQ", "SSH", "SV", "TP"]


def test_create_opdm_objects_with_other_key_profile_and_metadata_override(microgrid_be_igm, microgrid_nl_igm):
    objects = create_opdm_objects([profile_files(microgrid_be_igm), profile_files(microgrid_nl_igm)], key_profile="SSH",
                                  metadata={"data-source": "PDN", "pmd:versionNumber": "009"})

    assert [opdm_object["pmd:TSO"] for opdm_object in objects] == ["ELIA", "TENNET"]
    assert all(opdm_object["pmd:cgmesProfile"] == "SSH" for opdm_object in objects)
    assert all(opdm_object["data-source"] == "PDN" and opdm_object["pmd:versionNumber"] == "009"
               for opdm_object in objects)


def test_load_triplets_of_one_profile(microgrid_be_igm):
    data = load_opdm_objects_to_triplets([microgrid_be_igm], profile="SSH")

    assert data.query("KEY == 'label'").VALUE.tolist() == ["20140601T1030Z_1D_ELIA_SSH_001.xml"]
    assert data.query("KEY == 'Model.profile'").VALUE.str.contains("SteadyStateHypothesis").all()


def test_load_triplets_of_all_profiles(microgrid_be_igm, microgrid_boundary):
    data = load_opdm_objects_to_triplets([microgrid_be_igm, microgrid_boundary])

    assert data.query("KEY == 'Type' and VALUE == 'FullModel'").shape[0] == 6
    assert data.INSTANCE_ID.nunique() == 6


def test_opdm_data_from_models_parses_opdm_objects_but_passes_triplets_through(microgrid_be_igm, make_triplets):
    triplets = make_triplets([("a", "Type", "Terminal")])

    assert get_opdm_data_from_models(triplets) is triplets
    parsed = get_opdm_data_from_models([microgrid_be_igm])
    assert isinstance(parsed, pd.DataFrame)
    assert "Terminal" in set(parsed.query("KEY == 'Type'").VALUE)
