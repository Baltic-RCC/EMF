import io
import zipfile

import pandas as pd
import pytest
from lxml import etree

from emf.common.helpers.cgmes import export_to_cgmes_zip, get_metadata_from_rdfxml
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.helpers.utils import get_xml_from_zip

HEADER = b"""<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:md="http://iec.ch/TC57/61970-552/ModelDescription/1#">
  <md:FullModel rdf:about="urn:uuid:5cf1c8e1-5a3b-4a7e-9c38-9b9f6bf6c1f4">
    <md:Model.scenarioTime>2025-07-06T09:30:00Z</md:Model.scenarioTime>
    <md:Model.version>2</md:Model.version>
    <md:Model.DependentOn rdf:resource="urn:uuid:eq"/>
    <md:Model.DependentOn rdf:resource="urn:uuid:tp"/>
    <md:Model.DependentOn rdf:resource="urn:uuid:ssh"/>
    <md:Model.Supersedes rdf:resource="urn:uuid:previous"/>
    <md:Model.profile>http://entsoe.eu/CIM/StateVariables/4/1</md:Model.profile>
    <md:Model.profile>http://entsoe.eu/CIM/Extension/1/1</md:Model.profile>
  </md:FullModel>
</rdf:RDF>"""

HELPER_OBJECTS = ("Distribution", "NamespaceMap")


def parse(xml: bytes):
    return etree.parse(io.BytesIO(xml))


def model_objects(data):
    """(ID, KEY, VALUE) rows without the Distribution/NamespaceMap objects triplets adds per parsed file"""
    helper_ids = set(data.query("KEY == 'Type' and VALUE in @HELPER_OBJECTS").ID)
    data = data[~data.ID.isin(helper_ids)]
    return set(map(tuple, data[["ID", "KEY", "VALUE"]].astype(str).values.tolist()))


def test_header_metadata_uses_full_model_about_as_mrid():
    metadata = get_metadata_from_rdfxml(parse(HEADER))

    assert metadata["Model.mRID"] == "5cf1c8e1-5a3b-4a7e-9c38-9b9f6bf6c1f4"
    assert metadata["Model.scenarioTime"] == "2025-07-06T09:30:00Z"
    assert metadata["Model.version"] == "2"


def test_repeated_header_keys_become_lists_and_resources_are_read():
    metadata = get_metadata_from_rdfxml(parse(HEADER))

    assert metadata["Model.DependentOn"] == ["urn:uuid:eq", "urn:uuid:tp", "urn:uuid:ssh"]
    assert metadata["Model.Supersedes"] == "urn:uuid:previous"
    assert metadata["Model.profile"] == ["http://entsoe.eu/CIM/StateVariables/4/1", "http://entsoe.eu/CIM/Extension/1/1"]


def test_header_metadata_requires_parsed_xml():
    with pytest.raises(AssertionError):
        get_metadata_from_rdfxml(HEADER)


def profile_file(opdm_object, profile):
    component = next(component["opdm:Profile"] for component in opdm_object["opde:Component"]
                     if component["opdm:Profile"]["pmd:cgmesProfile"] == profile)
    return io.BytesIO(component["DATA"])


def test_header_metadata_matches_triplets_of_same_file(microgrid_be_igm):
    metadata = get_metadata_from_rdfxml(get_xml_from_zip(profile_file(microgrid_be_igm, "SV")))

    data = load_opdm_objects_to_triplets([microgrid_be_igm], profile="SV")
    assert metadata["Model.mRID"] == data.query("KEY == 'Type' and VALUE == 'FullModel'").ID.item()
    assert metadata["Model.DependentOn"] == data.query("KEY == 'Model.DependentOn'").VALUE.map(
        lambda value: f"urn:uuid:{value}").tolist()


@pytest.fixture
def ssh_tp_sv(microgrid_be_igm):
    return [load_opdm_objects_to_triplets([microgrid_be_igm], profile=profile) for profile in ("SSH", "TP", "SV")]


def test_exported_zips_contain_the_same_objects(ssh_tp_sv):
    exported = {file.name: file for file in export_to_cgmes_zip(ssh_tp_sv)}

    assert sorted(exported) == ["20140601T1030Z_1D_ELIA_SSH_001.zip", "20140601T1030Z_1D_ELIA_SV_001.zip",
                                "20140601T1030Z_1D_ELIA_TP_001.zip"]
    for original, profile in zip(ssh_tp_sv, ("SSH", "TP", "SV")):
        file = exported[f"20140601T1030Z_1D_ELIA_{profile}_001.zip"]
        assert model_objects(pd.read_RDF([file])) == model_objects(original)


def test_export_writes_one_cgmes_2_4_15_xml_per_zip(ssh_tp_sv):
    for file in export_to_cgmes_zip(ssh_tp_sv):
        with zipfile.ZipFile(file) as archive:
            xml_name, = archive.namelist()
            xml = archive.read(xml_name)
        assert xml_name == file.name.replace(".zip", ".xml")
        assert b"http://iec.ch/TC57/2013/CIM-schema-cim16#" in xml


def test_export_skips_classes_outside_cgmes_schema(ssh_tp_sv):
    ssh = ssh_tp_sv[0]
    unknown = pd.DataFrame([("x1", "Type", "NotACimClass"), ("x1", "NotACimClass.value", "1")],
                           columns=["ID", "KEY", "VALUE"]).assign(INSTANCE_ID=ssh.INSTANCE_ID.iloc[0])

    exported_ssh, = export_to_cgmes_zip([ssh, unknown])

    assert "x1" not in set(pd.read_RDF([exported_ssh]).ID)


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="triplets read_RDF keeps XML entities escaped and the export escapes them again, "
                          "so values with & ' < > change on every load/export cycle")
def test_export_keeps_values_with_xml_special_characters(microgrid_be_igm):
    source = get_metadata_from_rdfxml(get_xml_from_zip(profile_file(microgrid_be_igm, "EQ")))
    assert "'" in source["Model.description"]

    exported, = export_to_cgmes_zip([load_opdm_objects_to_triplets([microgrid_be_igm], profile="EQ")])

    assert get_metadata_from_rdfxml(get_xml_from_zip(exported))["Model.description"] == source["Model.description"]
