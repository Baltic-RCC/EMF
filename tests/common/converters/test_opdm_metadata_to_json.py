import json

import pytest

from emf.common.converters import opdm_metadata_to_json

PROFILES = ("EQ", "SSH", "TP", "SV")


def component(profile: str) -> str:
    file_name = f"20250102T0930Z_1D_AST_{profile}_001.zip"
    return f"""
      <opde:Component>
        <opdm:Profile>
          <pmd:cgmesProfile>{profile}</pmd:cgmesProfile>
          <pmd:fileName>{file_name}</pmd:fileName>
          <pmd:content-reference>CGMES/1D/AST/20250102/093000/{profile}/{file_name}</pmd:content-reference>
        </opdm:Profile>
      </opde:Component>"""


def publish_message(*profiles: str) -> str:
    """OPDM publication notification, the OPDM object is in the second sm:part"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<sm:Publish xmlns:sm="http://entsoe.eu/opde/ServiceModel/1/0"
            xmlns:opde="http://entsoe.eu/opde/ObjectModel/1/0"
            xmlns:opdm="http://entsoe.eu/opdm/ObjectModel/1/0"
            xmlns:pmd="http://entsoe.eu/opdm/ProfileMetaData/1/0">
  <sm:part name="publicationId">publication-1</sm:part>
  <sm:part name="publication">
    <opdm:OPDMObject>
      <opde:Id>object-1</opde:Id>
      <opde:Object-Type>IGM</opde:Object-Type>
      <pmd:TSO>AST</pmd:TSO>
      <pmd:timeHorizon>1D</pmd:timeHorizon>
      <pmd:scenarioDate>2025-01-02T09:30:00Z</pmd:scenarioDate>
      <pmd:versionNumber>001</pmd:versionNumber>{''.join(component(profile) for profile in profiles)}
    </opdm:OPDMObject>
  </sm:part>
</sm:Publish>"""


@pytest.mark.parametrize("encode", [True, False], ids=["bytes", "str"])
def test_convert_returns_list_with_the_published_opdm_object(encode):
    message = publish_message(*PROFILES)

    content, content_type = opdm_metadata_to_json.convert(message.encode() if encode else message)

    assert content_type == "application/json"
    assert json.loads(content) == [{
        "opde:Id": "object-1",
        "opde:Object-Type": "IGM",
        "pmd:TSO": "AST",
        "pmd:timeHorizon": "1D",
        "pmd:scenarioDate": "2025-01-02T09:30:00Z",
        "pmd:versionNumber": "001",
        "opde:Component": [{"opdm:Profile": {
            "pmd:cgmesProfile": profile,
            "pmd:fileName": f"20250102T0930Z_1D_AST_{profile}_001.zip",
            "pmd:content-reference": f"CGMES/1D/AST/20250102/093000/{profile}/20250102T0930Z_1D_AST_{profile}_001.zip",
        }} for profile in PROFILES],
    }]


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="xmltodict turns a single opde:Component into a dict, consumers iterate it as a list of profiles")
def test_convert_keeps_single_component_as_list():
    content, _ = opdm_metadata_to_json.convert(publish_message("EQ"))

    components = json.loads(content)[0]["opde:Component"]

    assert isinstance(components, list)
    assert [c["opdm:Profile"]["pmd:cgmesProfile"] for c in components] == ["EQ"]
