import math
import uuid
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile

import pytest

from emf.common.helpers.utils import (attr_to_dict, convert_dict_str_to_bool, filter_and_flatten_dict, flatten_dict,
                                      get_xml_from_zip, is_valid_uuid, sanitize_nan, zip_xml)
import config
from emf.common.config_parser import parse_app_properties


def test_flatten_dict_joins_nested_keys_and_indexes_lists():
    nested = {
        "@id": "urn:uuid:1",
        "task_properties": {"merge_type": "EU", "included": ["AST", "PSE"], "nested": {"deep": 1}},
        "task_status_trace": [{"status": "created", "timestamp": "t0"}, {"status": "started", "timestamp": "t1"}],
        "task_tags": [],
    }

    assert flatten_dict(nested) == {
        "@id": "urn:uuid:1",
        "task_properties.merge_type": "EU",
        "task_properties.included[0]": "AST",
        "task_properties.included[1]": "PSE",
        "task_properties.nested.deep": 1,
        "task_status_trace[0].status": "created",
        "task_status_trace[0].timestamp": "t0",
        "task_status_trace[1].status": "started",
        "task_status_trace[1].timestamp": "t1",
    }


def test_flatten_dict_uses_custom_separator():
    assert flatten_dict({"a": {"b": {"c": 1}}}, separator="/") == {"a/b/c": 1}


def test_filter_and_flatten_dict_builds_rabbit_headers_from_task_header_keys(merge_task):
    # TASK_HEADER_KEYS ends with a comma, so the key list contains "" which must simply be skipped
    settings = {}
    parse_app_properties(settings, config.paths.task_generator.task_generator)
    header_keys = settings["TASK_HEADER_KEYS"].split(",")

    headers = filter_and_flatten_dict(merge_task, header_keys)

    assert headers == {
        "@id": merge_task["@id"],
        "job_id": merge_task["job_id"],
        "run_id": "https://example.com/runs/IntraDayCGM/1",
        "process_id": "https://example.com/processes/CGM_CREATION",
        "@type": "Task",
        "task_properties.merge_type": "EU",
        "task_properties.time_horizon": "ID",
    }


def test_convert_dict_str_to_bool_converts_nested_boolean_strings_in_place():
    data = {
        "a": "True", "b": "true", "c": "TRUE", "d": "False", "e": "false", "f": "FALSE",
        "version": "001", "merge_type": "EU", "flag": True, "count": 0,
        "task_properties": {"replacement": "True", "scaling": "false", "inner": {"debug": "false"}},
    }

    result = convert_dict_str_to_bool(data)

    assert result is data
    assert data == {
        "a": True, "b": True, "c": True, "d": False, "e": False, "f": False,
        "version": "001", "merge_type": "EU", "flag": True, "count": 0,
        "task_properties": {"replacement": True, "scaling": False, "inner": {"debug": False}},
    }


@pytest.mark.parametrize("value", ["yes", "0", "1", "tRue", ["True"]])
def test_convert_dict_str_to_bool_leaves_other_values_untouched(value):
    assert convert_dict_str_to_bool({"key": value}) == {"key": value}


def test_sanitize_nan_replaces_nan_in_nested_structures():
    data = {"a": float("nan"), "b": [1.5, float("nan"), {"c": math.nan}], "d": "nan", "e": None, "f": 0.0}

    assert sanitize_nan(data) == {"a": None, "b": [1.5, None, {"c": None}], "d": "nan", "e": None, "f": 0.0}


def test_sanitize_nan_returns_scalars():
    assert sanitize_nan(float("nan")) is None
    assert sanitize_nan(3) == 3


def test_zip_xml_and_get_xml_from_zip_round_trip():
    xml = b'<?xml version="1.0" encoding="UTF-8"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><item>1</item></rdf:RDF>'
    xml_file = BytesIO(xml)
    xml_file.name = "20250610T0930Z_1D_AST_SSH_001.xml"

    zipped = zip_xml(xml_file)

    assert zipped.name == "20250610T0930Z_1D_AST_SSH_001.zip"
    with ZipFile(zipped) as archive:
        assert archive.namelist() == ["20250610T0930Z_1D_AST_SSH_001.xml"]
        assert archive.read("20250610T0930Z_1D_AST_SSH_001.xml") == xml
    tree = get_xml_from_zip(zipped)
    assert tree.getroot().tag == "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF"
    assert tree.getroot()[0].text == "1"


def test_get_xml_from_zip_reads_zip_file_from_path(tmp_path):
    path = tmp_path / "model.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("model.xml", "<root><child/></root>")

    assert get_xml_from_zip(str(path)).getroot()[0].tag == "child"


@pytest.mark.parametrize("value, expected", [
    ("84969ee9-16e4-492b-bbd3-928000e5c8c2", True),
    ("84969EE9-16E4-492B-BBD3-928000E5C8C2", True),
    ("84969ee916e4492bbbd3928000e5c8c2", True),
    ("urn:uuid:84969ee9-16e4-492b-bbd3-928000e5c8c2", True),
    (uuid.UUID("84969ee9-16e4-492b-bbd3-928000e5c8c2"), True),
    ("84969ee9-16e4-492b-bbd3-928000e5c8c", False),
    ("not-a-uuid", False),
    ("", False),
    (None, False),
    (12345, False),
])
def test_is_valid_uuid(value, expected):
    assert is_valid_uuid(value) is expected


class _Island:
    def __init__(self):
        self.status = "CONVERGED"
        self.iterations = 4
        self.reference_bus = _Bus()
        self.buses = [_Bus(), 7]
        self.metadata = {"computed": datetime(2025, 6, 10, 12, 30)}
        self._private = "hidden"

    @property
    def mismatch(self):
        return 0.5

    def method(self):
        return "not an attribute"


class _Bus:
    def __init__(self):
        self.id = "bus-1"
        self.voltage = 1.02


def test_attr_to_dict_collects_public_attributes_recursively():
    assert attr_to_dict(_Island()) == {
        "status": "CONVERGED",
        "iterations": 4,
        "mismatch": 0.5,
        "reference_bus": {"id": "bus-1", "voltage": 1.02},
        "buses": [{"id": "bus-1", "voltage": 1.02}, 7],
        "metadata": {"computed": datetime(2025, 6, 10, 12, 30)},
    }


def test_attr_to_dict_sanitizes_values_to_strings():
    assert attr_to_dict(_Island(), sanitize_to_strings=True) == {
        "status": "CONVERGED",
        "iterations": "4",
        "mismatch": "0.5",
        "reference_bus": {"id": "bus-1", "voltage": "1.02"},
        "buses": [{"id": "bus-1", "voltage": "1.02"}, "7"],
        "metadata": {"computed": "2025-06-10T12:30:00"},
    }
