import json
import logging
from pathlib import Path

import pytest

import config
from emf.common.config_parser import parse_app_properties

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def properties_file(tmp_path, monkeypatch):
    """Writes a .properties file and clears env variables of the same (upper-cased) names"""
    def _properties_file(text: str):
        path = tmp_path / "test.properties"
        path.write_text(text)
        for line in text.splitlines():
            if "=" in line:
                monkeypatch.delenv(line.split("=", 1)[0].strip().upper(), raising=False)
        return path
    return _properties_file


def test_keys_are_upper_cased(properties_file):
    path = properties_file("[MAIN]\nemf_test_queue = object-storage.models\nEMF_TEST_Mixed_Case = 1\n")
    settings = {}

    parse_app_properties(settings, str(path))

    assert settings == {"EMF_TEST_QUEUE": "object-storage.models", "EMF_TEST_MIXED_CASE": "1"}


def test_environment_value_overrides_file_value(properties_file, monkeypatch, caplog):
    path = properties_file("[MAIN]\nEMF_TEST_SERVER = file-server\nEMF_TEST_PORT = 5672\n")
    monkeypatch.setenv("EMF_TEST_SERVER", "env-server")
    settings = {}

    with caplog.at_level(logging.INFO, logger="emf.common.config_parser"):
        parse_app_properties(settings, str(path))

    assert settings == {"EMF_TEST_SERVER": "env-server", "EMF_TEST_PORT": "5672"}
    assert "[ENVIRONMENT] EMF_TEST_SERVER = env-server" in caplog.messages
    assert "[PROPERTIES] EMF_TEST_PORT = 5672" in caplog.messages


def test_lower_case_key_is_overridden_by_upper_case_environment_variable(properties_file, monkeypatch):
    path = properties_file("[MAIN]\nemf_test_server = file-server\n")
    monkeypatch.setenv("EMF_TEST_SERVER", "env-server")
    settings = {}

    parse_app_properties(settings, str(path))

    assert settings["EMF_TEST_SERVER"] == "env-server"


@pytest.mark.parametrize("sanitize_mask", ["****", "<hidden>"])
def test_password_values_are_masked_in_log_but_kept_in_globals(properties_file, monkeypatch, caplog, sanitize_mask):
    path = properties_file("[MAIN]\nEMF_TEST_USERNAME = operator\nEMF_TEST_PASSWORD = file-secret\nEMF_TEST_DB_PASSWORD_HASH = abc\n")
    monkeypatch.setenv("EMF_TEST_PASSWORD", "env-secret")
    settings = {}

    with caplog.at_level(logging.INFO, logger="emf.common.config_parser"):
        parse_app_properties(settings, str(path), sanitize_mask=sanitize_mask)

    assert settings["EMF_TEST_PASSWORD"] == "env-secret"
    assert settings["EMF_TEST_DB_PASSWORD_HASH"] == "abc"
    log_text = caplog.text
    assert "env-secret" not in log_text and "file-secret" not in log_text and "= abc" not in log_text
    assert f"[ENVIRONMENT] EMF_TEST_PASSWORD = {sanitize_mask}" in caplog.messages
    assert "[PROPERTIES] EMF_TEST_USERNAME = operator" in caplog.messages
    logged_values = {record.parameter_name: record.parameter_value for record in caplog.records}
    assert logged_values == {"EMF_TEST_USERNAME": "operator",
                             "EMF_TEST_PASSWORD": sanitize_mask,
                             "EMF_TEST_DB_PASSWORD_HASH": sanitize_mask}


def test_values_stay_strings_by_default(properties_file):
    path = properties_file("[MAIN]\nEMF_TEST_FLAG = False\nEMF_TEST_COUNT = 3\n")
    settings = {}

    parse_app_properties(settings, str(path))

    assert settings == {"EMF_TEST_FLAG": "False", "EMF_TEST_COUNT": "3"}


@pytest.mark.parametrize("raw_value, expected", [
    ("5", 5),
    ("1.5", 1.5),
    ("True", True),
    ("None", None),
    ("['AST', 'PSE']", ["AST", "PSE"]),
    ("{'a': 1}", {"a": 1}),
    ("emf-tasks", "emf-tasks"),
    ("abc", "abc"),
])
def test_eval_types_converts_literals_and_keeps_other_strings(properties_file, raw_value, expected):
    path = properties_file(f"[MAIN]\nEMF_TEST_VALUE = {raw_value}\n")
    settings = {}

    parse_app_properties(settings, str(path), eval_types=True)

    assert settings["EMF_TEST_VALUE"] == expected


@pytest.mark.xfail(strict=True, raises=SyntaxError,
                   reason="eval_types only catches ValueError, ast.literal_eval raises SyntaxError for values like URLs")
@pytest.mark.parametrize("raw_value", ["http://localhost:9200", "two words", "*"])
def test_eval_types_keeps_strings_that_are_not_python_syntax(properties_file, raw_value):
    path = properties_file(f"[MAIN]\nEMF_TEST_VALUE = {raw_value}\n")
    settings = {}

    parse_app_properties(settings, str(path), eval_types=True)

    assert settings["EMF_TEST_VALUE"] == raw_value


def test_eval_types_converts_environment_values(properties_file, monkeypatch):
    path = properties_file("[MAIN]\nEMF_TEST_COUNT = 1\n")
    monkeypatch.setenv("EMF_TEST_COUNT", "7")
    settings = {}

    parse_app_properties(settings, str(path), eval_types=True)

    assert settings["EMF_TEST_COUNT"] == 7


def test_custom_section_is_parsed_instead_of_main(properties_file):
    path = properties_file("[MAIN]\nEMF_TEST_MAIN_KEY = main\n\n[CUSTOM]\nEMF_TEST_CUSTOM_KEY = custom\n")
    settings = {}

    parse_app_properties(settings, str(path), section="CUSTOM")

    assert settings == {"EMF_TEST_CUSTOM_KEY": "custom"}


def test_blank_value_is_empty_string(properties_file):
    path = properties_file("[MAIN]\nEMF_TEST_BLANK =\n")
    settings = {}

    parse_app_properties(settings, str(path))

    assert settings == {"EMF_TEST_BLANK": ""}


@pytest.mark.parametrize("folder, stem, relative_path", [
    ("task_generator", "task_generator", "config/task_generator/task_generator.properties"),
    ("task_generator", "process_conf", "config/task_generator/process_conf.json"),
    ("task_generator", "timeframe_conf", "config/task_generator/timeframe_conf.json"),
    ("integrations", "elastic", "config/integrations/elastic.properties"),
    ("cgm_worker", "config_areas_mapping", "config/cgm_worker/config_areas_mapping.json"),
    ("logging", "custom_logger", "config/logging/custom_logger.properties"),
    ("report_publisher", "test_results2", "config/report_publisher/test_results2.xml"),
])
def test_config_paths_point_at_real_files(folder, stem, relative_path):
    path = getattr(getattr(config.paths, folder), stem)

    assert path == (REPO_ROOT / relative_path).resolve()
    assert path.is_file()


def test_config_paths_skip_dunder_files_and_folders():
    assert not hasattr(config.paths, "__pycache__")
    # config/__init__.py is the only file directly in config/, so no "config" group is created
    assert not hasattr(config.paths, "config")


def test_config_paths_can_be_passed_to_json_load():
    # config/__init__.py adds Path.read so the discovered paths behave like open files for json.load
    process_config = json.load(config.paths.task_generator.process_conf)

    assert {process["@id"].rsplit("/", 1)[-1] for process in process_config} == {"CGM_CREATION", "RMM_CREATION"}


@pytest.mark.parametrize("properties_path", sorted((REPO_ROOT / "config").rglob("*.properties")),
                         ids=lambda path: f"{path.parent.name}/{path.name}")
def test_every_properties_file_has_a_main_section(properties_path):
    settings = {}

    parse_app_properties(settings, str(properties_path))

    assert settings
