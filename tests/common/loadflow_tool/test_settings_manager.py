import json
from unittest import mock

import pypowsybl as pp
import pytest

from emf.common.loadflow_tool import loadflow_settings, settings_manager
from emf.common.loadflow_tool.settings_manager import LoadflowSettingsManager


@pytest.fixture(autouse=True)
def _no_override_from_environment(monkeypatch):
    monkeypatch.delenv("LOADFLOW_CONFIG_OVERRIDE_PATH", raising=False)


@pytest.fixture
def elastic_down():
    with mock.patch.object(settings_manager, "Elasticsearch", side_effect=ConnectionError("elastic down")) as client:
        yield client


def elastic_returning(source):
    client = mock.MagicMock()
    client.return_value.get.return_value.raw = {"_source": source}
    return mock.patch.object(settings_manager, "Elasticsearch", client)


def write_json(path, content):
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


@pytest.mark.parametrize("keyword", ["EU_DEFAULT", "BA_DEFAULT", "IGM_VALIDATION"])
def test_repository_settings_are_used_when_elastic_is_unavailable(elastic_down, keyword):
    default = getattr(loadflow_settings, keyword)

    manager = LoadflowSettingsManager(settings_keyword=keyword)

    assert manager.config["LF_PROVIDER"] == default.provider_parameters
    assert manager.config["LF_PARAMETERS"]["balance_type"] == default.balance_type
    assert manager.config["LF_PARAMETERS"]["read_slack_bus"] == default.read_slack_bus
    assert "provider_parameters" not in manager.config["LF_PARAMETERS"]


def test_changing_manager_settings_does_not_change_repository_defaults(elastic_down):
    manager = LoadflowSettingsManager(settings_keyword="BA_DEFAULT")

    manager.set("LF_PROVIDER.maxNewtonRaphsonIterations", "999")

    assert loadflow_settings.BA_DEFAULT.provider_parameters["maxNewtonRaphsonIterations"] == "50"


def test_settings_are_read_from_elastic_by_keyword():
    source = {"LF_PROVIDER": {"maxNewtonRaphsonIterations": "7"}, "LF_PARAMETERS": {"read_slack_bus": False}}
    with elastic_returning(source) as client:
        manager = LoadflowSettingsManager(elastic_server="http://elastic.test:9200", elastic_api_key="key",
                                          elastic_index="lf-index", settings_keyword="BA_RELAXED_1")

    client.assert_called_once_with("http://elastic.test:9200", api_key="key")
    client.return_value.get.assert_called_once_with(index="lf-index", id="BA_RELAXED_1")
    assert manager.config == source


def test_override_file_is_deep_merged_into_base_settings(elastic_down, tmp_path):
    override = write_json(tmp_path / "override.json", {
        "LF_PROVIDER": {"maxNewtonRaphsonIterations": "99", "slackBusCountryFilter": "LT"},
        "LF_PARAMETERS": {"write_slack_bus": True},
    })

    manager = LoadflowSettingsManager(settings_keyword="BA_DEFAULT", override_path=str(override))

    assert manager.config["LF_PROVIDER"]["maxNewtonRaphsonIterations"] == "99"
    assert manager.config["LF_PROVIDER"]["slackBusCountryFilter"] == "LT"
    assert manager.config["LF_PROVIDER"]["slackBusSelectionMode"] == "LARGEST_GENERATOR"
    assert manager.config["LF_PARAMETERS"]["write_slack_bus"] is True
    assert manager.config["LF_PARAMETERS"]["balance_type"] == loadflow_settings.BA_DEFAULT.balance_type


def test_override_path_is_taken_from_environment(elastic_down, tmp_path, monkeypatch):
    override = write_json(tmp_path / "override.json", {"LF_PROVIDER": {"maxOuterLoopIterations": "5"}})
    monkeypatch.setenv("LOADFLOW_CONFIG_OVERRIDE_PATH", str(override))

    manager = LoadflowSettingsManager(settings_keyword="EU_DEFAULT")

    assert manager.config["LF_PROVIDER"]["maxOuterLoopIterations"] == "5"


def test_missing_override_file_raises(elastic_down, tmp_path):
    with pytest.raises(FileNotFoundError, match="Override config not found"):
        LoadflowSettingsManager(override_path=str(tmp_path / "missing.json"))


def test_override_must_be_a_mapping(elastic_down, tmp_path):
    override = write_json(tmp_path / "override.json", ["not", "a", "mapping"])

    with pytest.raises(ValueError, match="mapping"):
        LoadflowSettingsManager(override_path=str(override))


def test_yaml_override_without_pyyaml_raises(elastic_down, tmp_path):
    override = tmp_path / "override.yaml"
    override.write_text("LF_PROVIDER:\n  maxOuterLoopIterations: '5'\n", encoding="utf-8")

    with mock.patch.object(settings_manager, "yaml", None), pytest.raises(RuntimeError, match="PyYAML"):
        LoadflowSettingsManager(override_path=str(override))


@pytest.mark.parametrize("key, value, expected", [
    ("voltage_init_mode", "DC_VALUES", pp.loadflow.VoltageInitMode.DC_VALUES),
    ("voltage_init_mode", "VoltageInitMode.PREVIOUS_VALUES", pp.loadflow.VoltageInitMode.PREVIOUS_VALUES),
    ("voltage_init_mode", "uniform_values", pp.loadflow.VoltageInitMode.UNIFORM_VALUES),
    ("balance_type", "BalanceType.PROPORTIONAL_TO_CONFORM_LOAD", pp.loadflow.BalanceType.PROPORTIONAL_TO_CONFORM_LOAD),
    ("connected_component_mode", " main ", pp.loadflow.ConnectedComponentMode.MAIN),
    ("voltage_init_mode", "NOT_AN_ENUM", "NOT_AN_ENUM"),
    ("voltage_init_mode", pp.loadflow.VoltageInitMode.DC_VALUES, pp.loadflow.VoltageInitMode.DC_VALUES),
])
def test_enum_strings_are_resolved_to_pypowsybl_enums(elastic_down, key, value, expected):
    manager = LoadflowSettingsManager()
    parameters = {key: value, "read_slack_bus": "True"}

    resolved = manager._resolve_enums(parameters)

    assert resolved == {key: expected, "read_slack_bus": "True"}
    assert parameters[key] is value


@pytest.mark.parametrize("keyword", ["EU_DEFAULT", "BA_RELAXED_2"])
def test_built_parameters_match_repository_settings(elastic_down, keyword):
    default = getattr(loadflow_settings, keyword)

    built = LoadflowSettingsManager(settings_keyword=keyword).build_pypowsybl_parameters()

    assert repr(built) == repr(default)


@pytest.mark.pypowsybl
def test_built_parameters_solve_ieee14(elastic_down):
    parameters = LoadflowSettingsManager(settings_keyword="BA_DEFAULT").build_pypowsybl_parameters()

    results = pp.loadflow.run_ac(pp.network.create_ieee14(), parameters)

    assert results[0].status == pp.loadflow.ComponentStatus.CONVERGED


def test_json_export_contains_merged_settings(elastic_down):
    manager = LoadflowSettingsManager(settings_keyword="EU_RELAXED")
    manager.set("LF_PROVIDER.maxOuterLoopIterations", "3")

    exported = manager.to_bytesio("json")

    assert exported.name == "loadflow-settings.json"
    content = json.loads(exported.getvalue())
    assert content["LF_PROVIDER"] == manager.config["LF_PROVIDER"]
    assert content["LF_PARAMETERS"]["countries_to_balance"] == []


def test_export_rejects_unknown_format(elastic_down):
    with pytest.raises(ValueError, match="fmt"):
        LoadflowSettingsManager().to_bytesio("xml")


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="pypowsybl enums are not enum.Enum, so _to_plain ignores enum_repr and exports str()")
@pytest.mark.parametrize("enum_repr, expected", [("name", "UNIFORM_VALUES"), ("value", 0)])
def test_export_represents_enums_as_requested(elastic_down, enum_repr, expected):
    exported = LoadflowSettingsManager(settings_keyword="EU_DEFAULT").export_config(enum_repr=enum_repr)

    assert exported["LF_PARAMETERS"]["voltage_init_mode"] == expected


@pytest.mark.pypowsybl
@pytest.mark.xfail(strict=True, raises=TypeError,
                   reason="_resolve_enums maps connected_component_mode, but the exported field is component_mode, "
                          "so exported settings stay unusable strings")
def test_exported_settings_can_be_loaded_and_solved(elastic_down):
    exported = json.loads(LoadflowSettingsManager(settings_keyword="EU_DEFAULT").to_bytesio("json").getvalue())
    with elastic_returning(exported):
        manager = LoadflowSettingsManager(settings_keyword="EU_DEFAULT")

    results = pp.loadflow.run_ac(pp.network.create_ieee14(), manager.build_pypowsybl_parameters())

    assert results[0].status == pp.loadflow.ComponentStatus.CONVERGED
