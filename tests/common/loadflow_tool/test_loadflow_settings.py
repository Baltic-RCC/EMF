import pypowsybl as pp
import pytest

from emf.common.helpers.loadflow import load_network_model
from emf.common.loadflow_tool import loadflow_settings

SETTINGS = ["OPENLOADFLOW_DEFAULT", "IGM_VALIDATION", "EU_DEFAULT", "EU_RELAXED", "BA_DEFAULT", "BA_RELAXED_1",
            "BA_RELAXED_2"]

# 'reactivePowerRemoteControl' was renamed 'generatorReactivePowerRemoteControl' in OpenLoadFlow, unknown keys are ignored
STALE_PROVIDER_KEY = pytest.mark.xfail(strict=True, raises=AssertionError,
                                       reason="provider parameter 'reactivePowerRemoteControl' does not exist in "
                                              "OpenLoadFlow and is silently ignored")


@pytest.mark.parametrize("settings_name", [
    "OPENLOADFLOW_DEFAULT",
    pytest.param("IGM_VALIDATION", marks=STALE_PROVIDER_KEY),
    "EU_DEFAULT",
    pytest.param("EU_RELAXED", marks=STALE_PROVIDER_KEY),
    "BA_DEFAULT",
    "BA_RELAXED_1",
    "BA_RELAXED_2",
])
def test_provider_parameters_exist_in_openloadflow(settings_name):
    known = set(pp.loadflow.get_provider_parameters_names())

    unknown = set(getattr(loadflow_settings, settings_name).provider_parameters) - known

    assert not unknown


@pytest.mark.parametrize("settings_name", SETTINGS)
def test_provider_parameter_values_are_strings(settings_name):
    provider_parameters = getattr(loadflow_settings, settings_name).provider_parameters

    assert all(isinstance(value, str) for value in provider_parameters.values())


@pytest.mark.parametrize("settings_name", ["EU_DEFAULT", "EU_RELAXED", "BA_DEFAULT", "BA_RELAXED_1",
                                           "BA_RELAXED_2"])
def test_slack_mismatch_meets_qocdc_sv_injection_limit(settings_name):
    # QoCDC SV_INJECTION_LIMIT = 0.1 MW
    provider_parameters = getattr(loadflow_settings, settings_name).provider_parameters

    assert float(provider_parameters["slackBusPMaxMismatch"]) <= 0.1


@pytest.mark.pypowsybl
@pytest.mark.parametrize("settings_name", SETTINGS)
def test_settings_converge_on_ieee14(settings_name):
    network = pp.network.create_ieee14()

    results = pp.loadflow.run_ac(network, getattr(loadflow_settings, settings_name))

    assert results[0].status == pp.loadflow.ComponentStatus.CONVERGED


@pytest.mark.pypowsybl
@pytest.mark.parametrize("settings_name", SETTINGS)
def test_settings_converge_on_merged_microgrid(settings_name, microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    network = load_network_model([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])

    results = pp.loadflow.run_ac(network, getattr(loadflow_settings, settings_name))

    assert [result.status for result in results] == [pp.loadflow.ComponentStatus.CONVERGED]
