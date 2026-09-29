import socket

import pytest

from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets


def test_network_is_blocked():
    with pytest.raises(RuntimeError, match="Network access blocked"):
        socket.create_connection(("localhost", 9200), timeout=1)


@pytest.mark.pypowsybl
def test_ieee14_igm_loads_and_solves(ieee14_igm):
    import pypowsybl as pp

    assert {c["opdm:Profile"]["pmd:cgmesProfile"] for c in ieee14_igm["opde:Component"]} == {"EQ", "SSH", "TP", "SV"}
    assert not load_opdm_objects_to_triplets([ieee14_igm]).empty

    network = load_network_model([ieee14_igm])
    assert len(network.get_buses()) == 14
    assert pp.loadflow.run_ac(network)[0].status_text == "Converged"


@pytest.mark.pypowsybl
def test_microgrid_loads_with_boundary_and_solves(microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    import pypowsybl as pp

    data = load_opdm_objects_to_triplets([microgrid_be_igm, microgrid_boundary])
    assert "Terminal" in set(data.query("KEY == 'Type'").VALUE)

    network = load_network_model([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])
    assert pp.loadflow.run_ac(network)[0].status_text == "Converged"


@pytest.mark.pypowsybl
def test_igm_fixtures_are_independent(ieee14_igm, igm_factory):
    other = igm_factory("create_ieee14")
    ieee14_igm["opde:Component"][0]["opdm:Profile"]["DATA"] = None
    assert other["opde:Component"][0]["opdm:Profile"]["DATA"] is not None


def test_merge_task_example(merge_task):
    assert merge_task["@type"] == "Task"
    assert {"merge_type", "time_horizon", "timestamp_utc"} <= merge_task["task_properties"].keys()
