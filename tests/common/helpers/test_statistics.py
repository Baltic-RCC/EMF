import pandas as pd
import pytest

from emf.common.helpers.statistics import (get_load_and_generation_ssh, get_system_metrics, get_tieflow_data,
                                           sum_on_KEY, type_tableview_merge)


def cim_object(object_id, object_type, attributes):
    return [(object_id, "Type", object_type)] + [(object_id, key, str(value)) for key, value in attributes.items()]


def tie_line(name, from_iso, to_iso, injection_p, flow_p, description="AC line", area="ca_LT", node_breaker=True):
    """One boundary point with the TSO line end (TieFlow terminal), its boundary EquivalentInjection and SV results"""
    node_type = "ConnectivityNode" if node_breaker else "TopologicalNode"
    node = f"{name}_node"
    topological_node = f"{name}_tn" if node_breaker else node
    rows = cim_object(node, node_type, {
        "IdentifiedObject.name": name,
        "IdentifiedObject.description": description,
        f"{node_type}.boundaryPoint": "true",
        f"{node_type}.fromEndIsoCode": from_iso,
        f"{node_type}.toEndIsoCode": to_iso,
        f"{node_type}.ConnectivityNodeContainer": f"{name}_container",
    })
    rows += cim_object(f"{name}_container", "Line", {"IdentifiedObject.energyIdentCodeEic": f"10T-{name}"})
    rows += cim_object(f"{name}_acl", "ACLineSegment", {"IdentifiedObject.name": f"{name} line"})
    rows += cim_object(f"{name}_t_line", "Terminal", {"IdentifiedObject.name": f"{name} line end",
                                                      "Terminal.ConductingEquipment": f"{name}_acl",
                                                      f"Terminal.{node_type}": node,
                                                      "Terminal.TopologicalNode": topological_node})
    rows += cim_object(f"{name}_ei", "EquivalentInjection", {"IdentifiedObject.name": f"{name} injection",
                                                             "EquivalentInjection.p": injection_p,
                                                             "EquivalentInjection.q": 0})
    rows += cim_object(f"{name}_t_ei", "Terminal", {"IdentifiedObject.name": f"{name} injection end",
                                                    "Terminal.ConductingEquipment": f"{name}_ei",
                                                    f"Terminal.{node_type}": node,
                                                    "Terminal.TopologicalNode": topological_node})
    rows += cim_object(f"{name}_tf", "TieFlow", {"TieFlow.ControlArea": area, "TieFlow.Terminal": f"{name}_t_line"})
    rows += cim_object(f"{name}_sv", "SvPowerFlow", {"SvPowerFlow.Terminal": f"{name}_t_line",
                                                     "SvPowerFlow.p": flow_p, "SvPowerFlow.q": 0})
    rows += cim_object(f"{name}_svv", "SvVoltage", {"SvVoltage.TopologicalNode": topological_node,
                                                      "SvVoltage.v": 330})
    return rows


def control_area(area_id="ca_LT", name="LT", area_type="ControlAreaTypeKind.Interchange"):
    return cim_object(area_id, "ControlArea", {"IdentifiedObject.name": name, "ControlArea.type": area_type,
                                               "IdentifiedObject.energyIdentCodeEic": f"10Y{name}"})


@pytest.fixture
def baltic_model(make_triplets):
    rows = control_area()
    rows += tie_line("XLT_LV1", "LV", "LT", injection_p=-100, flow_p=99.5)
    rows += tie_line("XLT_SE1", "LT", "SE", injection_p=480, flow_p=-479, description="HVDC NordBalt")
    rows += cim_object("load", "EnergyConsumer", {"EnergyConsumer.p": 500, "EnergyConsumer.q": 100})
    rows += cim_object("gen", "SynchronousMachine", {"RotatingMachine.p": -900, "RotatingMachine.q": -50})
    return make_triplets(rows)


@pytest.mark.parametrize("precision, expected", [(1, 3.3), (2, 3.33), (0, 3.0)])
def test_sum_on_key_sums_string_values_and_rounds(make_triplets, precision, expected):
    data = make_triplets([("a", "EnergyConsumer.p", "1.111"), ("b", "EnergyConsumer.p", "2.222"),
                          ("c", "RotatingMachine.p", "50")])
    assert sum_on_KEY(data, "EnergyConsumer.p", precision=precision) == expected


def test_load_and_generation_are_summed_per_key_and_missing_keys_are_zero(make_triplets):
    data = make_triplets([("l1", "EnergyConsumer.p", "10.04"), ("l2", "EnergyConsumer.p", "5"),
                          ("l1", "EnergyConsumer.q", "2"), ("g1", "RotatingMachine.p", "-20.5"),
                          ("g1", "SvPowerFlow.p", "1000")])

    assert get_load_and_generation_ssh(data) == {"EnergyConsumer.p": 15.0, "EnergyConsumer.q": 2.0,
                                                 "RotatingMachine.p": -20.5, "RotatingMachine.q": 0.0}


def test_type_tableview_merge_follows_reference_to_next_type(make_triplets):
    data = make_triplets(
        cim_object("t1", "Terminal", {"Terminal.ConnectivityNode": "cn1", "IdentifiedObject.name": "T1"})
        + cim_object("t2", "Terminal", {"Terminal.ConnectivityNode": "missing", "IdentifiedObject.name": "T2"})
        + cim_object("cn1", "ConnectivityNode", {"IdentifiedObject.name": "N1"}))

    merged = type_tableview_merge(data, "Terminal->ConnectivityNode")

    assert merged[["ID_Terminal", "ID_ConnectivityNode", "IdentifiedObject.name_Terminal",
                   "IdentifiedObject.name_ConnectivityNode"]].values.tolist() == [["t1", "cn1", "T1", "N1"]]


def test_type_tableview_merge_reverse_arrow_joins_referencing_type(make_triplets):
    data = make_triplets(cim_object("ca", "ControlArea", {"IdentifiedObject.name": "LT"})
                         + cim_object("tf1", "TieFlow", {"TieFlow.ControlArea": "ca"})
                         + cim_object("tf2", "TieFlow", {"TieFlow.ControlArea": "ca"}))

    merged = type_tableview_merge(data, "ControlArea<-TieFlow")

    assert sorted(merged["ID_TieFlow"]) == ["tf1", "tf2"]
    assert set(merged["ID_ControlArea"]) == {"ca"}


def test_type_tableview_merge_uses_explicit_reference_attribute(make_triplets):
    data = make_triplets(cim_object("ei", "EquivalentInjection", {"EquivalentInjection.p": 5})
                         + cim_object("t_ei", "Terminal", {"Terminal.ConductingEquipment": "ei"})
                         + cim_object("t_other", "Terminal", {"Terminal.ConductingEquipment": "other"}))

    merged = type_tableview_merge(data, "EquivalentInjection<-Terminal.ConductingEquipment")

    assert merged[["ID_Terminal", "ID_EquivalentInjection", "EquivalentInjection.p"]].values.tolist() == [["t_ei", "ei", 5]]


def test_tieflow_data_combines_injection_line_and_sv_results(baltic_model):
    tieflow_data = get_tieflow_data(baltic_model).set_index("IdentifiedObject.name")

    assert tieflow_data.loc["XLT_LV1", "cross_border"] == "LT-LV"
    assert tieflow_data.loc["XLT_SE1", "cross_border"] == "LT-SE"
    assert tieflow_data.loc["XLT_LV1", "EquivalentInjection.p"] == -100
    assert tieflow_data.loc["XLT_LV1", "SvPowerFlow.p"] == 99.5
    assert tieflow_data.loc["XLT_SE1", "IdentifiedObject.energyIdentCodeEic_Line"] == "10T-XLT_SE1"
    assert not tieflow_data.loc["XLT_LV1", "BoundaryPoint.isDirectCurrent"]
    assert tieflow_data.loc["XLT_SE1", "BoundaryPoint.isDirectCurrent"]


def test_tieflow_data_falls_back_to_topological_nodes_for_bus_branch_models(make_triplets):
    data = make_triplets(control_area() + tie_line("XLT_LV1", "LV", "LT", injection_p=-100, flow_p=99.5,
                                                   node_breaker=False))

    tieflow_data = get_tieflow_data(data)

    assert tieflow_data["cross_border"].tolist() == ["LT-LV"]
    assert tieflow_data["EquivalentInjection.p"].tolist() == [-100]


def test_system_metrics_losses_and_net_position(baltic_model):
    metrics = get_system_metrics(baltic_model)

    # load sign convention: load + generation + net position (export > 0) = -losses
    assert metrics["total_load"] == 500
    assert metrics["generation"] == -900
    assert metrics["tieflow_np"]["EquivalentInjection_p"] == 380
    assert metrics["losses"] == pytest.approx(20)
    assert metrics["losses_coefficient"] == pytest.approx(20 / (500 + 900 + 580))
    assert metrics["tieflow_abs"]["EquivalentInjection_p"] == 580


def test_system_metrics_ac_net_position_excludes_hvdc(baltic_model):
    metrics = get_system_metrics(baltic_model)

    assert metrics["tieflow_acnp"]["EquivalentInjection_p"] == -100
    assert metrics["tieflow_hvdc"] == [{"eic": "10T-XLT_SE1", "EquivalentInjection_p": 480, "EquivalentInjection_q": 0,
                                        "SvPowerFlow_p": -479, "SvPowerFlow_q": 0}]


def test_system_metrics_only_count_interchange_control_areas(make_triplets):
    rows = control_area() + control_area("ca_other", "LT_AGC", "ControlAreaTypeKind.AGC")
    rows += tie_line("XLT_LV1", "LV", "LT", injection_p=-100, flow_p=99.5)
    rows += tie_line("XLT_LV2", "LV", "LT", injection_p=-40, flow_p=40, area="ca_other")
    rows += cim_object("load", "EnergyConsumer", {"EnergyConsumer.p": 100})

    metrics = get_system_metrics(make_triplets(rows))

    assert metrics["tieflow_np"]["EquivalentInjection_p"] == -100


def test_system_metrics_use_given_tieflow_data(baltic_model):
    tieflow_data = pd.DataFrame({"EquivalentInjection.p": [10.0], "EquivalentInjection.q": [0.0],
                                 "SvPowerFlow.p": [-10.0], "SvPowerFlow.q": [0.0],
                                 "BoundaryPoint.isDirectCurrent": [False]})

    metrics = get_system_metrics(baltic_model, tieflow_data=tieflow_data)

    assert metrics["tieflow_np"]["EquivalentInjection_p"] == 10
    assert metrics["losses"] == pytest.approx(-(500 - 900 + 10))
    assert metrics["tieflow_hvdc"] == []
