from unittest import mock

import pandas as pd
import polars as pl
import pytest

from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.model_validator import validator_functions
from emf.model_validator.validator_functions import (
    check_not_retained_switches_between_nodes,
    get_ac_net_position,
    get_nodes_against_kirchhoff_first_law,
    get_sum_of_loads,
    modify_region_name_for_denmark,
)


def terminal(terminal_id, node, equipment, node_key="Terminal.TopologicalNode"):
    return [(terminal_id, "Type", "Terminal"), (terminal_id, node_key, node),
            (terminal_id, "Terminal.ConductingEquipment", equipment)]


def power_flow(flow_id, terminal_id, p, q=0.0):
    return [(flow_id, "Type", "SvPowerFlow"), (flow_id, "SvPowerFlow.Terminal", terminal_id),
            (flow_id, "SvPowerFlow.p", str(p)), (flow_id, "SvPowerFlow.q", str(q))]


def sv_injection(injection_id, node, p, q=0.0):
    return [(injection_id, "Type", "SvInjection"), (injection_id, "SvInjection.TopologicalNode", node),
            (injection_id, "SvInjection.pInjection", str(p)), (injection_id, "SvInjection.qInjection", str(q))]


def node_with_flows(node, flows):
    rows = []
    for number, (p, q) in enumerate(flows):
        rows += terminal(f"{node}_t{number}", node, f"{node}_eq{number}")
        rows += power_flow(f"{node}_pf{number}", f"{node}_t{number}", p, q)
    return rows


# ---------------------------------------------------------------- Kirchhoff's first law

@pytest.fixture
def triplets_input_accepted():
    """get_nodes_against_kirchhoff_first_law parses any input as OPDM objects (see the xfail test), let dummy triplets through"""
    with mock.patch.object(validator_functions, "load_opdm_objects_to_triplets", side_effect=lambda opdm_objects: opdm_objects):
        yield


def test_kirchhoff_accepts_triplets_as_documented(make_triplets):
    data = make_triplets(node_with_flows("tn1", [(10, 0), (-9, 0)]))

    violated = get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True)

    assert violated["Terminal.TopologicalNode"].tolist() == ["tn1"]


@pytest.mark.parametrize("flows, violated", [
    ([(10.0, 5.0), (-10.0, -5.0)], False),
    ([(10.0, 0.0), (-9.95, 0.0)], False),
    ([(0.1, 0.0)], False),
    ([(0.0, -0.1)], False),
    ([(0.11, 0.0)], True),
    ([(0.0, -0.11)], True),
    ([(10.0, 3.0), (-10.0, -2.5)], True),
    ([(-10.0, 0.0), (9.5, 0.0)], True),
])
def test_kirchhoff_default_limit_is_qocdc_sv_injection_limit(make_triplets, triplets_input_accepted, flows, violated):
    data = make_triplets(node_with_flows("tn1", flows))

    result = get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True)

    assert result["Terminal.TopologicalNode"].tolist() == (["tn1"] if violated else [])


def test_kirchhoff_custom_limit(make_triplets, triplets_input_accepted):
    data = make_triplets(node_with_flows("tn1", [(10.0, 0.0), (-9.7, 0.0)]))

    assert get_nodes_against_kirchhoff_first_law(original_models=data, sv_injection_limit=0.5, nodes_only=True).empty
    assert not get_nodes_against_kirchhoff_first_law(original_models=data, sv_injection_limit=0.2, nodes_only=True).empty


def test_kirchhoff_nodes_only_returns_unique_violated_nodes(make_triplets, triplets_input_accepted):
    data = make_triplets(node_with_flows("bad", [(10, 0), (-5, 0), (-3, 0)]) + node_with_flows("good", [(1, 1), (-1, -1)]))

    result = get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True)

    assert list(result.columns) == ["Terminal.TopologicalNode"]
    assert result["Terminal.TopologicalNode"].tolist() == ["bad"]


def test_kirchhoff_terminal_rows_list_every_terminal_of_violated_nodes_with_node_sums(make_triplets, triplets_input_accepted):
    data = make_triplets(node_with_flows("bad", [(10, 1), (-5, 0), (-3, 0)]) + node_with_flows("good", [(1, 1), (-1, -1)]))

    result = get_nodes_against_kirchhoff_first_law(original_models=data)

    assert set(result["Terminal"]) == {"bad_t0", "bad_t1", "bad_t2"}
    assert set(result["Terminal.TopologicalNode"]) == {"bad"}
    assert result["SvPowerFlow.p"].tolist() == pytest.approx([2.0] * 3)
    assert result["SvPowerFlow.q"].tolist() == pytest.approx([1.0] * 3)


def test_kirchhoff_sv_injection_compensates_mismatch_only_when_considered(make_triplets, triplets_input_accepted):
    data = make_triplets(node_with_flows("tn1", [(10, 2), (-9, 0)]) + sv_injection("inj1", "tn1", -1, -2))

    assert not get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True).empty
    assert get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True, consider_sv_injection=True).empty


def test_kirchhoff_sv_injection_too_small_still_violates(make_triplets, triplets_input_accepted):
    data = make_triplets(node_with_flows("tn1", [(10, 0), (-9, 0)]) + sv_injection("inj1", "tn1", -0.5))

    result = get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=True, consider_sv_injection=True)

    assert result["Terminal.TopologicalNode"].tolist() == ["tn1"]


def test_kirchhoff_flows_from_separate_sv_data(make_triplets, triplets_input_accepted):
    models = make_triplets(terminal("t1", "tn1", "eq1") + terminal("t2", "tn1", "eq2")
                           + power_flow("pf1", "t1", 10) + power_flow("pf2", "t2", -10))
    cgm_sv = make_triplets(power_flow("pf1", "t1", 10) + power_flow("pf2", "t2", -8))

    assert get_nodes_against_kirchhoff_first_law(original_models=models, nodes_only=True).empty
    result = get_nodes_against_kirchhoff_first_law(original_models=models, cgm_sv_data=cgm_sv, nodes_only=True)
    assert result["Terminal.TopologicalNode"].tolist() == ["tn1"]


@pytest.mark.parametrize("nodes_only", [False, True])
def test_kirchhoff_without_terminals_returns_empty_frame(make_triplets, triplets_input_accepted, nodes_only):
    data = make_triplets(power_flow("pf1", "t1", 10))

    result = get_nodes_against_kirchhoff_first_law(original_models=data, nodes_only=nodes_only)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.pypowsybl
def test_kirchhoff_on_solved_ieee14_flags_only_the_node_of_a_changed_flow(ieee14_igm):
    assert get_nodes_against_kirchhoff_first_law(original_models=[ieee14_igm]).empty

    data = load_opdm_objects_to_triplets([ieee14_igm])
    sv = load_opdm_objects_to_triplets([ieee14_igm], profile="SV")
    flow_id, terminal_id = sv.query("KEY == 'SvPowerFlow.Terminal'")[["ID", "VALUE"]].iloc[0]
    node = data.query("ID == @terminal_id and KEY == 'Terminal.TopologicalNode'").VALUE.item()
    p = sv.query("ID == @flow_id and KEY == 'SvPowerFlow.p'").index
    sv.loc[p, "VALUE"] = str(float(sv.loc[p, "VALUE"].item()) + 1.0)

    result = get_nodes_against_kirchhoff_first_law(original_models=[ieee14_igm], cgm_sv_data=sv, nodes_only=True)

    assert result["Terminal.TopologicalNode"].tolist() == [node]


# ---------------------------------------------------------------- non-retained switches

def switch(switch_id, retained, is_open, node_1, node_2):
    return ([(switch_id, "Type", "Breaker"), (switch_id, "Switch.retained", retained), (switch_id, "Switch.open", is_open)]
            + terminal(f"{switch_id}_t1", node_1, switch_id) + terminal(f"{switch_id}_t2", node_2, switch_id))


def switch_states(data):
    if isinstance(data, pl.DataFrame):
        data = data.to_pandas()
    return dict(data.query("KEY == 'Switch.open'")[["ID", "VALUE"]].itertuples(index=False))


@pytest.fixture
def switches(make_triplets):
    return make_triplets(
        switch("violating", "false", "false", "tn1", "tn2")
        + switch("retained", "true", "false", "tn1", "tn2")
        + switch("already_open", "false", "true", "tn1", "tn2")
        + switch("same_node", "false", "false", "tn1", "tn1")
    )


def test_non_retained_closed_switch_between_nodes_is_counted_but_not_opened_by_default(switches):
    data, violated = check_not_retained_switches_between_nodes(switches)

    assert violated == 1
    assert switch_states(data) == {"violating": "false", "retained": "false", "already_open": "true", "same_node": "false"}


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_non_retained_switch_between_nodes_is_opened_and_type_kept(switches, engine):
    original = switches if engine == "pandas" else pl.from_pandas(switches)

    data, violated = check_not_retained_switches_between_nodes(original, open_not_retained_switches=True)

    assert violated == 1
    assert type(data) is type(original)
    assert len(data) == len(original)
    assert switch_states(data) == {"violating": "true", "retained": "false", "already_open": "true", "same_node": "false"}


@pytest.mark.parametrize("rows", [
    switch("retained", "true", "false", "tn1", "tn2"),
    switch("already_open", "false", "true", "tn1", "tn2"),
    switch("same_node", "false", "false", "tn1", "tn1"),
    [("no_terminals", "Type", "Breaker"), ("no_terminals", "Switch.retained", "false"), ("no_terminals", "Switch.open", "false")],
], ids=["retained", "open", "same_node", "no_terminals"])
def test_switches_that_are_not_violating_are_ignored(make_triplets, rows):
    original = make_triplets(rows)

    data, violated = check_not_retained_switches_between_nodes(original, open_not_retained_switches=True)

    assert violated == 0
    pd.testing.assert_frame_equal(data, original)


# ---------------------------------------------------------------- AC net position

def control_area(area_id, area_type="ControlAreaTypeKind.Interchange"):
    return [(area_id, "Type", "ControlArea"), (area_id, "ControlArea.type", area_type)]


def tie_point(name, p, equipment_type="ACLineSegment", node_description="Tie line node", area="ca",
              node_key="Terminal.ConnectivityNode"):
    """Boundary node with the TSO's tie equipment (the TieFlow terminal) and the EquivalentInjection of the neighbour"""
    node, equipment, injection = f"{name}_node", f"{name}_equipment", f"{name}_injection"
    return ([(node, "Type", "ConnectivityNode"), (node, "IdentifiedObject.description", node_description),
             (equipment, "Type", equipment_type),
             (injection, "Type", "EquivalentInjection"), (injection, "EquivalentInjection.p", str(p)),
             (f"{name}_tieflow", "Type", "TieFlow"), (f"{name}_tieflow", "TieFlow.ControlArea", area),
             (f"{name}_tieflow", "TieFlow.Terminal", f"{name}_equipment_terminal")]
            + terminal(f"{name}_equipment_terminal", node, equipment, node_key)
            + terminal(f"{name}_injection_terminal", node, injection, node_key))


@pytest.mark.parametrize("node_key", ["Terminal.ConnectivityNode", "Terminal.TopologicalNode"])
@pytest.mark.parametrize("area_type", ["ControlAreaTypeKind.Interchange",
                                       "http://iec.ch/TC57/2013/CIM-schema-cim16#ControlAreaTypeKind.Interchange"])
def test_ac_net_position_sums_injections_at_interchange_tie_points_rounded(make_triplets, node_key, area_type):
    data = make_triplets(control_area("ca", area_type)
                         + tie_point("a", 100.123, node_key=node_key)
                         + tie_point("b", -40.001, node_key=node_key))

    assert get_ac_net_position(data) == 60.12


def test_ac_net_position_ignores_tie_flows_of_other_control_area_types(make_triplets):
    data = make_triplets(control_area("ca") + control_area("forecast", "ControlAreaTypeKind.Forecast")
                         + tie_point("a", 100) + tie_point("b", 55, area="forecast"))

    assert get_ac_net_position(data) == 100.0


@pytest.mark.parametrize("equipment_type", ["DCLineSegment", "ACDCConverter", "CsConverter", "VsConverter"])
def test_ac_net_position_excludes_dc_equipment_tie_points(make_triplets, equipment_type):
    data = make_triplets(control_area("ca") + tie_point("ac", 100) + tie_point("dc", 500, equipment_type=equipment_type))

    assert get_ac_net_position(data) == 100.0


def test_ac_net_position_excludes_boundary_nodes_described_as_hvdc(make_triplets):
    data = make_triplets(control_area("ca") + tie_point("ac", 100) + tie_point("hvdc", 500, node_description="HVDC Estlink"))

    assert get_ac_net_position(data) == 100.0


def test_ac_net_position_is_zero_without_ac_injections(make_triplets):
    data = make_triplets(control_area("ca") + tie_point("dc", 500, equipment_type="DCLineSegment"))

    assert get_ac_net_position(data) == 0.0


@pytest.mark.parametrize("missing_type", ["ControlArea", "TieFlow", "Terminal", "EquivalentInjection"])
def test_ac_net_position_is_none_when_required_class_is_missing(make_triplets, missing_type):
    data = make_triplets(control_area("ca") + tie_point("a", 100))
    missing_ids = data.query("KEY == 'Type' and VALUE == @missing_type").ID

    assert get_ac_net_position(data[~data.ID.isin(missing_ids)]) is None


# ---------------------------------------------------------------- sum of loads

def load(load_id, load_type, p, q=1.0):
    return [(load_id, "Type", load_type), (load_id, "EnergyConsumer.p", str(p)), (load_id, "EnergyConsumer.q", str(q))]


@pytest.fixture
def loads(make_triplets):
    return make_triplets(load("c1", "ConformLoad", 10.0) + load("c2", "ConformLoad", -5.0) + load("c3", "ConformLoad", 2.54)
                         + load("n1", "NonConformLoad", 100.0, q=-3) + load("e1", "EnergyConsumer", 7.0))


@pytest.mark.parametrize("load_type, expected", [
    ("ConformLoad", 12.5),
    ("NonConformLoad", 100.0),
    ("EnergyConsumer", 7.0),
    ("AsynchronousMachine", 0.0),
])
def test_sum_of_loads_counts_only_positive_active_power_of_given_type(loads, load_type, expected):
    assert get_sum_of_loads(loads, parameter_name=load_type) == pytest.approx(expected)


# ---------------------------------------------------------------- Danish regions

DK2_EIC = "10YDK-2--------M"


def geographical_region(region_id, name, eic=None):
    rows = [(region_id, "Type", "GeographicalRegion"), (region_id, "IdentifiedObject.name", name)]
    return rows + ([(region_id, "IdentifiedObject.energyIdentCodeEic", eic)] if eic else [])


def sub_region(sub_region_id, name, region):
    return [(sub_region_id, "Type", "SubGeographicalRegion"), (sub_region_id, "IdentifiedObject.name", name),
            (sub_region_id, "SubGeographicalRegion.Region", region)]


def control_area_with_eic(eic):
    return control_area("ca") + [("ca", "IdentifiedObject.energyIdentCodeEic", eic)]


DK_CONTROL_AREA = control_area_with_eic(DK2_EIC)
# IGM sub regions point to a "DK" region without EIC, the region carrying the control area EIC has another ID
MISMATCHED_DK_REGIONS = (geographical_region("dk", "DK") + geographical_region("dk_eic", "DK2", DK2_EIC)
                         + sub_region("sjaelland", "Sjaelland", "dk") + sub_region("entsoe", "ENTSO-E", "dk")
                         + DK_CONTROL_AREA)


def regions_of_sub_regions(data):
    if isinstance(data, pl.DataFrame):
        data = data.to_pandas()
    return dict(data.query("KEY == 'SubGeographicalRegion.Region'")[["ID", "VALUE"]].itertuples(index=False))


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_dk_sub_regions_moved_to_region_with_control_area_eic(make_triplets, engine):
    original = make_triplets(MISMATCHED_DK_REGIONS)
    original = original if engine == "pandas" else pl.from_pandas(original)

    data = modify_region_name_for_denmark(original)

    assert type(data) is type(original)
    assert regions_of_sub_regions(data) == {"sjaelland": "dk_eic", "entsoe": "dk"}


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_dk_region_fix_changes_only_the_region_reference(make_triplets, engine):
    original = make_triplets(MISMATCHED_DK_REGIONS)

    data = modify_region_name_for_denmark(original if engine == "pandas" else pl.from_pandas(original))

    data = data if isinstance(data, pd.DataFrame) else data.to_pandas()
    rows = lambda frame: set(frame.astype(str).itertuples(index=False, name=None))
    assert rows(data) - rows(original) == {("sjaelland", "SubGeographicalRegion.Region", "dk_eic", "test-instance")}
    assert rows(original) - rows(data) == {("sjaelland", "SubGeographicalRegion.Region", "dk", "test-instance")}


@pytest.mark.parametrize("rows", [
    geographical_region("dk", "DK") + geographical_region("dk_eic", "DK2", DK2_EIC)
    + sub_region("sjaelland", "Sjaelland", "dk") + sub_region("fyn", "Fyn", "dk_eic") + DK_CONTROL_AREA,
    geographical_region("dk", "DK") + geographical_region("dk_eic", "DK2", DK2_EIC) + geographical_region("dk_eic_2", "DK", DK2_EIC)
    + sub_region("sjaelland", "Sjaelland", "dk") + DK_CONTROL_AREA,
    geographical_region("dk", "DK") + geographical_region("dk_other", "DK1", "10YDK-1--------W")
    + sub_region("sjaelland", "Sjaelland", "dk") + DK_CONTROL_AREA,
    geographical_region("se", "SE") + geographical_region("se_eic", "SE4", "10Y1001A1001A47J")
    + sub_region("skane", "Skane", "se") + control_area_with_eic("10Y1001A1001A47J"),
], ids=["already_referencing_eic_region", "several_eic_regions", "no_region_with_control_area_eic", "not_danish"])
def test_dk_regions_unchanged_outside_the_mismatch_situation(make_triplets, rows):
    original = make_triplets(rows)

    data = modify_region_name_for_denmark(original)

    pd.testing.assert_frame_equal(data, original)
