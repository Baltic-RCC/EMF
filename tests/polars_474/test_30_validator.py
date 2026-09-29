"""
Model validator functions (emf/model_validator/validator_functions.py) - polars since 09f5ebc/54281cc.

Oracle: pandas validator_functions.py at 09f5ebc^ (except get_ac_net_position, which was
rebuilt on purpose in 3367dcc and is checked against hand-computed values instead).
"""
import pandas as pd
import polars as pl
import pypowsybl as pp
import pytest

from helpers import assert_same_triplets, cgmes_triplets, to_pd, triplets

PREEXISTING = "pre-existing: pandas original behaves the same, not introduced by #474"


@pytest.fixture
def pl_accepts_triplets(monkeypatch, validator_pl):
    """
    get_nodes_against_kirchhoff_first_law on dev still calls load_opdm_objects_to_triplets on its
    input unconditionally (the fix is on the unmerged branch fix-model-validator, 9088148). For
    parity tests on triplets we bypass that; test_kirchhoff_accepts_triplets_directly covers the gap.
    """
    monkeypatch.setattr(validator_pl, "load_opdm_objects_to_triplets", lambda opdm_objects, **_: opdm_objects)


@pytest.fixture(scope="module")
def exported_models():
    models = {}
    for name, factory in [("BE", pp.network.create_micro_grid_be_network),
                          ("NL", pp.network.create_micro_grid_nl_network)]:
        network = factory()
        pp.loadflow.run_ac(network)
        models[name] = cgmes_triplets(network)
    return models


def switch_model():
    """S1 violates (two TNs), S2 same TN, S3 retained, S4 already open."""
    rows = []
    for sw, retained, is_open, tns in [("S1", "false", "false", ("TN_A", "TN_B")),
                                       ("S2", "false", "false", ("TN_A", "TN_A")),
                                       ("S3", "true", "false", ("TN_A", "TN_B")),
                                       ("S4", "false", "true", ("TN_A", "TN_B"))]:
        rows += [(sw, "Type", "Breaker"), (sw, "Switch.retained", retained), (sw, "Switch.open", is_open)]
        for i, tn in enumerate(tns, start=1):
            t = f"{sw}_T{i}"
            rows += [(t, "Type", "Terminal"), (t, "Terminal.ConductingEquipment", sw),
                     (t, "Terminal.TopologicalNode", tn)]
    return triplets(rows)


def kirchhoff_model(extra_rows=()):
    """TN_A balanced (10 / -10), TN_B off by 1 MW (5 / -4), SvInjection on TN_B of -1 MW."""
    rows = []
    for t, tn, p in [("T1", "TN_A", "10"), ("T2", "TN_A", "-10"), ("T3", "TN_B", "5"), ("T4", "TN_B", "-4")]:
        rows += [(t, "Type", "Terminal"), (t, "Terminal.ConductingEquipment", f"EQ_{t}"),
                 (t, "Terminal.TopologicalNode", tn),
                 (f"F_{t}", "Type", "SvPowerFlow"), (f"F_{t}", "SvPowerFlow.Terminal", t),
                 (f"F_{t}", "SvPowerFlow.p", p), (f"F_{t}", "SvPowerFlow.q", "0")]
    rows += [("INJ", "Type", "SvInjection"), ("INJ", "SvInjection.TopologicalNode", "TN_B"),
             ("INJ", "SvInjection.pInjection", "-1"), ("INJ", "SvInjection.qInjection", "0")]
    rows += list(extra_rows)
    return triplets(rows)


def dk_region_model():
    """DK sub-region points to a region without EIC; the control area EIC region must be used."""
    return triplets([
        ("GR_DK", "Type", "GeographicalRegion"), ("GR_DK", "IdentifiedObject.name", "DK"),
        ("GR_DK", "IdentifiedObject.energyIdentCodeEic", "NONE"),
        ("GR_EIC", "Type", "GeographicalRegion"), ("GR_EIC", "IdentifiedObject.name", "Energinet"),
        ("GR_EIC", "IdentifiedObject.energyIdentCodeEic", "10Y1001A1001A796"),
        ("CA", "Type", "ControlArea"), ("CA", "IdentifiedObject.energyIdentCodeEic", "10Y1001A1001A796"),
        ("CA", "IdentifiedObject.name", "DK1"),
        ("SGR", "Type", "SubGeographicalRegion"), ("SGR", "IdentifiedObject.name", "DK-West"),
        ("SGR", "SubGeographicalRegion.Region", "GR_DK"),
        ("SGR_B", "Type", "SubGeographicalRegion"), ("SGR_B", "IdentifiedObject.name", "ENTSO-E"),
        ("SGR_B", "SubGeographicalRegion.Region", "GR_DK"),
    ])


def acnp_model(area_type="ControlAreaTypeKind.Interchange"):
    """
    Three tie points on one control area:
      TN_X  AC tie, injection 100 MW        -> counted
      TN_Y  boundary node described 'HVDC'  -> excluded
      TN_Z  equipment is a DCLineSegment    -> excluded
    """
    rows = [("CA", "Type", "ControlArea"), ("CA", "ControlArea.type", area_type)]
    for i, (tn, equipment_type, description, p) in enumerate(
            [("TN_X", "ACLineSegment", "Xnode", "100"), ("TN_Y", "ACLineSegment", "HVDC Estlink", "50"),
             ("TN_Z", "DCLineSegment", "Xnode", "25")]):
        eq, t, tf, ei, ti = f"L{i}", f"T{i}", f"TF{i}", f"EI{i}", f"TI{i}"
        rows += [(tn, "Type", "TopologicalNode"), (tn, "IdentifiedObject.description", description),
                 (eq, "Type", equipment_type),
                 (t, "Type", "Terminal"), (t, "Terminal.ConductingEquipment", eq), (t, "Terminal.TopologicalNode", tn),
                 (tf, "Type", "TieFlow"), (tf, "TieFlow.ControlArea", "CA"), (tf, "TieFlow.Terminal", t),
                 (ei, "Type", "EquivalentInjection"), (ei, "EquivalentInjection.p", p),
                 (ti, "Type", "Terminal"), (ti, "Terminal.ConductingEquipment", ei), (ti, "Terminal.TopologicalNode", tn)]
    return triplets(rows)


def loads_model():
    return triplets([
        ("L1", "Type", "ConformLoad"), ("L1", "EnergyConsumer.p", "100.5"), ("L1", "EnergyConsumer.q", "10"),
        ("L2", "Type", "ConformLoad"), ("L2", "EnergyConsumer.p", "-20"), ("L2", "EnergyConsumer.q", "-2"),
        ("L3", "Type", "NonConformLoad"), ("L3", "EnergyConsumer.p", "40"), ("L3", "EnergyConsumer.q", "4"),
        ("L4", "Type", "ConformLoad"), ("L4", "EnergyConsumer.p", "0"),
    ])


# ------------------------------------------------------------------------------------ parity

@pytest.mark.parity
@pytest.mark.parametrize("model", ["BE", "NL"])
@pytest.mark.parametrize("nodes_only", [True, False])
@pytest.mark.parametrize("sv_injection", [True, False])
def test_kirchhoff_matches_pandas_on_exported_igm(validator_pl, validator_pd, pl_accepts_triplets,
                                                 exported_models, model, nodes_only, sv_injection):
    data = exported_models[model]
    kwargs = dict(nodes_only=nodes_only, consider_sv_injection=sv_injection)
    got = validator_pl.get_nodes_against_kirchhoff_first_law(data, **kwargs)
    exp = validator_pd.get_nodes_against_kirchhoff_first_law(data, **kwargs)
    assert sorted(got["Terminal.TopologicalNode"].astype(str)) == sorted(exp["Terminal.TopologicalNode"].astype(str))


@pytest.mark.parity
@pytest.mark.parametrize("sv_injection", [True, False])
def test_kirchhoff_matches_pandas_on_synthetic_node(validator_pl, validator_pd, pl_accepts_triplets, sv_injection):
    data = kirchhoff_model()
    got = validator_pl.get_nodes_against_kirchhoff_first_law(data, nodes_only=True, consider_sv_injection=sv_injection)
    exp = validator_pd.get_nodes_against_kirchhoff_first_law(data, nodes_only=True, consider_sv_injection=sv_injection)
    assert sorted(got["Terminal.TopologicalNode"]) == sorted(exp["Terminal.TopologicalNode"])
    assert sorted(got["Terminal.TopologicalNode"]) == ([] if sv_injection else ["TN_B"])


@pytest.mark.parity
def test_kirchhoff_ignores_flows_of_unknown_terminals_like_pandas(validator_pl, validator_pd, pl_accepts_triplets):
    """
    A flow whose terminal is not in the models has no TopologicalNode. pandas groupby drops the
    NaN key; polars group_by keeps a null group, which is reported as a violated node.
    """
    data = kirchhoff_model([("F_X", "Type", "SvPowerFlow"), ("F_X", "SvPowerFlow.Terminal", "T_UNKNOWN"),
                            ("F_X", "SvPowerFlow.p", "7"), ("F_X", "SvPowerFlow.q", "0")])
    got = validator_pl.get_nodes_against_kirchhoff_first_law(data, nodes_only=True, consider_sv_injection=True)
    exp = validator_pd.get_nodes_against_kirchhoff_first_law(data, nodes_only=True, consider_sv_injection=True)
    assert exp.empty
    assert got["Terminal.TopologicalNode"].isna().sum() == 0 and got.empty, \
        f"null TopologicalNode reported as violated node: {got.to_dict('records')}"


@pytest.mark.parity
@pytest.mark.parametrize("open_switches", [False, True])
def test_non_retained_switches_match_pandas(validator_pl, validator_pd, open_switches):
    data = switch_model()
    got_data, got_count = validator_pl.check_not_retained_switches_between_nodes(data.copy(), open_switches)
    exp_data, exp_count = validator_pd.check_not_retained_switches_between_nodes(data.copy(), open_switches)
    assert got_count == exp_count == 1
    assert_same_triplets(got_data, exp_data)
    if open_switches:
        assert ("S1", "Switch.open", "str:true") in {(r.ID, r.KEY, "str:" + str(r.VALUE))
                                                     for r in to_pd(got_data).itertuples()}


@pytest.mark.parity
@pytest.mark.parametrize("model", ["BE", "NL"])
def test_non_retained_switches_match_pandas_on_exported_igm(validator_pl, validator_pd, exported_models, model):
    data = exported_models[model]
    assert validator_pl.check_not_retained_switches_between_nodes(data)[1] == \
        validator_pd.check_not_retained_switches_between_nodes(data)[1]


@pytest.mark.parity
@pytest.mark.parametrize("parameter_name", ["ConformLoad", "NonConformLoad", None])
def test_sum_of_loads_matches_pandas(validator_pl, validator_pd, parameter_name):
    data = loads_model()
    assert validator_pl.get_sum_of_loads(data, parameter_name) == validator_pd.get_sum_of_loads(data, parameter_name)


def test_sum_of_loads_excludes_negative_conform_loads(validator_pl):
    assert validator_pl.get_sum_of_loads(loads_model()) == 100.5


@pytest.mark.parity
def test_denmark_region_fix_matches_pandas(validator_pl, validator_pd):
    data = dk_region_model()
    got = validator_pl.modify_region_name_for_denmark(data.copy())
    exp = validator_pd.modify_region_name_for_denmark(data.copy())
    assert_same_triplets(got, exp)
    region = to_pd(got).query("ID == 'SGR' and KEY == 'SubGeographicalRegion.Region'")["VALUE"].tolist()
    assert region == ["GR_EIC"]


@pytest.mark.parity
@pytest.mark.xfail(reason=PREEXISTING + " (model without ControlArea -> type_tableview returns None)", strict=True)
@pytest.mark.parametrize("model", ["BE", "NL"])
def test_denmark_region_fix_is_noop_for_other_models(validator_pl, exported_models, model):
    data = exported_models[model]
    assert_same_triplets(validator_pl.modify_region_name_for_denmark(data.copy()), data)


# ------------------------------------------------------------------------------ functionality

@pytest.mark.parametrize("area_type", ["ControlAreaTypeKind.Interchange",
                                       "http://iec.ch/TC57/2013/CIM-schema-cim16#ControlAreaTypeKind.Interchange"])
def test_ac_net_position_counts_only_ac_interchange_ties(validator_pl, area_type):
    assert validator_pl.get_ac_net_position(acnp_model(area_type)) == 100.0


def test_ac_net_position_ignores_non_interchange_areas(validator_pl):
    assert validator_pl.get_ac_net_position(acnp_model("ControlAreaTypeKind.Forecast")) == 0.0


@pytest.mark.parametrize("engine", ["pandas", "polars"])
def test_functions_accept_both_engines(validator_pl, pl_accepts_triplets, engine):
    """Callers pass pandas today; polars input must work too and keep its type where returned."""
    convert = (lambda d: d) if engine == "pandas" else pl.from_pandas
    assert validator_pl.get_ac_net_position(convert(acnp_model())) == 100.0
    assert validator_pl.get_sum_of_loads(convert(loads_model())) == 100.5
    data, count = validator_pl.check_not_retained_switches_between_nodes(convert(switch_model()), True)
    assert count == 1 and isinstance(data, pl.DataFrame if engine == "polars" else pd.DataFrame)
    modified = validator_pl.modify_region_name_for_denmark(convert(dk_region_model()))
    assert isinstance(modified, pl.DataFrame if engine == "polars" else pd.DataFrame)
    nodes = validator_pl.get_nodes_against_kirchhoff_first_law(convert(kirchhoff_model()), nodes_only=True)
    assert list(nodes["Terminal.TopologicalNode"]) == ["TN_B"]


# ----------------------------------------------------------------------- errors / missing input

@pytest.mark.errors
@pytest.mark.xfail(reason=PREEXISTING + "; fixed on unmerged branch fix-model-validator (9088148)", strict=True)
def test_kirchhoff_accepts_triplets_directly(validator_pl):
    """PostLFValidator passes parsed triplets (model_validator.py:131); dev crashes on that input."""
    result = validator_pl.get_nodes_against_kirchhoff_first_law(kirchhoff_model(), nodes_only=True)
    assert list(result["Terminal.TopologicalNode"]) == ["TN_B"]


@pytest.mark.errors
def test_ac_net_position_missing_tables_returns_none(validator_pl):
    assert validator_pl.get_ac_net_position(loads_model()) is None


@pytest.mark.errors
def test_ac_net_position_unparsable_value_is_skipped(validator_pl):
    data = acnp_model()
    data.loc[(data.ID == "EI0") & (data.KEY == "EquivalentInjection.p"), "VALUE"] = "not-a-number"
    assert validator_pl.get_ac_net_position(data) == 0.0


@pytest.mark.errors
def test_sum_of_loads_non_numeric_value_raises_like_pandas(validator_pl, validator_pd):
    data = triplets([("L1", "Type", "ConformLoad"), ("L1", "EnergyConsumer.p", "abc")])
    with pytest.raises(Exception):
        validator_pd.get_sum_of_loads(data)
    with pytest.raises(Exception):
        validator_pl.get_sum_of_loads(data)


@pytest.mark.errors
def test_sum_of_loads_no_loads(validator_pl):
    assert validator_pl.get_sum_of_loads(triplets([("X", "Type", "Breaker")])) == 0.0


@pytest.mark.errors
def test_kirchhoff_without_terminals_returns_empty(validator_pl, pl_accepts_triplets):
    result = validator_pl.get_nodes_against_kirchhoff_first_law(triplets([("X", "Type", "Breaker")]))
    assert isinstance(result, pd.DataFrame) and result.empty


@pytest.mark.errors
def test_switch_check_without_switches(validator_pl):
    data = triplets([("T1", "Type", "Terminal"), ("T1", "Terminal.TopologicalNode", "TN"),
                     ("T1", "Terminal.ConductingEquipment", "L1")])
    returned, count = validator_pl.check_not_retained_switches_between_nodes(data, True)
    assert count == 0 and returned is data


@pytest.mark.errors
@pytest.mark.xfail(reason=PREEXISTING, strict=True)
def test_denmark_fix_on_model_without_regions(validator_pl):
    """TemporaryPreMergeModifications runs this for every DKE/DKW model; a model missing
    GeographicalRegion must not crash the pre-merge step."""
    data = triplets([("X", "Type", "Breaker")])
    assert_same_triplets(validator_pl.modify_region_name_for_denmark(data), data)
