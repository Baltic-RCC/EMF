"""
Post-merge processing (emf/model_merger/post_processing.py) - polars since PR #524 (dev, b94156b).

Oracle: pandas post_processing.py at b94156b^1 (dev right before the merge). Every function is
run on the same synthetic CGM (pp_scenario.py) through both implementations and the resulting
triplet tables are compared as sets of (ID, KEY, VALUE), with numeric VALUEs compared as numbers
so '0', 0 and '0.0' are equal.
"""
import itertools
import uuid

import polars as pl
import pytest

import pp_scenario
from helpers import assert_same_triplets, to_pd, triplet_set, triplets


@pytest.fixture
def data():
    original, sv, ssh = pp_scenario.build()
    return {"original": original, "sv": sv, "ssh": ssh}


@pytest.fixture(autouse=True)
def deterministic_uuid(monkeypatch):
    """add_missing_sv_tap_steps creates new ids with uuid4; make them identical in both runs."""
    state = {"counter": itertools.count()}

    def reset():
        state["counter"] = itertools.count()

    monkeypatch.setattr(uuid, "uuid4", lambda: uuid.UUID(int=next(state["counter"])))
    return reset


def _pd(data):
    """
    Object-dtype copies for the pandas oracle: it was written for pandas 2 object columns and
    cannot assign ints into pandas 3 pyarrow string columns (oracle limitation, not under test).
    """
    return {k: v.astype(object) for k, v in data.items()}


def _pl(data):
    return {k: pl.from_pandas(v) for k, v in data.items()}


# ------------------------------------------------------------------------ function-level parity
# (name, call(module, frames) -> output or tuple of outputs, oracle function name if renamed)

CASES = {
    "remove_small_islands": lambda m, d: m.remove_small_islands(d["sv"], 10),
    "remove_equivalent_shunt_section": lambda m, d: m.remove_equivalent_shunt_section(d["sv"], d["original"]),
    "add_missing_sv_tap_steps": lambda m, d: m.add_missing_sv_tap_steps(d["sv"], d["ssh"]),
    "check_and_fix_dependencies": lambda m, d: m.check_and_fix_dependencies(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_data=d["original"]),
    "remove_duplicate_sv_voltages": lambda m, d: m.remove_duplicate_sv_voltages(
        cgm_sv_data=d["sv"], original_data=d["original"]),
    "set_paired_boundary_injections_to_zero": lambda m, d: m.set_paired_boundary_injections_to_zero(
        original_models=d["original"], cgm_ssh_data=d["ssh"]),
    "check_energized_boundary_nodes": lambda m, d: m.check_energized_boundary_nodes(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"], fix_errors=True),
    "check_for_disconnected_terminals": lambda m, d: m.check_for_disconnected_terminals(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"], fix_errors=True),
    "check_non_regulating_rotating_machine_q": lambda m, d: m.check_non_regulating_rotating_machine_q(
        cgm_ssh_data=d["ssh"], original_models=d["original"], fix_errors=True),
    "check_rotating_machine_q_outside_p_limits": lambda m, d: m.check_rotating_machine_q_outside_p_limits(
        cgm_ssh_data=d["ssh"], original_models=d["original"], fix_errors=True),
    "check_non_ltc_tap_changer_step": lambda m, d: m.check_non_ltc_tap_changer_step(
        cgm_ssh_data=d["ssh"], cgm_sv_data=d["sv"], original_models=d["original"], fix_errors=True),
    "check_net_interchanges": lambda m, d: m.check_net_interchanges(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"]),
    "check_non_boundary_equivalent_injections": lambda m, d: m.check_non_boundary_equivalent_injections(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"], threshold=0.1, fix_errors=True),
    "energy_source_injections": lambda m, d: getattr(
        m, "check_all_kind_of_injections", getattr(m, "check_injection_type_vs_powerflow", None))(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"], injection_name="EnergySource",
        fields_to_check={"SvPowerFlow.p": "EnergySource.activePower"}, threshold=0.1, fix_errors=True),
    "external_network_injections": lambda m, d: getattr(
        m, "check_all_kind_of_injections", getattr(m, "check_injection_type_vs_powerflow", None))(
        cgm_sv_data=d["sv"], cgm_ssh_data=d["ssh"], original_models=d["original"],
        injection_name="ExternalNetworkInjection", fields_to_check={"SvPowerFlow.p": "ExternalNetworkInjection.p"},
        threshold=0.1, fix_errors=True),
}


def _as_tuple(result):
    return result if isinstance(result, tuple) else (result,)


def _without_pandas_artifacts(frame):
    """
    The pandas check_net_interchanges wrote a stray ('<area>', 'index', 0) triplet into the SSH
    (leaked reset_index column). polars fixed that; it is excluded from parity on purpose and
    asserted separately in test_net_interchange_writes_no_index_triplet.
    """
    frame = to_pd(frame)
    return frame[frame["KEY"].astype(str) != "index"]


@pytest.mark.parity
@pytest.mark.parametrize("case", sorted(CASES))
def test_function_matches_pandas(post_processing_pl, post_processing_pd, data, deterministic_uuid, case):
    deterministic_uuid()
    expected = _as_tuple(CASES[case](post_processing_pd, _pd(data)))
    deterministic_uuid()
    got = _as_tuple(CASES[case](post_processing_pl, _pl(data)))
    assert len(got) == len(expected)
    for g, e in zip(got, expected):
        assert isinstance(g, pl.DataFrame), f"{case} returned {type(g).__name__}, pipeline expects polars"
        assert_same_triplets(g, _without_pandas_artifacts(e))


@pytest.mark.parity
@pytest.mark.parametrize("case", sorted(CASES))
def test_function_changes_something(post_processing_pl, data, case):
    """Guard against a vacuous parity pass: the scenario must trigger every function."""
    got = _as_tuple(CASES[case](post_processing_pl, _pl(data)))
    before = {k: triplet_set(v) for k, v in data.items()}
    after = [triplet_set(g) for g in got]
    assert any(a not in before.values() for a in after), f"{case}: scenario did not trigger any change"


# ------------------------------------------------------------------------ pipeline (integration)

def _pipeline(module, d, additional_processing=True, threshold=0.1, fix=True):
    """Body of run_post_merge_processing after the pandas->polars boundary (same order as production)."""
    sv, ssh, original = d["sv"], d["ssh"], d["original"]
    sv = module.remove_equivalent_shunt_section(sv, original)
    sv = module.remove_small_islands(sv, 10)
    sv = module.remove_duplicate_sv_voltages(cgm_sv_data=sv, original_data=original)
    sv = module.add_missing_sv_tap_steps(sv, ssh)
    sv = module.check_and_fix_dependencies(cgm_sv_data=sv, cgm_ssh_data=ssh, original_data=original)
    ssh = module.set_paired_boundary_injections_to_zero(original_models=original, cgm_ssh_data=ssh)
    if additional_processing:
        sv, ssh = module.check_for_disconnected_terminals(cgm_sv_data=sv, cgm_ssh_data=ssh,
                                                          original_models=original, fix_errors=True)
        ssh = module.check_energized_boundary_nodes(cgm_sv_data=sv, cgm_ssh_data=ssh,
                                                    original_models=original, fix_errors=True)
        ssh = module.check_non_regulating_rotating_machine_q(cgm_ssh_data=ssh, original_models=original,
                                                             fix_errors=True)
        ssh = module.check_rotating_machine_q_outside_p_limits(cgm_ssh_data=ssh, original_models=original,
                                                               fix_errors=True)
        ssh, sv = module.check_non_ltc_tap_changer_step(cgm_ssh_data=ssh, cgm_sv_data=sv,
                                                        original_models=original, fix_errors=True)
    check = getattr(module, "check_all_kind_of_injections", None) or module.check_injection_type_vs_powerflow
    for name, field in [("EnergySource", "EnergySource.activePower"),
                        ("ExternalNetworkInjection", "ExternalNetworkInjection.p")]:
        ssh = check(cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original, injection_name=name,
                    fields_to_check={"SvPowerFlow.p": field}, threshold=threshold, fix_errors=fix)
    ssh = module.check_non_boundary_equivalent_injections(cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original,
                                                          threshold=threshold, fix_errors=fix)
    try:
        ssh = module.check_net_interchanges(cgm_sv_data=sv, cgm_ssh_data=ssh, original_models=original)
    except (KeyError, pl.exceptions.ColumnNotFoundError):
        pass
    return sv, ssh


@pytest.mark.integration
@pytest.mark.parametrize("additional_processing", [True, False])
@pytest.mark.parametrize("fix", [True, False])
def test_full_post_processing_chain_matches_pandas(post_processing_pl, post_processing_pd, data,
                                                   deterministic_uuid, additional_processing, fix):
    deterministic_uuid()
    sv_e, ssh_e = _pipeline(post_processing_pd, _pd(data), additional_processing, fix=fix)
    deterministic_uuid()
    sv_g, ssh_g = _pipeline(post_processing_pl, _pl(data), additional_processing, fix=fix)
    assert_same_triplets(sv_g, _without_pandas_artifacts(sv_e))
    assert_same_triplets(ssh_g, _without_pandas_artifacts(ssh_e))


def test_net_interchange_writes_no_index_triplet(post_processing_pl, data):
    ssh = to_pd(post_processing_pl.check_net_interchanges(pl.from_pandas(data["sv"]), pl.from_pandas(data["ssh"]),
                                                          pl.from_pandas(data["original"])))
    assert not (ssh["KEY"].astype(str) == "index").any()
    assert float(ssh.query("ID == 'CA1' and KEY == 'ControlArea.netInterchange'")["VALUE"].iloc[0]) == 95.0


@pytest.mark.integration
def test_chain_output_converts_back_to_pandas_for_export(post_processing_pl, data):
    """run_post_merge_processing hands pandas to export_to_cgmes_zip; schema must survive."""
    sv, ssh = _pipeline(post_processing_pl, _pl(data))
    for frame in (sv.to_pandas(), ssh.to_pandas()):
        assert list(frame.columns)[:4] == ["ID", "KEY", "VALUE", "INSTANCE_ID"]
        assert frame["ID"].notna().all() and frame["KEY"].notna().all()


# ------------------------------------------------------------------------- specific behaviour

def test_duplicate_boundary_voltage_keeps_non_zero(post_processing_pl, data):
    sv = to_pd(post_processing_pl.remove_duplicate_sv_voltages(pl.from_pandas(data["sv"]),
                                                               pl.from_pandas(data["original"])))
    kept = sv[(sv.KEY == "SvVoltage.TopologicalNode") & (sv.VALUE == "XN1")]["ID"].tolist()
    assert kept == ["SVV1b"]


def test_duplicate_voltage_with_unparsable_value(post_processing_pl, post_processing_pd, data):
    """
    A non-numeric voltage on a duplicated boundary node: pandas raised decimal.InvalidOperation
    (whole post-processing aborted); polars must not crash and must keep the valid 401 kV row.
    """
    import decimal
    sv = data["sv"].copy()
    sv.loc[(sv.ID == "SVV1a") & (sv.KEY == "SvVoltage.v"), "VALUE"] = "n/a"
    with pytest.raises(decimal.InvalidOperation):
        post_processing_pd.remove_duplicate_sv_voltages(sv.astype(object), data["original"].astype(object))
    got = to_pd(post_processing_pl.remove_duplicate_sv_voltages(pl.from_pandas(sv), pl.from_pandas(data["original"])))
    kept = got[(got.KEY == "SvVoltage.TopologicalNode") & (got.VALUE == "XN1")]["ID"].tolist()
    assert kept == ["SVV1b"], f"kept {kept}; a row whose voltage cannot be parsed was preferred over 401 kV"


def test_paired_injections_zeroed_unpaired_untouched(post_processing_pl, data):
    ssh = to_pd(post_processing_pl.set_paired_boundary_injections_to_zero(pl.from_pandas(data["original"]),
                                                                         pl.from_pandas(data["ssh"])))
    p = ssh[ssh.KEY == "EquivalentInjection.p"].set_index("ID")["VALUE"].astype(float)
    assert p["EI_A1"] == p["EI_B1"] == p["EI_A2"] == p["EI_B2"] == 0.0
    assert p["EI_A3"] == 4.0 and p["EI_INT"] == 10.0


# ----------------------------------------------------------------------- errors / missing input

@pytest.mark.errors
@pytest.mark.parametrize("case", sorted(CASES))
def test_function_tolerates_minimal_input(post_processing_pl, post_processing_pd, case):
    """
    Profiles with none of the objects a function looks for (e.g. an IGM without tap changers or
    boundary nodes) must pass through unchanged, or fail the same way pandas did.
    """
    empty = {"original": triplets([("FM_A_TP", "Type", "FullModel"),
                                   ("FM_A_TP", "Model.profile", "http://entsoe.eu/CIM/Topology/4/1")]),
             "sv": triplets([("FM_SV", "Type", "FullModel")], "SV"),
             "ssh": triplets([("FM_SSH", "Type", "FullModel"),
                              ("FM_SSH", "Model.profile", "http://entsoe.eu/CIM/SteadyStateHypothesis/1/1")], "SSH")}
    try:
        expected = _as_tuple(CASES[case](post_processing_pd, _pd(empty)))
        pandas_error = None
    except Exception as error:  # noqa: BLE001
        expected, pandas_error = None, error
    try:
        got = _as_tuple(CASES[case](post_processing_pl, _pl(empty)))
        polars_error = None
    except Exception as error:  # noqa: BLE001
        got, polars_error = None, error
    if pandas_error is None:
        assert polars_error is None, f"{case}: polars raised {type(polars_error).__name__}: {polars_error}; pandas did not"
        for g, e in zip(got, expected):
            assert_same_triplets(g, e)
    else:
        assert polars_error is not None, f"{case}: pandas raised {pandas_error!r}, polars silently continued"


@pytest.mark.errors
def test_injection_check_without_fields_is_noop(post_processing_pl, data):
    ssh = pl.from_pandas(data["ssh"])
    assert post_processing_pl.check_all_kind_of_injections(pl.from_pandas(data["sv"]), ssh,
                                                           pl.from_pandas(data["original"]),
                                                           fields_to_check=None) is ssh


@pytest.mark.errors
def test_injection_check_unknown_type_is_noop(post_processing_pl, data):
    ssh = pl.from_pandas(data["ssh"])
    out = post_processing_pl.check_all_kind_of_injections(
        pl.from_pandas(data["sv"]), ssh, pl.from_pandas(data["original"]), injection_name="AsynchronousMachine",
        fields_to_check={"SvPowerFlow.p": "RotatingMachine.p"})
    assert out is ssh
