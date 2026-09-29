"""
Scaler (emf/model_merger/scaler.py) - polars since d567584.

Oracle: pandas scaler.py at d567584^. Both run on identical copies of a real pypowsybl CGM
(CGMES MicroGrid BE + NL merged with subnetworks, see helpers.cgm_network) with identical
schedules. Two things are compared separately on purpose:
  * the network after scaling (what gets exported)       -> numerical parity
  * model.scaled / scaled_entity / scaled_hvdc (report)  -> contract parity
"""
import pytest

from helpers import ac_schedules, cgm_network, dc_schedules, network_state, run_scaler

REPORT_KEYS = {"area", "prescale_acnp", "initial_offset_acnp", "postscale_acnp", "final_offset_acnp", "success"}

SCENARIOS = {
    "reachable_targets": dict(ac=ac_schedules(250.0, 250.0)),
    "large_shift": dict(ac=ac_schedules(400.0, 400.0)),
    "diverges_in_main_island": dict(ac=ac_schedules(-150.0, -150.0)),  # loads driven negative -> LF fails
    "inconsistent_targets": dict(ac=ac_schedules(300.0, 200.0)),  # cannot both be met -> max iterations
}


def _both(scaler_pl, scaler_pd, ac, dc=None, network_factory=cgm_network, **kwargs):
    dc = dc if dc is not None else dc_schedules()
    pl_model = run_scaler(scaler_pl, network_factory(), ac, dc, **kwargs)
    pd_model = run_scaler(scaler_pd, network_factory(), ac, dc, **kwargs)
    return pl_model, pd_model


def _by_area(entity):
    return {row["area"]: row for row in entity}


# ------------------------------------------------------------------------------------ parity

@pytest.mark.parity
@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_network_after_scaling_matches_pandas(scaler_pl, scaler_pd, scenario):
    pl_model, pd_model = _both(scaler_pl, scaler_pd, **SCENARIOS[scenario])
    pl_loads, pl_bl = network_state(pl_model.network)
    pd_loads, pd_bl = network_state(pd_model.network)
    assert pl_loads == pd_loads
    assert pl_bl == pd_bl


@pytest.mark.parity
@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_scaled_flag_matches_pandas(scaler_pl, scaler_pd, scenario):
    pl_model, pd_model = _both(scaler_pl, scaler_pd, **SCENARIOS[scenario])
    assert pl_model.scaled == pd_model.scaled


@pytest.mark.parity
@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_merge_report_scaled_entity_matches_pandas(scaler_pl, scaler_pd, scenario):
    """
    scaled_entity goes to the merge report and to the CGM QAR (merge_functions.lvl8_report_cgm).
    pandas selected ALL rows of iteration 0 and of the last iteration (.loc[[0, max]]); the polars
    port keeps only the first and last row, which drops initial_offset_acnp and postscale_acnp.
    """
    pl_model, pd_model = _both(scaler_pl, scaler_pd, **SCENARIOS[scenario])
    assert _by_area(pl_model.scaled_entity) == _by_area(pd_model.scaled_entity)


@pytest.mark.parity
def test_hvdc_setpoints_and_report_match_pandas(scaler_pl, scaler_pd):
    """One boundary line flagged as HVDC with a DC schedule -> same setpoint and scaled_hvdc."""
    factory = lambda: cgm_network(hvdc_line_index=0)  # noqa: E731
    eic = factory().get_boundary_lines(all_attributes=True).iloc[0]["lineEnergyIdentificationCodeEIC"]
    dc = dc_schedules(resource=eic, value=120.0, in_domain="NL", out_domain="BE")
    pl_model, pd_model = _both(scaler_pl, scaler_pd, ac=ac_schedules(), dc=dc, network_factory=factory)
    assert network_state(pl_model.network) == network_state(pd_model.network)
    assert sorted(pl_model.scaled_hvdc, key=str) == sorted(pd_model.scaled_hvdc, key=str)


@pytest.mark.parity
def test_duplicate_and_nan_domain_schedules_match_pandas(scaler_pl, scaler_pd):
    """ACNP schedules with a zero-value duplicate and 'NaN' strings in the domain fields."""
    ac = ac_schedules(250.0, 250.0) + [
        {"value": 0.0, "in_domain": None, "out_domain": "BE"},        # duplicate, must lose to 250
        {"value": 999.0, "in_domain": "NaN", "out_domain": "NaN"},    # no registered resource
    ]
    pl_model, pd_model = _both(scaler_pl, scaler_pd, ac=ac)
    assert network_state(pl_model.network) == network_state(pd_model.network)


@pytest.mark.parity
def test_debug_diagnostics_path_matches_pandas(scaler_pl, scaler_pd):
    """debug=True runs get_areas_metrics / get_areas_losses (polars joins) on every iteration."""
    pl_model, pd_model = _both(scaler_pl, scaler_pd, ac=ac_schedules(), debug=True)
    assert network_state(pl_model.network) == network_state(pd_model.network)


# ------------------------------------------------------------------------------ functionality

def test_scaling_reaches_targets(scaler_pl, monkeypatch):
    model = run_scaler(scaler_pl, cgm_network(), ac_schedules(250.0, 250.0), dc_schedules())
    assert model.scaled is True
    threshold = int(scaler_pl.BALANCE_THRESHOLD)
    for row in model.scaled_entity:
        assert abs(row["final_offset_acnp"]) <= threshold
        assert row["success"] is True


def test_report_contains_all_documented_keys(scaler_pl):
    """Keys the merge report consumers read (area, final_offset_acnp, success) plus the rest."""
    model = run_scaler(scaler_pl, cgm_network(), ac_schedules(), dc_schedules())
    assert model.scaled_entity, "scaled_entity is empty"
    for row in model.scaled_entity:
        assert REPORT_KEYS <= set(row), f"missing {REPORT_KEYS - set(row)} in {row}"


def test_report_has_only_real_areas(scaler_pl):
    """Regression guard for bd6e400: the ITER column must not appear as a fake area row."""
    model = run_scaler(scaler_pl, cgm_network(), ac_schedules(), dc_schedules())
    assert {row["area"] for row in model.scaled_entity} == {"BE-0", "NL-0"}


# --------------------------------------------------------------------------- configuration

@pytest.mark.config
@pytest.mark.parametrize("setting,value", [
    ("MAX_ITERATION", "1"), ("MAX_ITERATION", "3"), ("BALANCE_THRESHOLD", "0"), ("BALANCE_THRESHOLD", "10"),
    ("CONSTANT_POWER_FACTOR", "True"), ("POWER_FACTOR_THRESHOLD", "0.5"),
])
def test_config_values_behave_like_pandas(scaler_pl, scaler_pd, monkeypatch, setting, value):
    for module in (scaler_pl, scaler_pd):
        monkeypatch.setattr(module, setting, value)
    pl_model, pd_model = _both(scaler_pl, scaler_pd, ac=ac_schedules(300.0, 200.0))
    assert network_state(pl_model.network) == network_state(pd_model.network)
    assert pl_model.scaled == pd_model.scaled
    pl_success = {r["area"]: r["success"] for r in pl_model.scaled_entity}
    pd_success = {r["area"]: r["success"] for r in pd_model.scaled_entity}
    assert pl_success == pd_success


# ----------------------------------------------------------------------- errors / missing input

@pytest.mark.errors
def test_network_without_subnetworks_is_rejected(scaler_pl):
    import pypowsybl as pp
    from helpers import MergedModelStub
    network = pp.network.create_micro_grid_be_network()
    with pytest.raises(Exception, match="missing subnetworks"):
        scaler_pl.scale_balance(model=MergedModelStub(network), ac_schedules=ac_schedules(),
                                dc_schedules=dc_schedules(), debug=False)


@pytest.mark.errors
def test_missing_ac_schedule_for_area_logs_and_matches_pandas(scaler_pl, scaler_pd, caplog):
    ac = [row for row in ac_schedules() if row["out_domain"] == "BE"]
    pl_model, pd_model = _both(scaler_pl, scaler_pd, ac=ac)
    assert "Missing target AC schedule for areas present in network model: ['NL']" in caplog.text
    assert network_state(pl_model.network) == network_state(pd_model.network)


@pytest.mark.errors
def test_hvdc_in_model_without_schedule_value(scaler_pl):
    factory = lambda: cgm_network(hvdc_line_index=0)  # noqa: E731
    eic = factory().get_boundary_lines(all_attributes=True).iloc[0]["lineEnergyIdentificationCodeEIC"]
    dc = dc_schedules(resource=eic, value=None, in_domain="NL", out_domain="BE")
    with pytest.raises(ValueError, match="Missing target DC schedule value"):
        run_scaler(scaler_pl, factory(), ac_schedules(), dc)


@pytest.mark.errors
def test_divergence_after_acnp_alignment_returns_unscaled(scaler_pl, monkeypatch):
    """Second loadflow diverges -> model returned with scaled=False, no exception."""
    import pypowsybl as pp
    real_run_ac = pp.loadflow.run_ac
    calls = {"n": 0}

    from types import SimpleNamespace
    fields = ["connected_component_num", "distributed_active_power", "iteration_count",
              "synchronous_component_num"]

    def diverged(result):
        data = {f: getattr(result, f) for f in fields}
        return SimpleNamespace(**data, status=pp.loadflow.ComponentStatus.FAILED)

    def run_ac(*args, **kwargs):
        calls["n"] += 1
        results = real_run_ac(*args, **kwargs)
        return [diverged(r) for r in results] if calls["n"] == 2 else results

    monkeypatch.setattr(scaler_pl.pp.loadflow, "run_ac", run_ac)
    model = run_scaler(scaler_pl, cgm_network(), ac_schedules(), dc_schedules())
    assert model.scaled is False


@pytest.mark.errors
def test_network_without_ishvdc_property_fails_clearly(scaler_pl):
    """Real CGMs carry isHvdc from the boundary set; without it scaling cannot classify lines."""
    import pypowsybl as pp
    network = pp.network.create_micro_grid_be_network()
    network.merge([pp.network.create_micro_grid_nl_network()])
    with pytest.raises(Exception):
        run_scaler(scaler_pl, network, ac_schedules(), dc_schedules())


# ------------------------------------------------------------------ integration with the merger

@pytest.mark.integration
def test_scaled_entity_feeds_cgm_qar_without_fake_areas(scaler_pl, monkeypatch):
    """scale_balance -> merge report -> lvl8_report_cgm: every ruleTarget must be a real area."""
    from emf.model_merger import merge_functions
    monkeypatch.setattr(scaler_pl, "BALANCE_THRESHOLD", "0")  # force per-area failures -> QAR warnings
    model = run_scaler(scaler_pl, cgm_network(), ac_schedules(300.0, 200.0), dc_schedules())
    failed = [r for r in model.scaled_entity if not r.get("success", True)]
    assert failed, "scenario must produce failed areas"
    assert {r["area"] for r in failed} <= {"BE-0", "NL-0"}
    assert all(r.get("final_offset_acnp") is not None for r in failed)
