"""
Performance: the reason for #474. pandas and polars run on the same enlarged inputs, best of N
runs each. Results are written to tests/polars_474/results/performance.json for the test report.

Run with:  pytest tests/polars_474 -m performance
Knobs (environment variables):
  PERF_TOLERANCE  polars may be at most this factor slower than pandas before failing (default 1.0)
  PERF_SCALE      multiplies input sizes (default 1)
  PERF_REPEAT     runs per implementation, best is kept (default 3)
"""
import copy
import json
import os
import time
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import pp_scenario
from helpers import (ac_schedules, cgm_network, cgmes_triplets, dc_schedules, es_replacement_records, fake_query_data,
                     identity_get_content, run_scaler)

pytestmark = pytest.mark.performance

TOLERANCE = float(os.environ.get("PERF_TOLERANCE", "1.0"))
SCALE = int(os.environ.get("PERF_SCALE", "1"))
REPEAT = int(os.environ.get("PERF_REPEAT", "3"))
RESULTS_FILE = Path(__file__).resolve().parent / "results" / "performance.json"
_results = {}


def _best(fn):
    times = []
    for _ in range(REPEAT):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return min(times)


def _record(name, rows, t_pd, t_pl):
    _results[name] = {"input_rows": rows, "pandas_s": round(t_pd, 4), "polars_s": round(t_pl, 4),
                      "speedup": round(t_pd / t_pl, 2) if t_pl else None}
    RESULTS_FILE.parent.mkdir(exist_ok=True)
    existing = json.loads(RESULTS_FILE.read_text()) if RESULTS_FILE.exists() else {}
    existing.update(_results)
    RESULTS_FILE.write_text(json.dumps(existing, indent=2))
    assert t_pl <= t_pd * TOLERANCE, f"{name}: polars {t_pl:.3f}s vs pandas {t_pd:.3f}s (tolerance x{TOLERANCE})"


def replicate(df, copies, arrow=False):
    """
    Copy a triplet table `copies` times with unique ids; references in VALUE follow their ids.
    arrow=True gives the pyarrow-backed dtypes that read_RDF produces in production.
    """
    ids = set(df["ID"].astype(str))
    frames = []
    value = df["VALUE"].astype(str)
    is_ref = value.isin(ids)
    for k in range(copies):
        part = df.astype(object).copy()
        part["ID"] = df["ID"].astype(str) + f"_{k}"
        part.loc[is_ref, "VALUE"] = value[is_ref] + f"_{k}"
        frames.append(part)
    out = pd.concat(frames, ignore_index=True)
    if arrow:
        import pyarrow as pa
        dictionary = pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))
        out = out.astype({"ID": "string[pyarrow]", "KEY": dictionary, "VALUE": "string[pyarrow]",
                          "INSTANCE_ID": dictionary})
    return out


@pytest.fixture(scope="module")
def big_igm():
    import pypowsybl as pp
    network = pp.network.create_micro_grid_be_network()
    pp.loadflow.run_ac(network)
    return replicate(cgmes_triplets(network), 300 * SCALE, arrow=True)


# ---------------------------------------------------------------------------------- replacement

def test_replacement_speed(monkeypatch, replacement_pl, replacement_pd):
    tsos = [f"TSO{i}" for i in range(12)]
    records = es_replacement_records(seed=1, tsos=tsos, per_tso=2500 * SCALE)
    for module in (replacement_pl, replacement_pd):
        monkeypatch.setattr(module, "query_data", fake_query_data(records))
        monkeypatch.setattr(module, "get_content", identity_get_content)
    args = (tsos, "ID", "2026-09-29T10:30:00Z")
    t_pd = _best(lambda: replacement_pd.find_replacement_models(*args))
    t_pl = _best(lambda: replacement_pl.find_replacement_models(*args))
    _record("replacement.find_replacement_models", len(records), t_pd, t_pl)


# ------------------------------------------------------------------------------------ validator

VALIDATOR_CALLS = {
    "kirchhoff": lambda m, d: m.get_nodes_against_kirchhoff_first_law(d, nodes_only=True, consider_sv_injection=True),
    "switches": lambda m, d: m.check_not_retained_switches_between_nodes(d),
    "sum_of_loads": lambda m, d: m.get_sum_of_loads(d, None),
    "ac_net_position": lambda m, d: m.get_ac_net_position(d),
}


@pytest.mark.parametrize("function", ["kirchhoff", "switches", "sum_of_loads"])
def test_validator_speed(monkeypatch, validator_pl, validator_pd, big_igm, function):
    """Production path: model_validator passes pandas triplets, each function converts them to polars."""
    monkeypatch.setattr(validator_pl, "load_opdm_objects_to_triplets", lambda opdm_objects, **_: opdm_objects)
    t_pd = _best(lambda: VALIDATOR_CALLS[function](validator_pd, big_igm))
    t_pl = _best(lambda: VALIDATOR_CALLS[function](validator_pl, big_igm))
    _record(f"validator.{function} (pandas input, production path)", len(big_igm), t_pd, t_pl)


def test_validator_conversion_overhead(monkeypatch, validator_pl, big_igm):
    """
    Informational: cost of pl.from_pandas per call vs polars logic on an already converted table.
    model_validator calls 4-5 of these functions per IGM, each converting the full model again.
    """
    monkeypatch.setattr(validator_pl, "load_opdm_objects_to_triplets", lambda opdm_objects, **_: opdm_objects)
    t_convert = _best(lambda: pl.from_pandas(big_igm))
    native = pl.from_pandas(big_igm)
    per_function = {name: round(_best(lambda: call(validator_pl, native)), 4) for name, call in VALIDATOR_CALLS.items()}
    via_pandas = {name: round(_best(lambda: call(validator_pl, big_igm)), 4) for name, call in VALIDATOR_CALLS.items()}
    _results["validator.conversion_overhead"] = {
        "input_rows": len(big_igm), "pl.from_pandas_s": round(t_convert, 4),
        "polars_input_s": per_function, "pandas_input_s": via_pandas,
        "all_functions_convert_each_call_s": round(sum(via_pandas.values()), 4),
        "all_functions_convert_once_s": round(t_convert + sum(per_function.values()), 4),
    }
    RESULTS_FILE.parent.mkdir(exist_ok=True)
    existing = json.loads(RESULTS_FILE.read_text()) if RESULTS_FILE.exists() else {}
    existing.update(_results)
    RESULTS_FILE.write_text(json.dumps(existing, indent=2))


# ------------------------------------------------------------------------------ post-processing

def test_post_processing_chain_speed(post_processing_pl, post_processing_pd):
    from test_40_post_processing import _pipeline
    original, sv, ssh = pp_scenario.build()
    copies = 3000 * SCALE
    data = {"original": replicate(original, copies), "sv": replicate(sv, copies), "ssh": replicate(ssh, copies)}
    rows = sum(len(v) for v in data.values())
    pd_input = lambda: {k: v.astype(object).copy() for k, v in data.items()}  # noqa: E731
    pl_frames = {k: pl.from_pandas(v.astype(str)) for k, v in data.items()}
    t_pd = _best(lambda: _pipeline(post_processing_pd, pd_input()))
    t_pl = _best(lambda: _pipeline(post_processing_pl, dict(pl_frames)))
    _record("post_processing.chain", rows, t_pd, t_pl)


# ---------------------------------------------------------------------------------------- scaler

def test_scaler_speed(scaler_pl, scaler_pd):
    """Small grid: dominated by pypowsybl loadflows; checks the polars conversions add no overhead."""
    ac, dc = ac_schedules(300.0, 200.0), dc_schedules()
    t_pd = _best(lambda: run_scaler(scaler_pd, cgm_network(), ac, dc))
    t_pl = _best(lambda: run_scaler(scaler_pl, cgm_network(), ac, dc))
    _record("scaler.scale_balance (MicroGrid BE+NL, max iterations)", None, t_pd, max(t_pl, 1e-9) if t_pl else t_pl)
