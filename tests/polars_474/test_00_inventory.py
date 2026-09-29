"""
Step 1 - inventory: find every place where #474 was applied and check that it is wired in.

The ticket's work landed in four production modules (see TEST_PLAN.md, section 2). These tests
fail if a module silently falls back to pandas, if a leftover *_pl.py copy is reintroduced, or
if a caller still expects the old pandas interface.
"""
import ast
import inspect
from pathlib import Path

import polars as pl
import pytest

pytestmark = pytest.mark.inventory

MIGRATED_MODULES = {
    "emf/model_merger/replacement.py": ["create_replacement_table", "_select_best_replacement_models",
                                        "_exclude_existing_models", "find_replacement_models"],
    "emf/model_merger/scaler.py": ["scale_balance", "get_areas_losses", "get_areas_metrics",
                                   "get_network_elements_map_to_areas", "get_fragmented_areas_participation"],
    "emf/model_validator/validator_functions.py": ["get_nodes_against_kirchhoff_first_law",
                                                   "check_not_retained_switches_between_nodes",
                                                   "get_ac_net_position", "get_sum_of_loads",
                                                   "modify_region_name_for_denmark"],
    "emf/model_merger/post_processing.py": ["remove_small_islands", "remove_equivalent_shunt_section",
                                            "add_missing_sv_tap_steps", "check_and_fix_dependencies",
                                            "remove_duplicate_sv_voltages",
                                            "set_paired_boundary_injections_to_zero",
                                            "check_energized_boundary_nodes", "check_for_disconnected_terminals",
                                            "check_non_regulating_rotating_machine_q",
                                            "check_rotating_machine_q_outside_p_limits",
                                            "check_non_ltc_tap_changer_step", "check_net_interchanges",
                                            "check_non_boundary_equivalent_injections",
                                            "check_all_kind_of_injections", "run_post_merge_processing"],
}

# (caller file, callee expression that must appear there)
CALL_SITES = [
    ("emf/model_merger/model_merger.py", "scaler.scale_balance"),
    ("emf/model_merger/model_merger.py", "run_replacement"),
    ("emf/model_merger/model_merger.py", "post_processing.run_post_merge_processing"),
    ("emf/model_validator/model_validator.py", "validator_functions.check_not_retained_switches_between_nodes"),
    ("emf/model_validator/model_validator.py", "validator_functions.get_nodes_against_kirchhoff_first_law"),
    ("emf/model_validator/model_validator.py", "validator_functions.modify_region_name_for_denmark"),
    ("emf/model_validator/model_validator.py", "get_ac_net_position"),
    ("emf/model_validator/model_validator.py", "get_sum_of_loads"),
]


def _source(repo_root, rel):
    return (repo_root / rel).read_text(encoding="utf-8")


@pytest.mark.parametrize("rel_path", sorted(MIGRATED_MODULES))
def test_module_imports_polars(repo_root, rel_path):
    tree = ast.parse(_source(repo_root, rel_path))
    imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert "polars" in imported, f"{rel_path} does not import polars - migration missing"


@pytest.mark.parametrize("rel_path,functions", sorted(MIGRATED_MODULES.items()))
def test_migrated_functions_still_exist(repo_root, rel_path, functions):
    tree = ast.parse(_source(repo_root, rel_path))
    defined = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    missing = [f for f in functions if f not in defined]
    assert not missing, f"{rel_path}: functions removed or renamed: {missing}"


def test_no_leftover_parallel_polars_copies(repo_root):
    """Commit 78af1f3 added *_pl.py side copies; a1266db removed them. They must not return."""
    leftovers = [str(p.relative_to(repo_root)) for p in (repo_root / "emf").rglob("*_pl.py")]
    assert not leftovers, f"parallel polars copies found: {leftovers}"


@pytest.mark.parametrize("caller,callee", CALL_SITES)
def test_call_sites_present(repo_root, caller, callee):
    assert callee in _source(repo_root, caller), f"{caller} no longer calls {callee}"


def test_every_polars_module_in_emf_is_covered(repo_root):
    """Catches new polars usage added later that this suite does not cover yet."""
    covered = set(MIGRATED_MODULES) | {
        # helpers that only pass polars frames through (no dataframe logic of their own)
        "emf/common/helpers/opdm_objects.py",
    }
    found = set()
    for path in (repo_root / "emf").rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "import polars" in text:
            found.add(str(path.relative_to(repo_root)).replace("\\", "/"))
    uncovered = sorted(found - covered)
    assert not uncovered, f"polars used in modules without tests: {uncovered}"


def test_post_processing_returns_pandas_to_callers(post_processing_pl):
    """model_merger/merge_functions are still pandas; the boundary conversion must stay."""
    src = inspect.getsource(post_processing_pl.run_post_merge_processing)
    assert "pl.from_pandas(input_models_triplets)" in src
    assert "sv_data.to_pandas()" in src and "ssh_data.to_pandas()" in src


def test_validator_public_functions_return_pandas_where_callers_expect_it(validator_pl, monkeypatch):
    """model_validator.py uses `.empty` on Kirchhoff results -> must stay a pandas DataFrame."""
    import pandas as pd
    from helpers import triplets
    monkeypatch.setattr(validator_pl, "load_opdm_objects_to_triplets", lambda opdm_objects, **_: opdm_objects)
    empty = triplets([("x", "Type", "Foo")])
    result = validator_pl.get_nodes_against_kirchhoff_first_law(empty)
    assert isinstance(result, pd.DataFrame)
