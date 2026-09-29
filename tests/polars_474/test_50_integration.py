"""
Components working together through their real production entry points.

  1. run_post_merge_processing (polars) vs the pandas one, with only I/O mocked
  2. model_validator pre-LF / post-LF / pre-merge classes feeding validator_functions (polars)
  3. validator output (ac_net_position, sum_conform_load) -> Elastic metadata -> replacement
     ACNP filtering (polars + pandas round trip)
"""
import json

import pandas as pd
import polars as pl
import pypowsybl as pp
import pytest

import pp_scenario
from helpers import assert_same_triplets, cgmes_triplets, es_replacement_records, fake_query_data, \
    identity_get_content, triplets

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------- 1. run_post_merge_processing

def _patch_post_processing_io(monkeypatch, module, original, sv, ssh, to_object):
    """Replace OPDM loading, SV header update and SSH creation with the synthetic CGM."""
    conv = (lambda df: df.astype(object)) if to_object else (lambda df: df.copy())
    monkeypatch.setattr(module, "load_opdm_objects_to_triplets", lambda opdm_objects, **_: conv(original))
    monkeypatch.setattr(module.merge_functions, "update_merged_model_sv",
                        lambda sv_data, opdm_object_meta: conv(sv))
    monkeypatch.setattr(module.merge_functions, "create_updated_ssh",
                        lambda models_as_triplets, sv_data, opdm_object_meta, **_: (sv_data, conv(ssh), opdm_object_meta))


@pytest.mark.parametrize("additional_processing", [True, False])
def test_run_post_merge_processing_matches_pandas(monkeypatch, post_processing_pl, post_processing_pd,
                                                  additional_processing):
    import uuid, itertools
    original, sv, ssh = pp_scenario.build()
    results = {}
    for name, module, to_object in [("pl", post_processing_pl, False), ("pd", post_processing_pd, True)]:
        counter = itertools.count()
        monkeypatch.setattr(uuid, "uuid4", lambda: uuid.UUID(int=next(counter)))
        _patch_post_processing_io(monkeypatch, module, original, sv, ssh, to_object)
        results[name] = module.run_post_merge_processing(input_models=[], exported_model=b"", opdm_object_meta={},
                                                         additional_processing=additional_processing)
    sv_pl, ssh_pl, _ = results["pl"]
    sv_pd, ssh_pd, _ = results["pd"]
    assert isinstance(sv_pl, pd.DataFrame) and isinstance(ssh_pl, pd.DataFrame), "caller expects pandas back"
    assert_same_triplets(sv_pl, sv_pd)
    assert_same_triplets(ssh_pl, ssh_pd[ssh_pd["KEY"].astype(str) != "index"])


@pytest.mark.config
@pytest.mark.parametrize("fix,threshold", [("False", "0.1"), ("True", "100")])
def test_run_post_merge_processing_respects_injection_config(monkeypatch, post_processing_pl, fix, threshold):
    """FIX_INJECTION_ERRORS / INJECTION_THRESHOLD from post_processing.properties are honoured."""
    original, sv, ssh = pp_scenario.build()
    _patch_post_processing_io(monkeypatch, post_processing_pl, original, sv, ssh, False)
    monkeypatch.setattr(post_processing_pl, "FIX_INJECTION_ERRORS", fix)
    monkeypatch.setattr(post_processing_pl, "INJECTION_THRESHOLD", threshold)
    _, ssh_out, _ = post_processing_pl.run_post_merge_processing([], b"", {}, additional_processing=False)
    power = ssh_out.query("ID == 'ES1' and KEY == 'EnergySource.activePower'")["VALUE"].astype(float).tolist()
    assert power == [30.0], "injection must stay untouched when fixing is off or mismatch is below threshold"


@pytest.mark.config
def test_run_post_merge_processing_small_island_limit(monkeypatch, post_processing_pl):
    original, sv, ssh = pp_scenario.build()
    _patch_post_processing_io(monkeypatch, post_processing_pl, original, sv, ssh, False)
    monkeypatch.setattr(post_processing_pl, "SMALL_ISLAND_SIZE", "1")
    sv_out, _, _ = post_processing_pl.run_post_merge_processing([], b"", {}, additional_processing=False)
    assert "ISL_SMALL" in set(sv_out["ID"]), "island of 2 nodes must survive a limit of 1"


# ------------------------------------------------------------------ 2. model validator classes

@pytest.fixture(scope="module")
def be_triplets():
    network = pp.network.create_micro_grid_be_network()
    pp.loadflow.run_ac(network)
    return network, cgmes_triplets(network)


def test_pre_lf_validator_with_switch_check_enabled(monkeypatch, be_triplets):
    from emf.model_validator import model_validator
    monkeypatch.setattr(model_validator, "CHECK_NON_RETAINED_SWITCHES", "True")
    validator = model_validator.PreLFValidator(network=be_triplets[1])
    validator.run_validation()
    assert validator.report["pre_validations"]["non_retained_switches"] is True


@pytest.mark.parametrize("tso", ["AST", "DKW"])
def test_pre_merge_modifications_run_end_to_end(monkeypatch, be_triplets, tso):
    """Pre-merge chain uses validator_functions (polars) and must return a pandas triplet table."""
    from emf.model_validator import model_validator
    from test_30_validator import dk_region_model, switch_model
    data = pd.concat([be_triplets[1], dk_region_model(), switch_model()], ignore_index=True)
    # Header-from-filename needs OPDM file names in the FullModel header (not present in pypowsybl
    # exports) and does not touch any polars code -> stubbed.
    monkeypatch.setattr(model_validator.TemporaryPreMergeModifications, "update_header_from_file_name",
                        lambda self: None)
    modifier = model_validator.TemporaryPreMergeModifications(network=data, tso=tso)
    result = modifier.run_pre_process_modifications()
    assert isinstance(result, pd.DataFrame)
    report = modifier.report["modification"]
    assert report["open_non_retained_switches"] is True
    assert report.get("update_region_names", False) is (tso == "DKW")
    opened = result[(result.ID == "S1") & (result.KEY == "Switch.open")]["VALUE"].tolist()
    assert opened == ["true"]


@pytest.mark.xfail(reason="pre-existing: PostLFValidator passes triplets, validator expects OPDM objects; "
                          "fixed on unmerged branch fix-model-validator (9088148)", strict=True)
def test_post_lf_kirchhoff_check_when_enabled(monkeypatch, be_triplets):
    from emf.model_validator import model_validator
    network, data = be_triplets
    validator = model_validator.PostLFValidator(network=network, network_triplets=data)
    validator.report = {"validations": {}}
    validator.validate_kirchhoff_first_law()
    assert "kirchhoff_first_law" in validator.report["validations"]


# ------------------------------------------------ 3. validator metadata -> replacement filtering

def test_validator_metadata_is_json_and_usable_by_replacement(monkeypatch, validator_pl, replacement_pl):
    """
    model_validator stores get_ac_net_position / get_sum_of_loads on the OPDM object, which goes
    to Elasticsearch as JSON and later drives filter_replacements_by_acnp during replacement.
    polars scalars (or numpy types) here would break the JSON write or the numeric filter.
    """
    from test_30_validator import acnp_model, loads_model
    acnp = validator_pl.get_ac_net_position(acnp_model())
    load = validator_pl.get_sum_of_loads(loads_model())
    assert isinstance(acnp, float) and isinstance(load, float)  # np.float64 is a float subclass
    assert not isinstance(acnp, pl.Series) and not isinstance(load, pl.Series)
    json.dumps({"ac_net_position": acnp, "sum_conform_load": load})

    records = es_replacement_records(seed=21)
    for r in records:
        r["ac_net_position"], r["sum_conform_load"] = acnp, load
    monkeypatch.setattr(replacement_pl, "query_data", fake_query_data(records))
    monkeypatch.setattr(replacement_pl, "get_content", identity_get_content)
    kept = replacement_pl.find_replacement_models(["AST"], "2D", "2026-09-29T10:30:00Z",
                                                  acnp_dict={"AST": acnp + 10}, acnp_threshold=200,
                                                  conform_load_factor=0.2)
    dropped = replacement_pl.find_replacement_models(["AST"], "2D", "2026-09-29T10:30:00Z",
                                                     acnp_dict={"AST": acnp + 500}, acnp_threshold=200,
                                                     conform_load_factor=0.2)
    assert len(kept) == 1 and dropped == []
