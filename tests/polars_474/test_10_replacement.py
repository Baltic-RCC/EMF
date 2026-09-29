"""
Replacement (emf/model_merger/replacement.py) - polars since d567584.

Oracle: pandas replacement.py at d567584^. Elasticsearch (query_data) and MinIO (get_content)
are replaced with in-memory fakes so both implementations see identical documents.
"""
import copy

import pytest

from helpers import es_replacement_records, fake_query_data, identity_get_content

HORIZONS = ["ID", "1D", "2D", "WK", "MO", "YR"]
TSOS = ["AST", "ELERING", "LITGRID", "PSE"]


@pytest.fixture
def patch_storage(monkeypatch, replacement_pl, replacement_pd):
    """Point both implementations at the same fake Elastic/MinIO."""

    def _apply(records):
        for module in (replacement_pl, replacement_pd):
            monkeypatch.setattr(module, "query_data", fake_query_data(records))
            monkeypatch.setattr(module, "get_content", identity_get_content)

    return _apply


def _ids(models):
    return sorted(m["opde:Id"] for m in models)


# ------------------------------------------------------------------------------------ parity

@pytest.mark.parity
@pytest.mark.parametrize("seed", range(12))
@pytest.mark.parametrize("horizon", HORIZONS)
def test_selection_matches_pandas(patch_storage, replacement_pl, replacement_pd, seed, horizon):
    """Same Elastic documents -> the same model picked per TSO (4-step cascade + tie-breaks)."""
    records = es_replacement_records(seed)
    patch_storage(records)
    kwargs = dict(tso_list=TSOS, time_horizon=horizon, scenario_date="2026-09-29T10:30:00Z")
    assert _ids(replacement_pl.find_replacement_models(**kwargs)) == \
        _ids(replacement_pd.find_replacement_models(**kwargs))


@pytest.mark.parity
@pytest.mark.parametrize("scenario_date", ["2026-09-28T00:30:00Z", "2026-09-27T23:30:00Z",
                                           "2026-09-21T12:30:00Z", "2026-03-29T01:30:00Z",
                                           "2026-01-05T09:30:00Z"])
def test_selection_matches_pandas_across_hours_and_days(patch_storage, replacement_pl, replacement_pd,
                                                        scenario_date):
    """Hour/day priority tables differ per weekday and hour, incl. DST night and a January MO case."""
    records = es_replacement_records(seed=7, anchor=scenario_date, days_back=40)
    patch_storage(records)
    for horizon in HORIZONS:
        kwargs = dict(tso_list=TSOS, time_horizon=horizon, scenario_date=scenario_date)
        assert _ids(replacement_pl.find_replacement_models(**kwargs)) == \
            _ids(replacement_pd.find_replacement_models(**kwargs)), horizon


@pytest.mark.parity
def test_exclusion_of_existing_models_matches_pandas(patch_storage, replacement_pl, replacement_pd):
    records = es_replacement_records(seed=3)
    patch_storage(records)
    existing = [r for r in records if r["pmd:TSO"] in ("AST", "PSE")][:25]
    kwargs = dict(tso_list=TSOS, time_horizon="2D", scenario_date="2026-09-29T10:30:00Z",
                  existing_models=copy.deepcopy(existing))
    selected = replacement_pl.find_replacement_models(**kwargs)
    assert _ids(selected) == _ids(replacement_pd.find_replacement_models(**kwargs))
    existing_keys = {(m["pmd:TSO"], m["pmd:scenarioDate"], m["pmd:timeHorizon"]) for m in existing}
    assert not [m for m in selected
                if (m["pmd:TSO"], m["pmd:scenarioDate"], m["pmd:timeHorizon"]) in existing_keys]


@pytest.mark.parity
@pytest.mark.parametrize("threshold,factor", [(200, 0.2), (50, 0.05), (1000, 1.0)])
def test_acnp_filter_matches_pandas(patch_storage, replacement_pl, replacement_pd, threshold, factor):
    """filter_replacements_by_acnp is pandas; the polars path round-trips through it."""
    records = es_replacement_records(seed=11)
    patch_storage(records)
    acnp = {"AST": 100.0, "ELERING": -300.0, "LITGRID": 0.0}  # PSE deliberately absent
    kwargs = dict(tso_list=TSOS, time_horizon="ID", scenario_date="2026-09-29T10:30:00Z",
                  acnp_dict=acnp, acnp_threshold=threshold, conform_load_factor=factor)
    assert _ids(replacement_pl.find_replacement_models(**kwargs)) == \
        _ids(replacement_pd.find_replacement_models(**kwargs))


@pytest.mark.parity
def test_replacement_table_priorities_match_pandas(replacement_pl, replacement_pd):
    """Priority columns per row (not just the final pick) are identical."""
    import pandas as pd
    import polars as pl
    records = es_replacement_records(seed=5)
    cfg = replacement_pl.replacement_config
    target = "2026-09-29T10:30:00Z"
    fields = ["opde:Id", "pmd:TSO", "pmd:scenarioDate", "pmd:timeHorizon", "pmd:versionNumber",
              "pmd:creationDate"]
    pl_df = replacement_pl.create_replacement_table(target, "ID", pl.DataFrame([{k: r[k] for k in fields}
                                                                                for r in records]), cfg)
    pd_df = replacement_pd.create_replacement_table(target, "ID", pd.DataFrame(records)[fields].copy(), cfg)
    cols = ["opde:Id", "priority_business", "priority_hour", "priority_day"]
    left = sorted(tuple(int(v) if isinstance(v, float) else v for v in row)
                  for row in pl_df.select(cols).iter_rows())
    right = sorted(tuple(int(v) if isinstance(v, float) else v for v in row)
                   for row in pd_df[cols].itertuples(index=False))
    assert left == right


# ------------------------------------------------------------------------------ functionality

def test_returns_untouched_elastic_documents(patch_storage, replacement_pl):
    """Winning rows map back to the original ES document: no helper columns leak to get_content."""
    records = es_replacement_records(seed=1)
    patch_storage(records)
    selected = replacement_pl.find_replacement_models(TSOS, "2D", "2026-09-29T10:30:00Z")
    by_id = {r["opde:Id"]: r for r in records}
    assert selected
    for model in selected:
        assert model == by_id[model["opde:Id"]]
        assert not {"priority_hour", "priority_day", "priority_business", "normalized_time_horizon",
                    "__row_idx", "target_time_horizon"} & set(model)


def test_one_model_per_tso(patch_storage, replacement_pl):
    patch_storage(es_replacement_records(seed=2))
    selected = replacement_pl.find_replacement_models(TSOS, "ID", "2026-09-29T10:30:00Z")
    tsos = [m["pmd:TSO"] for m in selected]
    assert len(tsos) == len(set(tsos))


def test_step1_preferred_over_other_steps(patch_storage, replacement_pl):
    """Same horizon + same day beats a newer/higher version from another step."""
    base = {"valid": True, "data-source": "OPDM", "ac_net_position": 0.0, "sum_conform_load": 1000.0,
            "pmd:TSO": "AST"}
    records = [
        {**base, "opde:Id": "step1", "pmd:scenarioDate": "2026-09-29T10:30:00Z", "pmd:timeHorizon": "1D",
         "pmd:versionNumber": "001", "pmd:creationDate": "2026-09-28T10:00:00.000Z"},
        {**base, "opde:Id": "step2", "pmd:scenarioDate": "2026-09-29T10:30:00Z", "pmd:timeHorizon": "2D",
         "pmd:versionNumber": "009", "pmd:creationDate": "2026-09-28T11:00:00.000Z"},
        {**base, "opde:Id": "step3", "pmd:scenarioDate": "2026-09-28T10:30:00Z", "pmd:timeHorizon": "1D",
         "pmd:versionNumber": "009", "pmd:creationDate": "2026-09-28T12:00:00.000Z"},
    ]
    patch_storage(records)
    assert _ids(replacement_pl.find_replacement_models(["AST"], "1D", "2026-09-29T10:30:00Z")) == ["step1"]


def test_full_tie_keeps_first_document_and_warns(patch_storage, replacement_pl, replacement_pd, caplog):
    base = {"valid": True, "data-source": "OPDM", "pmd:TSO": "AST", "pmd:scenarioDate": "2026-09-29T10:30:00Z",
            "pmd:timeHorizon": "1D", "pmd:versionNumber": "001", "pmd:creationDate": "2026-09-28T10:00:00.000Z",
            "ac_net_position": 0.0, "sum_conform_load": 1.0}
    records = [{**base, "opde:Id": "first"}, {**base, "opde:Id": "second"}]
    patch_storage(records)
    args = (["AST"], "1D", "2026-09-29T10:30:00Z")
    assert _ids(replacement_pl.find_replacement_models(*args)) == ["first"]
    assert _ids(replacement_pd.find_replacement_models(*args)) == ["first"]
    assert "Replacement filtering unreliable for: 'AST'" in caplog.text


# ----------------------------------------------------------------------- errors / missing input

@pytest.mark.errors
def test_empty_tso_list(replacement_pl):
    assert replacement_pl.find_replacement_models([], "1D", "2026-09-29T10:30:00Z") == []


@pytest.mark.errors
def test_unparsable_scenario_date(patch_storage, replacement_pl):
    patch_storage(es_replacement_records(seed=0))
    assert replacement_pl.find_replacement_models(TSOS, "1D", "not-a-date") == []


@pytest.mark.errors
def test_no_documents_in_elastic(patch_storage, replacement_pl, caplog):
    patch_storage([])
    assert replacement_pl.find_replacement_models(TSOS, "2D", "2026-09-29T10:30:00Z") == []
    assert "No replacement models found in Elastic" in caplog.text


@pytest.mark.errors
def test_query_failure_for_one_tso_does_not_block_others(monkeypatch, replacement_pl):
    records = es_replacement_records(seed=4)
    good = fake_query_data(records)

    def flaky(query, query_filter=None, *a, **k):
        if "ELERING" in query["pmd:TSO.keyword"]:
            raise ConnectionError("elastic down for this shard")
        return good(query, query_filter)

    monkeypatch.setattr(replacement_pl, "get_content", identity_get_content)
    args = (TSOS, "2D", "2026-09-29T10:30:00Z")
    monkeypatch.setattr(replacement_pl, "query_data", good)
    baseline = {m["pmd:TSO"] for m in replacement_pl.find_replacement_models(*args)}
    assert len(baseline) >= 3, "test data must produce replacements for most TSOs"
    monkeypatch.setattr(replacement_pl, "query_data", flaky)
    selected = {m["pmd:TSO"] for m in replacement_pl.find_replacement_models(*args)}
    assert selected == baseline - {"ELERING"}


@pytest.mark.errors
def test_list_valued_field_drops_only_that_document(patch_storage, replacement_pl, caplog):
    """ES multi-value fields come back as lists; that document is dropped, the rest still work."""
    records = es_replacement_records(seed=6)
    records[0]["pmd:timeHorizon"] = ["1D", "2D"]
    records[1]["pmd:scenarioDate"] = ["2026-09-29T10:30:00Z"]
    patch_storage(records)
    selected = replacement_pl.find_replacement_models(TSOS, "2D", "2026-09-29T10:30:00Z")
    assert {m["pmd:TSO"] for m in selected} == set(TSOS)
    assert records[0]["opde:Id"] not in _ids(selected) and records[1]["opde:Id"] not in _ids(selected)
    assert "is a list" in caplog.text


@pytest.mark.errors
@pytest.mark.parametrize("field,bad_value", [
    ("pmd:versionNumber", 3),           # int in one document, "00x" strings in the others
    ("ac_net_position", "123.4"),       # string in one document, floats in the others
    ("sum_conform_load", "n/a"),
])
def test_mixed_value_types_across_documents(patch_storage, replacement_pl, replacement_pd, field, bad_value):
    """
    pandas builds an object column and carries on per TSO. polars builds ONE frame for all TSOs
    with strict dtypes, so a single odd document must not wipe out replacement for every TSO.
    """
    records = es_replacement_records(seed=8)
    victim = next(r for r in records if r["pmd:TSO"] == "PSE")
    victim[field] = bad_value
    patch_storage(records)
    args = (TSOS, "2D", "2026-09-29T10:30:00Z")
    expected_tsos = {m["pmd:TSO"] for m in replacement_pd.find_replacement_models(*args)}
    got_tsos = {m["pmd:TSO"] for m in replacement_pl.find_replacement_models(*args)}
    assert got_tsos == expected_tsos, (
        f"one document with {field}={bad_value!r} changed the result: pandas {sorted(expected_tsos)}, "
        f"polars {sorted(got_tsos)}")


@pytest.mark.errors
def test_missing_optional_fields(patch_storage, replacement_pl):
    """Old documents without ACNP/conform-load metadata must still be usable."""
    records = es_replacement_records(seed=9)
    for r in records:
        r.pop("ac_net_position")
        r.pop("sum_conform_load")
    patch_storage(records)
    assert len(replacement_pl.find_replacement_models(TSOS, "2D", "2026-09-29T10:30:00Z")) == len(TSOS)


@pytest.mark.errors
def test_unknown_time_horizon(patch_storage, replacement_pl):
    patch_storage(es_replacement_records(seed=0))
    assert replacement_pl.find_replacement_models(TSOS, "XX", "2026-09-29T10:30:00Z") == []


@pytest.mark.errors
def test_all_candidates_filtered_out_by_acnp(patch_storage, replacement_pl, caplog):
    patch_storage(es_replacement_records(seed=10))
    selected = replacement_pl.find_replacement_models(
        TSOS, "2D", "2026-09-29T10:30:00Z", acnp_dict={t: 99999.0 for t in TSOS}, acnp_threshold=1,
        conform_load_factor=0.0001)
    assert selected == []
    assert "were all filtered out" in caplog.text


# ------------------------------------------------------------------ integration: run_replacement

@pytest.mark.integration
def test_run_replacement_forced_and_missing(patch_storage, replacement_pl):
    """Forced replacement removes the TSO's current model; missing TSOs are filled; report updated."""
    from emf.common.helpers.opdm_objects import DataSource
    from emf.model_merger.merge_functions import MergedModel

    records = es_replacement_records(seed=12)
    patch_storage(records)
    current = [{"pmd:TSO": "AST", "data-source": DataSource.OPDM, "opde:Id": "current-ast",
                "pmd:scenarioDate": "2026-09-29T10:30:00Z", "pmd:timeHorizon": "1D"}]
    merged = MergedModel()
    result = replacement_pl.run_replacement(
        igm_models=current, model_replacement=True, local_import_models=[], missing_local_import=[],
        missing_models=["ELERING", "LITGRID"], replace_tso=["AST"], time_horizon="2D",
        scenario_datetime="2026-09-29T10:30:00Z", merged_model=merged)
    ids = [m["opde:Id"] for m in result]
    assert "current-ast" not in ids
    assert sorted(m["pmd:TSO"] for m in result) == ["AST", "ELERING", "LITGRID"]
    assert merged.replaced is True
    assert len(merged.replaced_entity) == 3
    assert all(e["quality_indicator"] == "Substituted" for e in merged.replaced_entity)
