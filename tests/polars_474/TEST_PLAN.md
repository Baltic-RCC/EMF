# Test plan and report: #474 polars migration

Issue: https://github.com/Baltic-RCC/EMF/issues/474, "Investigate using Polars for optimized network modifications" (closed).
Tested revision: `dev` at `b94156b` (2026-09-28, merge of PR #524). The `main` status is summarised in section 7.
Environment: Python 3.11, dependencies installed from `uv.lock` (polars 1.43.2, pandas 3.0.3, pypowsybl 1.16.1, triplets 0.1.0, pyarrow 24.0.0), the same way `docker/Dockerfile` builds the image.

## 1. Goal and exit criteria

#474 is complete when every module it touched (a) gives the same results as the pandas code it replaced, or differs only in documented, intended ways, (b) still works with its real callers, (c) handles missing and malformed input no worse than before, (d) is faster, and (e) is installable from the project's declared dependencies.

In practice, #474 can be considered done when the whole suite passes, strict xfails are allowed only for the pre‑existing defects listed in section 6.3, and the performance run shows no production path slower than pandas.

## 2. Where #474 was applied

| Module | Commits / PR | Polars functions | Production callers |
|---|---|---|---|
| `emf/model_merger/replacement.py` | 78af1f3, d567584 (#512) | `create_replacement_table`, `_select_best_replacement_models`, `_exclude_existing_models`, `find_replacement_models` | `model_merger.py` → `run_replacement` |
| `emf/model_merger/scaler.py` | 78af1f3, d567584 (#512); fixes bd6e400, 5ba4db5 | `scale_balance` and all helpers | `model_merger.py:336` → merge report → `merge_functions.lvl8_report_cgm` → QAR; upload gate `model_merger.py:410` |
| `emf/model_validator/validator_functions.py` | 09f5ebc, 54281cc; 3367dcc (ACNP rebuild) | Kirchhoff check, non‑retained switches, `get_ac_net_position`, `get_sum_of_loads`, DK region fix | `model_validator.py` Pre‑LF, Post‑LF, pre‑merge modifications; ACNP and load metadata to Elastic, later read by replacement |
| `emf/model_merger/post_processing.py` | ed9a14f, 8b78b30, 08a9947 (PR #524, branch `post_processing_polars`) | all SV/SSH fix functions | `model_merger.py:388` → `run_post_merge_processing` (pandas in, polars inside, pandas out) |
| `emf/common/helpers/opdm_objects.py` | a737925 | `get_opdm_data_from_models` accepts polars | post_processing |

Branch `474_post_processing_pl` from the issue comments no longer exists; its work arrived through PR #524. The side copies `replacement_pl.py` and `scaler_pl.py` were deleted in a1266db. Branch `fix-model-validator` (9088148) holds validator fixes that are not merged into `dev`.

## 3. Approach

**Oracle.** For each module, the last pandas version was extracted from git into `oracle/` (`d567584^` for replacement and scaler, `09f5ebc^` for the validator, `b94156b^1` for post‑processing). Every test runs both versions on identical input and compares results. `get_ac_net_position` was rewritten on purpose (3367dcc), so it is checked against hand‑computed values instead.

**Test data.** No Elastic, MinIO, OPDM or RabbitMQ is needed; the clients are replaced in `conftest.py`.

| Area | Data |
|---|---|
| Replacement | Deterministic synthetic Elasticsearch documents: 4 TSOs, 30 time‑horizon codes, 16 days of history, plus month‑ahead candidates |
| Scaler | pypowsybl CGMES MicroGrid BE + NL merged with subnetworks, `isHvdc`/EIC properties and ConformLoad `detail` extensions |
| Validator | MicroGrid BE and NL exported to CGMES and parsed with triplets, plus small synthetic models for each rule |
| Post‑processing | A synthetic two‑IGM CGM (`pp_scenario.py`) where each function has at least one case that triggers it (checked by `test_function_changes_something`) |

Triplet tables use the same pyarrow dtypes that `read_RDF` produces in production.

**Categories (pytest markers).**

| Your requirement | Marker / file | Tests |
|---|---|---|
| Collect all places where it is applied | `inventory`, `test_00_inventory.py` | 20 |
| Functionality equal to pandas | `parity`, `test_10`–`test_40` | 148 |
| Components working together | `integration`, `test_50_integration.py` plus integration cases in `test_10`/`test_20` | 17 |
| Errors, missing input | `errors` | 41 |
| Configuration | `config`, `test_60_config.py` plus config cases in `test_20`/`test_50` | 22 |
| Performance | `performance`, `test_70_performance.py` (excluded by default) | 7 |
| Specified behaviour (no marker) | spread across files | 17 |

A test can carry two markers (e.g. `integration` + `config`), so the column adds up to more than the 269 tests.

## 4. What each module is tested for

**Replacement.** Model picked per TSO equals pandas for 12 data seeds × 6 time horizons, 5 scenario dates (weekday/weekend, midnight, DST night, a January MO case), exclusion of existing models, the ACNP filter at 3 threshold/factor settings, and per‑row priority columns. Also checked: 4‑step cascade preference, full‑tie behaviour and warning, untouched Elastic documents returned, and one model per TSO. Error cases: empty TSO list, bad date, empty Elastic, a query failure for one TSO, list‑valued fields, mixed value types, missing optional fields, unknown horizon, and everything filtered out. Integration: `run_replacement` with forced and missing TSOs updates the merged model.

**Scaler.** On the real merged network, compared with pandas: loads and boundary setpoints after scaling, the `scaled` flag, `scaled_entity` and `scaled_hvdc` for 4 scenarios (reachable, large shift, inconsistent targets, diverging), an HVDC link, duplicate and `'NaN'` schedules, and the `debug=True` diagnostics path. Configuration: `MAX_ITERATION` 1 and 3, `BALANCE_THRESHOLD` 0 and 10, `CONSTANT_POWER_FACTOR=True`, `POWER_FACTOR_THRESHOLD=0.5`. Error cases: no subnetworks, missing AC schedule, HVDC without a value, divergence after ACNP alignment, network without `isHvdc`. Integration: failed areas reaching the CGM QAR are real areas.

**Validator.** Kirchhoff (nodes‑only and full, with and without SvInjection) on the BE and NL exports and on a synthetic node, non‑retained switches (check and open), sum of loads for 3 parameter names, the DK region fix, ACNP (AC/DC/HVDC description, both enum spellings), and pandas vs polars inputs. Error cases: triplets passed directly, missing tables, unparsable values, no loads, no terminals, no switches, no regions.

**Post‑processing.** All 14 migrated functions are compared with pandas individually (15 cases; the injection check runs for two injection types), and the full production chain is compared with `additional_processing` and `FIX_INJECTION_ERRORS` on and off. `run_post_merge_processing` itself is run against pandas with only I/O mocked, including `INJECTION_THRESHOLD`, `FIX_INJECTION_ERRORS` and `SMALL_ISLAND_SIZE`. Every function is also run on a model that contains none of the objects it looks for.

**Configuration.** Dependencies are declared and locked, the installed polars matches the lock, the polars lower bound covers the API used, the triplets polars engine is present, `requirements.txt` agrees with the lock, the properties files read by migrated code parse, and `replacement_conf.json` covers all horizons, hours and days.

## 5. How to run

In PyCharm:

1. Create the interpreter from the lock file: `uv venv --python 3.11 && uv sync --frozen` (or `uv export --frozen --no-hashes -o req.txt` and then `pip install -r req.txt pytest`).
2. Do not use `requirements.txt`, which is outdated (finding F6).
3. Add a *pytest* run configuration with target `tests/polars_474` and working directory set to the repository root.
4. Performance tests: the same configuration with additional arguments `-m performance`.

Command line, from the repository root:

```
python -m pytest tests/polars_474                     # functional suite, about 1 minute
python -m pytest tests/polars_474 -m performance      # about 2 minutes, writes results/performance.json
python -m pytest tests/polars_474 -m "parity and not errors"   # any marker combination
```

Performance knobs are set through environment variables: `PERF_SCALE` (input size multiplier), `PERF_REPEAT` (runs per implementation, best is kept) and `PERF_TOLERANCE` (allowed slowdown factor, default 1.0).

## 6. Results on `dev` b94156b

Functional suite: 262 tests, **246 passed, 11 failed, 5 xfailed** (pre‑existing, strict). The JUnit output is in `results/junit.xml`.

### 6.1 Defects introduced by the migration

| ID | Severity | Where | What happens | Failing tests | Suggested fix |
|---|---|---|---|---|---|
| F1 | High | `scaler.py:730‑731` | The merge report loses `initial_offset_acnp` and `postscale_acnp` for every area. pandas took all rows of iteration 0 and of the last iteration (`.loc[[0, max]]`); polars keeps only the first and last row. `final_offset_acnp`, `success` and the network itself are correct. | `test_merge_report_scaled_entity_matches_pandas[*]` (3), `test_report_contains_all_documented_keys` | Select rows by `ITER` value: `filter(pl.col('ITER').is_in([0, max_iter]))`, then label by ITER instead of row index. |
| F2 | High | `replacement.py:415` | A single Elastic document whose `pmd:versionNumber`, `ac_net_position` or `sum_conform_load` has a different Python type from the rest (e.g. `3` among `"003"`) makes `pl.DataFrame(columns)` raise. The error is caught at line 446, so replacement silently returns nothing for **all** TSOs in the request. pandas returned replacements for all 4 TSOs in the same case. | `test_mixed_value_types_across_documents[*]` (3) | Build the frame with `strict=False` and an explicit schema (String for version and dates, Float64 via `cast(..., strict=False)` for ACNP and load). |
| F3 | Medium, dormant | `validator_functions.py:113` | An SvPowerFlow whose terminal is not in the models forms a null‑node group, which is reported as a Kirchhoff violation. pandas `groupby` dropped it. It is only reached with `CHECK_KIRCHHOFF_FIRST_LAW=True` (default False), and that path also crashes on P1. | `test_kirchhoff_ignores_flows_of_unknown_terminals_like_pandas` | Already in 9088148 on `fix-model-validator`; merge it into `dev`. |
| F4 | Low | `post_processing.py:227` | For duplicated boundary SvVoltages, a non‑numeric voltage (null after cast) sorts before valid ones and is kept, while the valid 401 kV row is removed. pandas raised `decimal.InvalidOperation` here, so this is better than before, but still wrong. | `test_duplicate_voltage_with_unparsable_value` | Sort key `pl.col("_v_numeric").is_null() \| (pl.col("_v_numeric") == 0)`. |
| F5 | Medium | `pyproject.toml` | `polars>=1.0`, but `DataFrame.join(maintain_order=...)` (`scaler.py:385`, `:623`) does not exist before polars 1.17.1 (checked by installing 1.0.0, 1.10.0, 1.12.0, 1.14.0, 1.16.0 and 1.17.1). The Docker image is not affected because it installs from `uv.lock`. | `test_polars_lower_bound_supports_used_api` | `polars>=1.17.1` |
| F6 | Medium | `requirements.txt` | The file is UTF‑16 encoded, has no polars, and pins pypowsybl 1.11.2, triplets 0.0.11 and pandas 2.2.2 against a lock with 1.16.1, 0.1.0 and 2.3.3/3.0.3. A PyCharm "install requirements" gives an environment where the migrated modules fail to import or behave differently. | `test_requirements_txt_consistent_with_lock` | Regenerate from the lock (`uv export --frozen --no-hashes -o requirements.txt`) or delete it. |
| F7 | Low, performance | `validator_functions.py` (`_as_polars` in every function) | `model_validator` passes pandas triplets, so each function converts the whole model again. With production dtypes, `get_sum_of_loads` is 3.6× slower than pandas. Running the five functions costs 0.98 s with a conversion per call and 0.35 s with one conversion (743 k rows). | `test_validator_speed[sum_of_loads]` (performance run) | Convert once in `model_validator.py` and pass the polars frame. |

### 6.2 Improvements over pandas (verified, now covered by tests)

The polars code fixes several pandas behaviours:

- **Stray triplet removed.** pandas `check_net_interchanges` wrote a stray `(<area>, 'index', 0)` triplet into the merged SSH; polars does not.
- **No crash on bad voltages.** pandas crashed the post‑merge step on a non‑numeric voltage; polars no longer does (F4 is what remains).
- **List‑valued Elastic fields.** These now drop only the affected document instead of the whole TSO.
- **Clean replacement documents.** Replacement returns the Elastic documents untouched, without helper columns.
- **Engine types preserved.** The validator functions accept both engines and preserve the input type where they return data.

### 6.3 Pre‑existing defects (strict xfail, not caused by #474)

| ID | Where | What | Status |
|---|---|---|---|
| P1 | `validator_functions.py:41`, called from `model_validator.py:131` | `get_nodes_against_kirchhoff_first_law` always calls `load_opdm_objects_to_triplets`, so the Post‑LF check crashes with `TypeError` on triplets when `CHECK_KIRCHHOFF_FIRST_LAW=True`. | Fixed on the unmerged branch `fix-model-validator` (9088148) |
| P2 | `modify_region_name_for_denmark` | Crashes with `AttributeError` when the model has no `ControlArea` or `GeographicalRegion`. Only runs for DKE/DKW. | Open |

## 7. Status per branch

| Branch | Polars modules | F1 | Fake `ITER` area in report | Notes |
|---|---|---|---|---|
| `main` (v1.9.0, f4be93e) | replacement, scaler, validator | yes | **yes** | Any scaling run needing 3 or more iterations gives `ITER` a `final_offset_acnp` above `BALANCE_THRESHOLD=2`, so `scaled=False` and the merged model is **not uploaded** (`model_merger.py:410`); the QAR also names a ControlArea "ITER". Post‑processing is still pandas. 82 of the suite's tests fail here. |
| `hotfix-v1.9.7`, `hotfix-v1.9.8` | replacement, scaler, validator | yes | fixed | |
| `dev` (b94156b) | all four | yes | fixed (bd6e400) | Results in section 6 |

## 8. Performance (`-m performance`, best of 3, this container)

| Operation | Input | pandas | polars | Speed‑up |
|---|---|---|---|---|
| Post‑processing full chain | 987 k triplets | 13.64 s | 1.85 s | 7.4× |
| Kirchhoff check (pandas input) | 743 k triplets | 1.16 s | 0.24 s | 4.8× |
| Non‑retained switches (pandas input) | 743 k triplets | 0.24 s | 0.18 s | 1.3× |
| Sum of loads (pandas input) | 743 k triplets | 0.043 s | 0.156 s | **0.28×** (F7) |
| Replacement selection | 30 k Elastic documents, 12 TSOs | 3.64 s | 2.01 s | 1.8× |
| Scaling, MicroGrid BE+NL | inconsistent targets, runs to `MAX_ITERATION`=15 | 1.31 s | 0.93 s | 1.4× |

Timings vary by about ±10 % between runs; ratios are stable. Re‑measure on the production worker with `PERF_SCALE` sized to a real pan‑European CGM before signing off.

## 9. Not covered here (manual or follow‑up)

1. `run_post_merge_processing` and the scaler on real IGMs and a real CGM. The comparison harness (`helpers.triplet_set`, `assert_same_triplets`) works unchanged on real data, but models cannot be committed to the repo.
2. A full merge in the Docker image through RabbitMQ, with the QAR validated against `QAR_v2.12.0.xsd`, comparing a `dev` merge against a v1.9.0 merge of the same timestamp.
3. Elastic mapping check: confirm the actual types of `pmd:versionNumber`, `ac_net_position` and `sum_conform_load` across the `emfos-opde-models` index, which tells how likely F2 is in production.
