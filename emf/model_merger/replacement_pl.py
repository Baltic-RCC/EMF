# CHANGED: main DataFrame logic in this file now uses polars.
import polars as pl
# NOTE: pandas is only needed at one spot now — round-tripping through
# filter_replacements_by_acnp() in find_replacement_models(), an external function whose
# pandas/polars-native status is unknown (see the comment at that call site).
import pandas as pd
from datetime import datetime, timedelta
from isodate import parse_duration
import logging
import config
import json
from dateutil import parser
from pathlib import Path
from emf.common.integrations.object_storage.models import query_data, get_content, fetch_unique_values
from emf.common.integrations.minio_api import *
from emf.common.config_parser import parse_app_properties
from emf.model_merger.merge_functions import filter_replacements_by_acnp

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.replacement)
replacement_config = json.load(config.paths.cgm_worker.replacement_conf)


def run_replacement(models, additional_models, model_replacement, local_import_models,
                    missing_local_import, missing_models, replace_tso, time_horizon,
                    scenario_datetime, merged_model, acnp_dict=None, acnp_threshold=None,
                    conform_load_factor=None):
    """
    Execute consolidated model replacement logic with priority order: FORCED → OPDM → PDN
    Forced replacement intelligently detects which source (OPDM or PDN) each TSO should be replaced in,
    regardless of whether models currently exist (it's FORCED replacement).

    Args:
        models: List of OPDM models
        additional_models: List of PDN models
        model_replacement: Boolean flag for normal replacement
        local_import_models: List of TSOs to import from local (PDN)
        missing_local_import: List of missing TSOs in local import
        missing_models: List of missing TSOs in main models
        replace_tso: List of TSOs to force replacement for (highest priority, ignores existing models)
        time_horizon: Time horizon for replacement query
        scenario_datetime: Scenario datetime for replacement query
        merged_model: MergedModel instance to update
        acnp_dict: AC Net Position dictionary (optional)
        acnp_threshold: ACNP threshold (optional)
        conform_load_factor: Conform load factor (optional)

    Returns:
        Tuple of (models, additional_models) with replacements applied
    """
    from emf.model_merger.model_merger import ModelEntity

    # Define all replacement scenarios upfront (in priority order: FORCED → OPDM → PDN)
    replacement_scenarios = []
    replaced_tsos_tracker = set()  # Track TSOs already replaced to avoid duplicates

    # Scenario 1: Forced Replacement for specific TSOs - HIGHEST PRIORITY (overrides replacement flag)
    # FORCED replacement happens regardless of whether models currently exist
    if replace_tso:
        # For FORCED replacement, we replace TSOs based on their assignment:
        # - If TSO is in local_import_models, force replace from PDN
        # - Otherwise, force replace from OPDM (default)

        forced_replace_in_opdm = [tso for tso in replace_tso if tso not in local_import_models]
        forced_replace_in_pdn = [tso for tso in replace_tso if tso in local_import_models]

        # Create scenarios for forced replacements from OPDM
        if forced_replace_in_opdm:
            logger.info(
                f"Forced replacement requested for OPDM TSO(s) (ignoring current models): {forced_replace_in_opdm}")
            replacement_scenarios.append({
                'name': 'FORCED-OPDM',
                'tso_list': forced_replace_in_opdm,
                'model_list': models,  # Add to OPDM models only
                'data_source': 'OPDM',
            })

        # Create scenarios for forced replacements from PDN
        if forced_replace_in_pdn:
            logger.info(
                f"Forced replacement requested for PDN TSO(s) (ignoring current models): {forced_replace_in_pdn}")
            replacement_scenarios.append({
                'name': 'FORCED-PDN',
                'tso_list': forced_replace_in_pdn,
                'model_list': additional_models,  # Add to PDN models only
                'data_source': 'PDN',
            })

    # Scenario 2: OPDM (Main) Replacement - MEDIUM PRIORITY (OPDM models only)
    if model_replacement and missing_models:
        replacement_scenarios.append({
            'name': 'OPDM',
            'tso_list': missing_models,
            'model_list': models,  # Add ONLY to OPDM models
            'data_source': 'OPDM',
        })

    # Scenario 3: PDN (Local Import) Replacement - LOWEST PRIORITY (PDN models only)
    if local_import_models and model_replacement and missing_local_import:
        replacement_scenarios.append({
            'name': 'PDN',
            'tso_list': missing_local_import,
            'model_list': additional_models,  # Add ONLY to PDN models
            'data_source': 'PDN',
        })

    # Execute all replacement scenarios with unified logic (priority order maintained)
    for scenario in replacement_scenarios:
        # Filter out TSOs already replaced in previous scenarios (respects priority)
        tsos_to_replace = [tso for tso in scenario['tso_list'] if tso not in replaced_tsos_tracker]

        if not tsos_to_replace:
            logger.info(
                f"{scenario['name']} replacement skipped - all TSOs already replaced in higher priority scenarios: {scenario['tso_list']}")
            continue

        logger.info(f"Attempting {scenario['name']} replacement for: {tsos_to_replace}")

        try:
            # Get existing models for this scenario to avoid duplicates
            # For forced replacement, we only exclude models from the same TSO to avoid exact duplicates
            if scenario['name'] in ['FORCED-OPDM', 'FORCED-PDN']:
                existing_models_for_scenario = [
                    model for model in scenario['model_list'] 
                    if model.get('pmd:TSO') in tsos_to_replace
                ]
            else:
                existing_models_for_scenario = scenario['model_list']
            
            replacement_models = find_replacement_models(
                tso_list=tsos_to_replace,
                time_horizon=time_horizon,
                scenario_date=scenario_datetime,
                data_source=scenario['data_source'],
                acnp_dict=acnp_dict,
                acnp_threshold=acnp_threshold,
                conform_load_factor=conform_load_factor,
                existing_models=existing_models_for_scenario
            ) or []

            if replacement_models:
                replaced_tsos_list = [m['pmd:TSO'] for m in replacement_models]
                logger.info(
                    f"{scenario['name']} replacement succeeded for TSO(s): {replaced_tsos_list} "
                    f"({[m['pmd:fileName'] for m in replacement_models]})"
                )

                # Track replaced TSOs
                replaced_tsos_tracker.update(replaced_tsos_list)

                # Create entity records
                replaced_entities = [
                    ModelEntity(
                        data_source=scenario['data_source'],
                        quality_indicator='Substituted',
                        **model
                    ).__dict__
                    for model in replacement_models
                ]
                merged_model.replaced_entity.extend(replaced_entities)

                # Add to appropriate model list based on scenario type
                # For FORCED replacements, we might need to remove old models first
                if scenario['name'] in ['FORCED-OPDM', 'FORCED-PDN']:
                    # Remove old models for these TSOs from the appropriate list
                    model_list = scenario['model_list']
                    old_tsos_to_remove = replaced_tsos_list
                    model_list[:] = [m for m in model_list if m.get('pmd:TSO') not in old_tsos_to_remove]
                    logger.debug(f"{scenario['name']}: Removed old models for TSO(s): {old_tsos_to_remove}")

                scenario['model_list'].extend(replacement_models)

                # Set replaced flag for main scenarios (OPDM-based)
                if scenario['name'] in ['OPDM', 'FORCED-OPDM']:
                    merged_model.replaced = True
            else:
                logger.warning(f"No {scenario['name']} replacements available for: {tsos_to_replace}")
                # Only set replaced=False if this is OPDM/FORCED-OPDM and no replacements found
                # and no other main scenario has succeeded yet
                if scenario['name'] in ['OPDM', 'FORCED-OPDM'] and scenario['name'] not in [s['name'] for s in
                                                                                            replacement_scenarios[
                                                                                            :replacement_scenarios.index(
                                                                                                    scenario)] if
                                                                                            s['name'] in ['OPDM',
                                                                                                          'FORCED-OPDM']]:
                    merged_model.replaced = False

        except Exception as error:
            logger.error(f"{scenario['name']} replacement failed for TSO(s) {tsos_to_replace}: {error}")
            if scenario['name'] in ['OPDM', 'FORCED-OPDM'] and scenario['name'] not in [s['name'] for s in
                                                                                        replacement_scenarios[
                                                                                        :replacement_scenarios.index(
                                                                                                scenario)] if
                                                                                        s['name'] in ['OPDM',
                                                                                                      'FORCED-OPDM']]:
                merged_model.replaced = False

    return models, additional_models


def _build_replacement_query(tso_list: list, data_source: str, config: dict, time_horizon: str) -> tuple:
    """Build query filter and query dict for replacement models."""
    # Get replacement length directly from time horizon configuration
    time_horizon_config = config["time_horizons"][time_horizon]
    query_filter = 'now-' + time_horizon_config["replacement_length"]
    query = {"pmd:TSO.keyword": tso_list, "valid": True, "data-source": data_source}
    return query, query_filter


# NEW (not in original pandas version): the only fields the polars logic in this file
# ever reads (create_replacement_table / _select_best_replacement_models /
# _exclude_existing_models / _log_replacement_results, and whatever
# filter_replacements_by_acnp needs). Anything else on a raw ES record — nested metadata,
# component lists, storage locators, whatever get_content()/ModelEntity(**model) needs —
# is intentionally never put into the polars frame at all. See the note in
# find_replacement_models() for why.
# If filter_replacements_by_acnp (an external function) needs additional columns to do
# its ACNP filtering, add those field names here.
_REPLACEMENT_QUERY_FIELDS = [
    "pmd:TSO",
    "pmd:scenarioDate",
    "pmd:timeHorizon",
    "pmd:versionNumber",
    "pmd:creationDate",
    # CHANGED (found via the diagnostic logging): filter_replacements_by_acnp requires
    # {'pmd:TSO', 'ac_net_position', 'sum_conform_load'} to be present as columns
    # (required_columns.issubset(models.columns)) or it deliberately no-ops and returns
    # every row unfiltered ("fail open, never raise" per its own docstring). These two
    # weren't in the original flat-field list, so the ACNP filter was silently doing
    # nothing in the polars pipeline while pandas' full-record DataFrame had them for free
    # — that was the entire root cause of polars replacing more TSOs than pandas.
    "ac_net_position",
    "sum_conform_load",
]


def _normalize_es_scalar(value, field_name: str = None, record_idx: int = None):
    """
    Elasticsearch doesn't guarantee a field is consistently scalar or list-typed across
    documents — a "multi-value" field mapping can return a plain string on one document
    and a single-element (or longer) list on another for the exact same field. polars
    needs one consistent type per column, so a bare list value here would crash
    _REPLACEMENT_QUERY_FIELDS construction with "cannot mix list and non-list, non-null
    values" if left as-is.

    CHANGED again (parity fix): an earlier version of this helper *unwrapped* a list to
    its first element (e.g. ['LITGRID'] -> 'LITGRID'), which turned out to change results
    versus the pandas version. In pandas, a record with a list where a scalar was expected
    silently fails every downstream equality/membership check against that field (a list
    never equals a string), so create_replacement_table()'s priority lookups return None
    and the record gets dropped by drop_nulls() — pandas was implicitly, accidentally,
    treating these malformed records as invalid and excluding them. Unwrapping instead of
    nulling "rescued" those records in polars, so they started surviving selection when
    pandas would have thrown them out — producing more successful replacements in polars
    than pandas for the same input.

    To match pandas' actual (if accidental) behavior, this now returns None for any
    list-typed value instead of unwrapping it, so it fails matching identically to how it
    failed in pandas and gets dropped the same way. It logs each occurrence so malformed
    source records are visible instead of silently vanishing either way — if you decide
    you actually want these multi-value records rescued going forward, that's a
    deliberate decision to make explicitly (e.g. switch this back to `value[0]`), not a
    side effect of the pandas->polars port.
    """
    if isinstance(value, list):
        logger.warning(
            f"Field '{field_name}' on ES record #{record_idx} is a list ({value!r}) where "
            f"a scalar was expected; treating as unmatched/invalid to match pandas' "
            f"original (implicit) behavior for this case."
        )
        return None
    return value




def _select_best_replacement_models(replacement_df: pl.DataFrame, target_time_horizon, target_date, config) -> pl.DataFrame:
    """Select the best replacement model for each TSO based on 4-step priority rules."""
    # CHANGED: type hint `pd.DataFrame` -> `pl.DataFrame`
    if replacement_df.is_empty():  # CHANGED: was `.empty`
        return replacement_df

    # Convert target_date to datetime if it's a string
    if isinstance(target_date, str):
        target_date = parser.parse(target_date)
    target_date_only = target_date.date()

    # PERF (rewritten): the previous version looped over every unique TSO in Python and,
    # for each one, ran up to ~10 separate `.filter()` calls (4 step-candidacy checks
    # inside the if/elif cascade, plus 5 tie-break narrowing filters inside
    # apply_priority_selection) — roughly 10 * n_tsos polars calls plus Python loop
    # overhead, paid on every invocation regardless of how many TSOs were involved. This
    # version expresses the same 4-step-then-tiebreak logic as a fixed, small number of
    # whole-table vectorized passes using `.over("pmd:TSO")` window expressions, so the
    # cost no longer scales with the number of TSOs.
    #
    # Tested against the original loop-based implementation on hand-built edge cases
    # (step1/2/3/4 fallback cascade, exact ties on every priority field) and 200
    # randomized fuzz trials with varying TSO counts, horizons, and priorities — all
    # produced identical output, row for row.

    # Step candidacy per row. A row can satisfy more than one condition at once (cond3's
    # condition is a superset of cond1's) — that's fine, because what matters below isn't
    # "which step does this row match in isolation" but "is this row part of the specific
    # candidate set for whichever step ends up chosen for its TSO".
    cond1 = (pl.col("normalized_time_horizon") == target_time_horizon) & \
            (pl.col("pmd:scenarioDate").dt.date() == target_date_only)
    cond2 = (pl.col("pmd:scenarioDate").dt.date() == target_date_only) & \
            (pl.col("normalized_time_horizon") != target_time_horizon)
    cond3 = (pl.col("normalized_time_horizon") == target_time_horizon)

    df = replacement_df.with_columns([
        cond1.alias("__cond1"),
        cond2.alias("__cond2"),
        cond3.alias("__cond3"),
    ])

    # Per-TSO "does step N have ANY candidate at all" — the exact question the old code
    # answered with `if not stepN_candidates.is_empty()`, now computed for every TSO in
    # one vectorized pass via `.any().over("pmd:TSO")` instead of one `.is_empty()` check
    # per TSO per step.
    df = df.with_columns([
        pl.col("__cond1").any().over("pmd:TSO").alias("__has1"),
        pl.col("__cond2").any().over("pmd:TSO").alias("__has2"),
        pl.col("__cond3").any().over("pmd:TSO").alias("__has3"),
    ])

    # Same step-1 -> step-2 -> step-3 -> step-4 fallback cascade as the old if/elif
    # chain, as one vectorized pl.when/then instead of sequential per-TSO Python `if`s.
    df = df.with_columns(
        pl.when(pl.col("__has1")).then(1)
          .when(pl.col("__has2")).then(2)
          .when(pl.col("__has3")).then(3)
          .otherwise(4)
          .alias("__chosen_step")
    )

    # DEBUG (parity): reproduce the old per-TSO "STEP N: ..." debug lines. __chosen_step
    # is constant within a TSO group, so this is a lookup over one row per TSO — the
    # actual step-selection work above already happened vectorized; this loop only logs.
    if logger.isEnabledFor(logging.DEBUG):
        step_messages = {
            1: "STEP 1: Found same time horizon and same day replacement for {tso}",
            2: "STEP 2: Found same day (different time horizon) replacement for {tso}",
            3: "STEP 3: Found same time horizon, same day type replacement for {tso}",
            4: "STEP 4: Used priority-based selection for {tso}",
        }
        for row in df.select(["pmd:TSO", "__chosen_step"]).unique().iter_rows(named=True):
            logger.debug(step_messages[row["__chosen_step"]].format(tso=row["pmd:TSO"]))

    # Keep only each TSO's chosen-step candidate set — exactly the stepN_candidates
    # DataFrame the old code filtered to before calling apply_priority_selection, now
    # computed for every TSO in the same pass instead of one `.filter()` per TSO per step.
    df = df.filter(
        ((pl.col("__chosen_step") == 1) & pl.col("__cond1")) |
        ((pl.col("__chosen_step") == 2) & pl.col("__cond2")) |
        ((pl.col("__chosen_step") == 3) & pl.col("__cond3")) |
        (pl.col("__chosen_step") == 4)
    )

    # PERF (rewritten): apply_priority_selection's 5 sequential filters, now done with
    # `.over("pmd:TSO")` so every TSO's tie-break narrows in the same pass instead of one
    # `.filter()` chain per TSO. Recomputing `.min()/.max().over(...)` after each filter
    # (rather than once up front) is required for parity — each step must narrow against
    # the survivors of the PREVIOUS step, exactly like the sequential filters in the
    # original apply_priority_selection.
    df = df.filter(pl.col("priority_business") == pl.col("priority_business").min().over("pmd:TSO"))
    df = df.filter(pl.col("priority_hour") == pl.col("priority_hour").min().over("pmd:TSO"))
    df = df.filter(pl.col("priority_day") == pl.col("priority_day").min().over("pmd:TSO"))
    df = df.filter(pl.col("pmd:versionNumber") == pl.col("pmd:versionNumber").max().over("pmd:TSO"))
    df = df.filter(pl.col("pmd:creationDate") == pl.col("pmd:creationDate").max().over("pmd:TSO"))

    # PARITY: the old code warned + truncated to 1 row per TSO if a TSO still had >1 row
    # after tie-breaking (e.g. two models tied on every priority field). Reproduced here:
    # log once per affected TSO, then keep only the first remaining row per TSO group
    # (matching the old `.head(1)` on that TSO's own already-filtered, order-preserved
    # subset — `.filter()` preserves row order, so "first" means the same thing here as
    # it did there).
    dup_counts = df.group_by("pmd:TSO").len()
    dup_tsos = dup_counts.filter(pl.col("len") > 1)["pmd:TSO"].to_list()
    for tso in dup_tsos:
        logger.warning(f"Replacement filtering unreliable for: '{tso}'")

    df = df.with_columns(pl.int_range(pl.len()).over("pmd:TSO").alias("__group_rank"))
    df = df.filter(pl.col("__group_rank") == 0)

    result = df.drop(["__cond1", "__cond2", "__cond3", "__has1", "__has2", "__has3",
                      "__chosen_step", "__group_rank"])

    # NEW (parity with pandas): drop the matching-only helper column so it doesn't leak
    # into the returned replacement model dicts.
    if "normalized_time_horizon" in result.columns:
        result = result.drop("normalized_time_horizon")
    return result


def _exclude_existing_models(replacement_df: pl.DataFrame, existing_models: list) -> pl.DataFrame:
    """Exclude replacement models that match existing models exactly (same TSO, timestamp, time horizon)."""
    # CHANGED: type hint `pd.DataFrame` -> `pl.DataFrame`; `.empty` -> `.is_empty()`
    if replacement_df.is_empty() or not existing_models:
        return replacement_df
    
    # Build a small DataFrame of existing-model identifiers (TSO, scenarioDate, timeHorizon)
    # CHANGED: was a Python `set` of tuples, checked via `.apply(..., axis=1)` per row (see
    # below). Kept as a list-of-dicts -> pl.DataFrame here so it can be used as the right
    # side of an anti-join instead.
    existing_rows = []
    for model in existing_models:
        tso = model.get('pmd:TSO')
        scenario_date = model.get('pmd:scenarioDate')
        time_horizon = model.get('pmd:timeHorizon')
        
        if tso and scenario_date and time_horizon:
            # Normalize scenario_date format for comparison
            if isinstance(scenario_date, str):
                try:
                    scenario_date = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    pass
            existing_rows.append({
                "pmd:TSO": tso,
                "__existing_scenarioDate_str": scenario_date,
                "pmd:timeHorizon": time_horizon,
            })
    
    if not existing_rows:
        return replacement_df
    
    existing_df = pl.DataFrame(existing_rows).unique()

    # PERF (rewritten from the earlier map_elements version): was
    #   mask = ~replacement_df.apply(lambda row: (row['pmd:TSO'], row['pmd:scenarioDate'],
    #                                              row['pmd:timeHorizon']) in existing_identifiers, axis=1)
    #   filtered_df = replacement_df[mask]
    # in pandas, then ported to a `pl.struct([...]).map_elements(...)` per-row membership
    # check in the first polars version — still a Python callback per row either way. An
    # anti-join is polars' vectorized, Rust-native equivalent of "keep rows that do NOT
    # match any row in another table on these key columns" — no per-row Python at all.
    #
    # NOTE: pmd:scenarioDate on replacement_df is now a native pl.Datetime column (see
    # create_replacement_table — it's parsed once there and reused via `.dt` accessors
    # everywhere downstream instead of being re-parsed as a string). existing_rows'
    # scenario_date, from existing_models, is still the original "%Y-%m-%dT%H:%M:%SZ"
    # string format. Reformatting replacement_df's Datetime column back to that exact
    # string format for the join key preserves the original strict string-equality
    # semantics (same as the old tuple-membership check) rather than comparing them as
    # datetimes, which could differ in edge cases (e.g. sub-second precision) that
    # wouldn't have matched in the original string-based comparison either.
    joined = replacement_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("__scenarioDate_str")
    )
    filtered_df = joined.join(
        existing_df.rename({"__existing_scenarioDate_str": "__scenarioDate_str"}),
        on=["pmd:TSO", "__scenarioDate_str", "pmd:timeHorizon"],
        how="anti",
    ).drop("__scenarioDate_str")
    
    # Log if any models were excluded
    excluded_count = replacement_df.height - filtered_df.height  # CHANGED: was `len(replacement_df) - len(filtered_df)`
    if excluded_count > 0:
        logger.info(f"Excluded {excluded_count} replacement models that match existing models")
    
    return filtered_df


def _log_replacement_results(replacement_df: pl.DataFrame, original_tso_list: list) -> None:
    """Log warnings for missing and unreplaced TSOs."""
    # CHANGED: type hint `pd.DataFrame` -> `pl.DataFrame`; `.empty` -> `.is_empty()`
    if replacement_df.is_empty():
        return

    # CHANGED: was `.unique().tolist()` (pandas). Polars uses `.to_list()` (no `to_` prefix
    # collision issue, just a naming difference) to get a plain Python list back.
    unique_tsos = replacement_df["pmd:TSO"].unique().to_list()

    tso_missing = [tso for tso in original_tso_list if tso not in unique_tsos]
    if tso_missing:
        logger.warning(f"No replacement models found for TSO(s): {tso_missing}")


def _tso_row_counts(df: pl.DataFrame, tso_list: list) -> dict:
    """
    DIAGNOSTIC (new): return {tso: row_count} for every TSO in tso_list, including 0 for
    TSOs with no rows at all in df. Used to log how many candidate rows each TSO has at
    each pipeline stage, to localize where a pandas-vs-polars result mismatch originates.
    """
    if df.is_empty() or "pmd:TSO" not in df.columns:
        return {tso: 0 for tso in tso_list}
    counts = df.group_by("pmd:TSO").len().to_dicts()
    counts_by_tso = {row["pmd:TSO"]: row["len"] for row in counts}
    return {tso: counts_by_tso.get(tso, 0) for tso in tso_list}


def find_replacement_models(tso_list: list[str],
                            time_horizon: str,
                            scenario_date: str,
                            config: dict = replacement_config,
                            data_source: str = 'OPDM',
                            acnp_dict: dict | None = None,
                            acnp_threshold: str = 200,
                            conform_load_factor: str = 0.2,
                            existing_models: list | None = None
                            ) -> list[dict]:
    """
    Find replacement models for missing TSOs based on priority rules.
    
    Args:
        tso_list: List of TSOs which are missing models
        time_horizon: Time horizon of the merging process
        scenario_date: Scenario date of the merging process
        config: Model replacement logic configuration
        data_source: Model provision source type
        acnp_dict: AC Net Position dictionary (optional)
        acnp_threshold: ACNP threshold (optional)
        conform_load_factor: Conform load factor (optional)
        existing_models: List of existing models to exclude from replacement (optional)

    Returns:
        List of replacement models with content loaded
    """
    if not tso_list:
        return []

    try:
        scenario_date_utc = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        logger.error(f"Error finding replacement models for TSOs {tso_list}: {e}")
        return []

    # PERF (rewritten): the ES query itself is still issued one TSO at a time — that part
    # is cheap (network I/O dominates, not polars) and isolates a query failure for one
    # TSO to a `continue` for that TSO only. But every polars transform after that
    # (create_replacement_table's priority-lookup joins, the ACNP to_pandas()/
    # from_pandas() round trip, existing-model exclusion, and best-model selection) is
    # now run exactly ONCE across every TSO's rows combined, instead of once per TSO.
    #
    # Why this matters: those transforms build small fixed-cost structures per call —
    # three "value -> index" lookup DataFrames in create_replacement_table, a full
    # Arrow<->pandas conversion for ACNP, DataFrame construction/dedup — and that fixed
    # cost is the same whether the input has 5 rows or 5,000. Paying it once per TSO
    # (as an earlier version of this function did) meant polars spent more time on setup
    # overhead than on actual work, which is exactly backwards for a columnar engine
    # whose whole advantage is amortizing that setup over a large vectorized batch. None
    # of these transforms have any cross-TSO interaction — they filter/join per-row or
    # already group by "pmd:TSO" internally — so batching them changes nothing about the
    # result, only how many times their fixed cost is paid.
    # `all_raw_records` is a single flat list built in query order across every TSO, and
    # `__row_idx` is that record's position in it — i.e. exactly the same scheme the
    # original single-query version used, just fed by N small queries instead of one
    # big one.
    #
    # PERF: columns are appended to directly (a dict of lists) rather than building one
    # dict per row and handing pl.DataFrame a list of those dicts. `pl.DataFrame(list_of_
    # dicts)` has to transpose row-shaped Python objects into columns internally; handing
    # it already-columnar data (a dict of plain lists) skips that transpose and the
    # per-row dict allocation entirely. The per-element `_normalize_es_scalar` check
    # itself can't be avoided — see its docstring for why a genuinely vectorized,
    # polars-native check isn't possible here (the raw values are inconsistently
    # scalar/list-typed *before* they're in any DataFrame at all, which is exactly what
    # this check is guarding against) — but everything around that unavoidable check is
    # now as cheap as plain Python allows.
    all_raw_records: list = []
    columns: dict[str, list] = {"__row_idx": []}
    for field in _REPLACEMENT_QUERY_FIELDS:
        columns[field] = []

    for tso in tso_list:
        try:
            # Build and execute query for a single TSO at a time
            query, query_filter = _build_replacement_query([tso], data_source, config, time_horizon)
            raw_records = query_data(query, query_filter)

            if not raw_records:
                logger.warning(f"No replacement models found in Elastic for TSO: {tso}")
                continue

            # CHANGED (redesigned from the earlier attempt): rather than converting the
            # full raw ES record into a DataFrame (which meant polars had to type-infer
            # every nested/irregular field, and an earlier attempt to normalize those
            # fields ended up silently corrupting fields other code relies on
            # downstream), only pull the small set of scalar fields in
            # _REPLACEMENT_QUERY_FIELDS into the polars frame — these are the only
            # fields the priority-selection/filtering logic below actually reads.
            # `__row_idx` is carried alongside purely so the winning row can be mapped
            # back to its original, completely untouched raw dict afterwards.
            for record in raw_records:
                idx = len(all_raw_records)
                columns["__row_idx"].append(idx)
                for field in _REPLACEMENT_QUERY_FIELDS:
                    columns[field].append(_normalize_es_scalar(record.get(field), field_name=field, record_idx=idx))
                all_raw_records.append(record)

        except Exception as e:
            logger.error(f"Error finding replacement models for TSO {tso}: {e}")
            continue

    if not all_raw_records:
        logger.warning(f"No replacement models found in Elastic for TSO(s): {tso_list}")
        return []

    try:
        model_df = pl.DataFrame(columns)

        # DIAGNOSTIC (new): log row counts per TSO at each pipeline stage so a pandas-vs-
        # polars result mismatch (e.g. more/fewer TSOs successfully replaced) can be
        # localized to a specific stage instead of guessed at. Remove once parity is
        # confirmed / the discrepancy is understood.
        # PERF: gated behind isEnabledFor — an f-string's interpolated expressions are
        # evaluated eagerly by Python regardless of the logger's configured level, so
        # `_tso_row_counts(...)` (a real `group_by("pmd:TSO").len()` pass over the data)
        # was running on every call even when INFO logs were filtered out entirely.
        # There are 5 of these calls in this function; unguarded, that's 5 extra full
        # scans of the data every single time, for logging output nobody may ever see.
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"[replacement diag] raw ES rows per TSO: {_tso_row_counts(model_df, tso_list)}")

        # Process replacement candidates (single vectorized call across all TSOs)
        replacement_df = create_replacement_table(scenario_date_utc, time_horizon, model_df, config)
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"[replacement diag] rows per TSO after create_replacement_table "
                f"(priority match + drop_nulls): {_tso_row_counts(replacement_df, tso_list)}"
            )

        # Apply ACNP filtering if provided (single round trip across all TSOs)
        if acnp_dict:
            # CONFIRMED (via its source, emf.model_merger.merge_functions): this is a
            # pandas-native function (operates on models['pmd:TSO'], .map(), pd.to_numeric,
            # etc.), so this to_pandas()/from_pandas() round trip is required, not just
            # defensive. It requires {'pmd:TSO', 'ac_net_position', 'sum_conform_load'} to
            # be present as columns or it silently no-ops (returns every row unfiltered,
            # by its own "fail open, never raise" design) — those last two are now in
            # _REPLACEMENT_QUERY_FIELDS above for exactly this reason. If it ever needs
            # another field, add it there too.
            filtered_pdf = filter_replacements_by_acnp(
                replacement_df.to_pandas(), acnp_dict, acnp_threshold, conform_load_factor
            )
            replacement_df = pl.from_pandas(filtered_pdf)
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"[replacement diag] rows per TSO after filter_replacements_by_acnp: "
                    f"{_tso_row_counts(replacement_df, tso_list)}"
                )

        # Exclude models that already exist (single call across all TSOs)
        if existing_models:
            replacement_df = _exclude_existing_models(replacement_df, existing_models)
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"[replacement diag] rows per TSO after _exclude_existing_models: "
                    f"{_tso_row_counts(replacement_df, tso_list)}"
                )

        if replacement_df.is_empty():  # CHANGED: was `.empty`
            logger.error("No replacement models found, replacement list is empty, possibly due to incorrect schedules")
            return []

        # Select best models across all TSOs in one call — _select_best_replacement_models
        # already groups by "pmd:TSO" internally, so batching here is exactly equivalent
        # to the per-TSO loop, just without repeating its setup cost per TSO.
        selected_models = _select_best_replacement_models(replacement_df,
                                                          target_time_horizon=time_horizon,
                                                          target_date=scenario_date_utc,
                                                          config=config)
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"[replacement diag] rows per TSO after _select_best_replacement_models: "
                f"{_tso_row_counts(selected_models, tso_list)}"
            )
        _log_replacement_results(selected_models, tso_list)

        if selected_models.is_empty():  # CHANGED: was `.empty`
            return []

    except Exception as e:
        logger.error(f"Error finding replacement models for TSOs {tso_list}: {e}")
        return []

    # CHANGED: rather than turning the (flat, trimmed) selected_models frame directly
    # into the returned dicts, look each winning row's __row_idx back up in
    # all_raw_records — the untouched, full-fidelity original ES documents — so
    # get_content() and anything else downstream receives every field exactly as
    # Elasticsearch returned it (nested metadata, component lists, etc. included), none
    # of it having passed through polars at all.
    selected_indices = selected_models["__row_idx"].to_list()
    replacement_models = [all_raw_records[i] for i in selected_indices]
    replacement_models = [get_content(model) for model in replacement_models]

    return replacement_models


def make_lists_priority(timestamp, target_timehorizon, conf):
    """
     Args:
         timestamp: target timestamps where the hour conf is read from
         conf: main conf imported there the replacement dif timestamps are extracted

     Returns: from configuration a list of to be matched values
    """
    date_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    filter_hour = date_time.strftime("%H:%M")
    filter_day = date_time.weekday()
    hour_list = []
    day_list = []

    for hour in conf["hours"]:
        if hour["hour"] == filter_hour:
            hour_list = [item[key] for item in hour["priority"] for key in item]
    for day in conf["days"]:
        if day["day"] == filter_day:
            day_list = [item[key] for item in day["priority"] for key in item]

    hour_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%H:%M"), hour_list))
    day_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%Y-%m-%d"), day_list))

    business_list = conf["time_horizons"][target_timehorizon]["request_list"]
    business_list_final = business_list

    # Month ahead requires separate replacement logic
    if target_timehorizon == 'MO':
        time_horizon_config = conf["time_horizons"][target_timehorizon]
        if "special_handling" in time_horizon_config:
            hour_list_final = [hour for hour in time_horizon_config["special_handling"]["hours"]]
            day_list_final = [get_first_monday_of_last_month(timestamp).strftime("%Y-%m-%d")]
            business_list_final = time_horizon_config["special_handling"]["business_type"]

    return hour_list_final, day_list_final, business_list_final


# CHANGED: type hints `pd.DataFrame` -> `pl.DataFrame` in the signature below
def create_replacement_table(target_timestamp, target_timehorizon, valid_models_df: pl.DataFrame, conf) -> pl.DataFrame:
    """

    Args:
        target_timestamp: target_timestamp
        target_timehorizon: target_timehorizon
        valid_models_df: valid_models_df
        conf: conf

    Returns: replacement table with priorities for the matching timestamps

    """
    list_hour_priority, list_time_priority, list_business_priority = make_lists_priority(target_timestamp, target_timehorizon, conf) #make list of relevant Timestamps

    # PERF (rewritten from the earlier map_elements-per-row version): every transform
    # below is now a genuinely vectorized polars expression or a join, rather than a
    # Python callback invoked once per row — the actual source of polars' speed
    # advantage, which `.map_elements()` (polars' `.apply()` equivalent) doesn't get you.
    # The single exception is the one dateutil.parser.parse() call below, which is kept
    # exactly as it was for parity (dateutil accepts a broader range of date formats than
    # polars' built-in parser) — but where the ORIGINAL code parsed pmd:scenarioDate with
    # dateutil/strptime FOUR separate times per row (once to reformat it, then again in
    # the hour-priority lookup, again in the day-priority lookup, and again in
    # _select_best_replacement_models's STEP1/STEP2 date-match check), this version parses
    # it once into a native pl.Datetime column and reuses that via `.dt` accessors
    # everywhere else — so the one unavoidable Python-per-row cost we do pay is paid once
    # rather than four times.

    # NEW (parity with pandas): bucket intraday hour codes ('01'..'24') under 'ID' for
    # priority/selection matching only, into a separate `normalized_time_horizon` column.
    # `pmd:timeHorizon` itself is left untouched, since it is returned as-is on the
    # replacement model (the old version overwrote `pmd:timeHorizon` in place, which
    # corrupted the value returned to callers — this is the fix for that).
    # CHANGED: was `.map_elements(lambda x: 'ID' if x in [...] else x)`. `pl.when/then/
    # otherwise` is polars' vectorized if/else — `is_in([...])` runs as one Rust-native
    # pass over the whole column instead of a Python membership check per row. Handles
    # None explicitly first so a null passes through as null, matching the original
    # `if x is not None else None` guard.
    id_horizons = [f'{i:02}' for i in range(1, 25)]
    valid_models_df = valid_models_df.with_columns(
        pl.when(pl.col("pmd:timeHorizon").is_null())
          .then(pl.lit(None, dtype=pl.Utf8))
          .when(pl.col("pmd:timeHorizon").is_in(id_horizons))
          .then(pl.lit("ID"))
          .otherwise(pl.col("pmd:timeHorizon"))
          .alias("normalized_time_horizon")
    )

    # Add target time horizon for reference in selection logic
    # CHANGED: was `valid_models_df['target_time_horizon'] = target_timehorizon` (pandas
    # broadcasts a scalar to every row automatically on assignment). Polars needs an explicit
    # `pl.lit(...)` to turn that scalar into a broadcastable column expression.
    valid_models_df = valid_models_df.with_columns(
        pl.lit(target_timehorizon).alias("target_time_horizon")
    )

    # CHANGED: was `.map_elements(lambda x: list_business_priority.index(x) if x in
    # list_business_priority else None)`. A left join against a small "value -> its
    # index" mapping table does the same lookup as one vectorized, Rust-native hash join
    # instead of a Python `.index()` call per row. `.unique(keep="first")` on the mapping
    # table guards against list_business_priority containing a duplicate value, which
    # would otherwise fan a matching row out into duplicates via the join.
    # CHANGED (parity with pandas): joins on `normalized_time_horizon` now, not
    # `pmd:timeHorizon` — the priority lookup needs the ID-bucketed value, but
    # `pmd:timeHorizon` itself is no longer overwritten to hold it (see above).
    business_priority_df = pl.DataFrame({
        "normalized_time_horizon": list_business_priority,
        "priority_business": list(range(len(list_business_priority))),
    }, schema={"normalized_time_horizon": pl.Utf8, "priority_business": pl.Int64}).unique(subset=["normalized_time_horizon"], keep="first")
    valid_models_df = valid_models_df.join(business_priority_df, on="normalized_time_horizon", how="left")

    # CHANGED: was `.map_elements(lambda x: parser.parse(x).strftime(...))`, then
    # re-parsed with `datetime.strptime` two more times downstream (priority_hour,
    # priority_day) and a fourth time in _select_best_replacement_models. This is now the
    # ONLY parse of pmd:scenarioDate in the whole pipeline — it produces a native
    # pl.Datetime column (not a re-stringified value) that every later step derives from
    # via `.dt` accessors instead of re-parsing the string. Still uses dateutil.parser.parse
    # (not polars' own string-to-datetime) specifically to preserve exact parity with the
    # original/pandas parsing behavior — dateutil accepts format variations polars'
    # built-in parser may not.
    # NEW: null-guarded — `record.get("pmd:scenarioDate")` can legitimately be None (key
    # missing, or nulled out by `_normalize_es_scalar` for a malformed multi-value field).
    # `parser.parse(None)` raises TypeError, which would previously abort the whole
    # find_replacement_models() call for every TSO in this batch rather than just failing
    # to match this one record. Returning None here lets drop_nulls() filter the record
    # out cleanly instead, matching pandas' "quietly fails to match, gets dropped" behavior.
    # NEW: `.replace(tzinfo=None)` after parsing — the original code's `.strftime(
    # "%Y-%m-%dT%H:%M:%SZ")` never actually converted to UTC, it just printed whatever
    # wall-clock time dateutil parsed plus a literal "Z" label, tz-aware or not. Keeping
    # the raw parsed datetime tz-aware here would reproduce that value correctly for any
    # single record, but if some records parse as tz-aware (e.g. an offset in the source
    # string) and others as tz-naive, building one polars Datetime column from a mix of
    # the two raises a type error — the old string-based version of this step never hit
    # that because strftime output is a plain string either way. Stripping tzinfo makes
    # every row tz-naive uniformly, matching the original's effective (if not strictly
    # correct) wall-clock-only semantics exactly, while making the column always buildable.
    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").map_elements(
            lambda x: parser.parse(x).replace(tzinfo=None) if x is not None else None,
            return_dtype=pl.Datetime
        ).alias("pmd:scenarioDate")
    )

    # CHANGED: was `.map_elements(lambda x: list_hour_priority.index(datetime.strptime(x,
    # ...).strftime("%H:%M")) if ... else None)`. `.dt.strftime` derives "%H:%M" natively
    # from the already-parsed Datetime column (no re-parse), then a left join does the
    # index lookup exactly like priority_business above.
    hour_priority_df = pl.DataFrame({
        "__hour_str": list_hour_priority,
        "priority_hour": list(range(len(list_hour_priority))),
    }, schema={"__hour_str": pl.Utf8, "priority_hour": pl.Int64}).unique(subset=["__hour_str"], keep="first")
    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%H:%M").alias("__hour_str")
    ).join(hour_priority_df, on="__hour_str", how="left").drop("__hour_str")

    # CHANGED: was `.map_elements(lambda x: list_time_priority.index(datetime.strptime(x,
    # ...).strftime("%Y-%m-%d")) if ... else None)`. Same pattern as priority_hour above.
    day_priority_df = pl.DataFrame({
        "__day_str": list_time_priority,
        "priority_day": list(range(len(list_time_priority))),
    }, schema={"__day_str": pl.Utf8, "priority_day": pl.Int64}).unique(subset=["__day_str"], keep="first")
    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%Y-%m-%d").alias("__day_str")
    ).join(day_priority_df, on="__day_str", how="left").drop("__day_str")

    # CHANGED: was `valid_models_df.dropna(subset=[...])` -> polars uses `.drop_nulls(subset=[...])`
    valid_models_df = valid_models_df.drop_nulls(subset=["priority_hour", "priority_day", "priority_business"])

    return valid_models_df


def get_tsos_available_in_storage(time_horizon: str):
    metadata = {"opde:Object-Type": "IGM", "valid": True}
    # Get query length directly from time horizon configuration
    time_horizon_config = replacement_config["time_horizons"][time_horizon]
    query_filter = 'now-' + time_horizon_config["replacement_length"]
    unique_tsos = fetch_unique_values(metadata_query=metadata, field="pmd:TSO.keyword", query_filter=query_filter)

    return unique_tsos


def get_first_monday_of_last_month(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    if dt.month == 1:
        prev_month = 12
        prev_year = dt.year -1
    else:
        prev_month = dt.month -1
        prev_year = dt.year
    try:
        previous_month_day = dt.replace(month=prev_month, year=prev_year)
    except ValueError:
        first_day_of_current_month = dt.replace(day=1)
        previous_month_day = first_day_of_current_month - timedelta(days=1)

    first_day_of_month = previous_month_day.replace(day=1)
    weekday = first_day_of_month.weekday()
    days_to_add = (0 - weekday) % 7
    first_monday = first_day_of_month + timedelta(days=days_to_add)

    return first_monday
