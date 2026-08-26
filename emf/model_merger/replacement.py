import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from isodate import parse_duration
import logging
import config
import json
from dataclasses import dataclass
from dateutil import parser
from pathlib import Path
from emf.common.integrations.object_storage.models import query_data, get_content, fetch_unique_values
from emf.common.integrations.minio_api import *
from emf.common.config_parser import parse_app_properties
from emf.common.helpers.opdm_objects import DataSource
from emf.model_merger.merge_functions import filter_replacements_by_acnp, ModelEntity

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.replacement)
replacement_config = json.load(config.paths.cgm_worker.replacement_conf)


@dataclass
class ReplacementRequest:
    """A single 'this TSO's IGM needs a same-source, different-timeframe substitute' request."""
    tso: str
    data_source: str
    forced: bool = False


def run_replacement(igm_models, model_replacement, local_import_models,
                    missing_local_import, missing_models, replace_tso, time_horizon,
                    scenario_datetime, merged_model, acnp_dict=None, acnp_threshold=None,
                    conform_load_factor=None):
    """
    Execute model replacement (the ENTSO-E EMF requirements STEP1-4 historical fallback) in a
    single pass over a flat, priority-ordered list of replacement requests, dispatched by
    data-source.

    Priority order: forced (replace_tso) first, then normal missing-model replacement. A TSO
    already satisfied by a higher-priority request is skipped by later ones.

    Args:
        igm_models: Unified list of OPDM/PDN model dicts, each carrying its own 'data-source'.
        model_replacement: Boolean flag for normal (non-forced) replacement.
        local_import_models: List of TSOs configured for local (PDN) import.
        missing_local_import: List of TSOs missing from PDN.
        missing_models: List of TSOs missing from OPDM.
        replace_tso: List of TSOs to force replacement for (highest priority, ignores existing models).
        time_horizon: Time horizon for replacement query.
        scenario_datetime: Scenario datetime for replacement query.
        merged_model: MergedModel instance to update (replaced_entity, replaced).
        acnp_dict: AC Net Position dictionary (optional).
        acnp_threshold: ACNP threshold (optional).
        conform_load_factor: Conform load factor (optional).

    Returns:
        igm_models with replacements applied.
    """
    logger.info("Starting missing model replacement")
    # Build the flat, priority-ordered request list: forced first, then normal missing-model replacement
    requests = []
    if replace_tso:
        forced_by_source = {}
        for tso in replace_tso:
            data_source = DataSource.PDN if tso in local_import_models else DataSource.OPDM
            forced_by_source.setdefault(data_source, []).append(tso)
            requests.append(ReplacementRequest(tso=tso, data_source=data_source, forced=True))
        for data_source, tsos in forced_by_source.items():
            logger.info(f"Forced {data_source} replacement requested (ignoring current models) for: {tsos}")

    if model_replacement:
        for tso in missing_models:
            requests.append(ReplacementRequest(tso=tso, data_source=DataSource.OPDM))
        if local_import_models:
            for tso in missing_local_import:
                requests.append(ReplacementRequest(tso=tso, data_source=DataSource.PDN))

    replaced_tsos_tracker = set()
    any_success = False
    results_by_label = {}

    for request in requests:
        label = f"{'forced ' if request.forced else ''}{request.data_source}"
        outcome = results_by_label.setdefault(
            label, {"succeeded": [], "no_replacement": [], "skipped": [], "errored": []})

        if request.tso in replaced_tsos_tracker:
            outcome["skipped"].append(request.tso)
            continue

        try:
            # Forced replacement excludes only the TSO's own existing model (it replaces regardless
            # of whether one exists); normal replacement excludes against the whole data-source
            if request.forced:
                existing_models_for_request = [
                    model for model in igm_models
                    if model.get('pmd:TSO') == request.tso and model.get('data-source') == request.data_source
                ]
            else:
                existing_models_for_request = [
                    model for model in igm_models if model.get('data-source') == request.data_source
                ]

            replacement_models = find_replacement_models(
                tso_list=[request.tso],
                time_horizon=time_horizon,
                scenario_date=scenario_datetime,
                data_source=request.data_source,
                acnp_dict=acnp_dict,
                acnp_threshold=acnp_threshold,
                conform_load_factor=conform_load_factor,
                existing_models=existing_models_for_request
            ) or []

            if replacement_models:
                outcome["succeeded"].append(request.tso)
                replaced_tsos_tracker.add(request.tso)
                any_success = True

                merged_model.replaced_entity.extend([
                    ModelEntity(quality_indicator='Substituted', **model).__dict__
                    for model in replacement_models
                ])

                if request.forced:
                    # Remove the TSO's old model of this data-source before adding its replacement
                    igm_models[:] = [
                        model for model in igm_models
                        if not (model.get('pmd:TSO') == request.tso
                                and model.get('data-source') == request.data_source)
                    ]
                igm_models.extend(replacement_models)
            else:
                outcome["no_replacement"].append(request.tso)

        except Exception as error:
            logger.error(f"{label} replacement failed for {request.tso}: {error}", exc_info=True)
            outcome["errored"].append(request.tso)

    # One consolidated line per replacement type, listing the TSOs behind each outcome
    for label, outcome in results_by_label.items():
        summary = ", ".join(f"{key}={value}" for key, value in outcome.items() if value)
        log = logger.warning if (outcome["no_replacement"] or outcome["errored"]) else logger.info
        log(f"{label} replacement: {summary}")

    if any_success:
        merged_model.replaced = True
    elif requests:
        merged_model.replaced = False

    return igm_models


def _build_replacement_query(tso_list: list, data_source: str, config: dict, time_horizon: str) -> tuple:
    """Build query filter and query dict for replacement models."""
    # Get replacement length directly from time horizon configuration
    time_horizon_config = config["time_horizons"][time_horizon]
    query_filter = 'now-' + time_horizon_config["replacement_length"]
    query = {"pmd:TSO.keyword": tso_list, "valid": True, "data-source": data_source}
    return query, query_filter


_REPLACEMENT_QUERY_FIELDS = [
    "pmd:TSO",
    "pmd:scenarioDate",
    "pmd:timeHorizon",
    "pmd:versionNumber",
    "pmd:creationDate",
    "ac_net_position",
    "sum_conform_load",
]


def _normalize_es_scalar(value, field_name: str = None, record_idx: int = None):
    """ES "multi-value" fields sometimes return a list instead of a scalar. Null it out
    (don't unwrap to value[0]) so the record fails matching and gets dropped, same as
    pandas did implicitly for this case -- unwrapping would rescue records pandas
    would have excluded."""
    if isinstance(value, list):
        logger.warning(
            f"Field '{field_name}' on ES record #{record_idx} is a list ({value!r}) where "
            f"a scalar was expected; treating as unmatched/invalid to match pandas' "
            f"original (implicit) behavior for this case."
        )
        return None
    return value


def _select_best_replacement_models(replacement_df: pl.DataFrame, target_time_horizon, target_date, config) -> pl.DataFrame:
    """Select the best replacement model per TSO via the 4-step priority cascade,
    vectorized with .over("pmd:TSO") instead of a per-TSO Python loop."""
    if replacement_df.is_empty():
        return replacement_df

    if isinstance(target_date, str):
        target_date = parser.parse(target_date)
    target_date_only = target_date.date()

    # Step candidacy per row (cond3 is a superset of cond1 -- fine, what matters is which
    # step ends up chosen per TSO below, not whether a row matches more than one in isolation)
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

    # Per-TSO "does step N have ANY candidate at all"
    df = df.with_columns([
        pl.col("__cond1").any().over("pmd:TSO").alias("__has1"),
        pl.col("__cond2").any().over("pmd:TSO").alias("__has2"),
        pl.col("__cond3").any().over("pmd:TSO").alias("__has3"),
    ])

    # step1 -> step2 -> step3 -> step4 fallback per TSO
    df = df.with_columns(
        pl.when(pl.col("__has1")).then(1)
          .when(pl.col("__has2")).then(2)
          .when(pl.col("__has3")).then(3)
          .otherwise(4)
          .alias("__chosen_step")
    )

    if logger.isEnabledFor(logging.DEBUG):
        step_messages = {
            1: "STEP 1: Found same time horizon and same day replacement for {tso}",
            2: "STEP 2: Found same day (different time horizon) replacement for {tso}",
            3: "STEP 3: Found same time horizon, same day type replacement for {tso}",
            4: "STEP 4: Used priority-based selection for {tso}",
        }
        for row in df.select(["pmd:TSO", "__chosen_step"]).unique().iter_rows(named=True):
            logger.debug(step_messages[row["__chosen_step"]].format(tso=row["pmd:TSO"]))

    # Keep only each TSO's chosen-step candidate set
    df = df.filter(
        ((pl.col("__chosen_step") == 1) & pl.col("__cond1")) |
        ((pl.col("__chosen_step") == 2) & pl.col("__cond2")) |
        ((pl.col("__chosen_step") == 3) & pl.col("__cond3")) |
        (pl.col("__chosen_step") == 4)
    )

    # Tie-break cascade: each filter must narrow against the PREVIOUS step's survivors,
    # so min()/max().over() is recomputed after each one, not just once up front.
    df = df.filter(pl.col("priority_business") == pl.col("priority_business").min().over("pmd:TSO"))
    df = df.filter(pl.col("priority_hour") == pl.col("priority_hour").min().over("pmd:TSO"))
    df = df.filter(pl.col("priority_day") == pl.col("priority_day").min().over("pmd:TSO"))
    df = df.filter(pl.col("pmd:versionNumber") == pl.col("pmd:versionNumber").max().over("pmd:TSO"))
    df = df.filter(pl.col("pmd:creationDate") == pl.col("pmd:creationDate").max().over("pmd:TSO"))

    # Still tied on every priority field -> warn and keep the first remaining row per TSO
    dup_counts = df.group_by("pmd:TSO").len()
    dup_tsos = dup_counts.filter(pl.col("len") > 1)["pmd:TSO"].to_list()
    for tso in dup_tsos:
        logger.warning(f"Replacement filtering unreliable for: '{tso}'")

    df = df.with_columns(pl.int_range(pl.len()).over("pmd:TSO").alias("__group_rank"))
    df = df.filter(pl.col("__group_rank") == 0)

    result = df.drop(["__cond1", "__cond2", "__cond3", "__has1", "__has2", "__has3",
                      "__chosen_step", "__group_rank"])

    if "normalized_time_horizon" in result.columns:
        result = result.drop("normalized_time_horizon")
    return result


def _exclude_existing_models(replacement_df: pl.DataFrame, existing_models: list) -> pl.DataFrame:
    """Exclude replacement models that match existing models exactly (same TSO, timestamp, time horizon)."""
    if replacement_df.is_empty() or not existing_models:
        return replacement_df

    # Existing-model identifiers (TSO, scenarioDate, timeHorizon) as the right side of an anti-join
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

    # Anti-join instead of a per-row membership check. scenarioDate is reformatted back to
    # its original string form so the match is exact string equality, not datetime equality
    # (which could differ on sub-second precision that wouldn't have matched originally).
    joined = replacement_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("__scenarioDate_str")
    )
    filtered_df = joined.join(
        existing_df.rename({"__existing_scenarioDate_str": "__scenarioDate_str"}),
        on=["pmd:TSO", "__scenarioDate_str", "pmd:timeHorizon"],
        how="anti",
    ).drop("__scenarioDate_str")

    excluded_count = replacement_df.height - filtered_df.height
    if excluded_count > 0:
        logger.info(f"Excluded {excluded_count} replacement models that match existing models")

    return filtered_df


def _log_replacement_results(replacement_df: pl.DataFrame, original_tso_list: list) -> None:
    """Log warnings for missing and unreplaced TSOs."""
    if replacement_df.is_empty():
        return

    unique_tsos = replacement_df["pmd:TSO"].unique().to_list()

    tso_missing = [tso for tso in original_tso_list if tso not in unique_tsos]
    if tso_missing:
        logger.warning(f"No replacement models found for TSO(s): {tso_missing}")


def _tso_row_counts(df: pl.DataFrame, tso_list: list) -> dict:
    """{tso: row_count} for every TSO in tso_list, 0 if absent -- for diagnostic logging."""
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

        replacement_df = create_replacement_table(scenario_date_utc, time_horizon, model_df, config)

        # filter_replacements_by_acnp is pandas-native, hence the round trip
        if acnp_dict:
            filtered_pdf = filter_replacements_by_acnp(
                replacement_df.to_pandas(), acnp_dict, acnp_threshold, conform_load_factor
            )
            replacement_df = pl.from_pandas(filtered_pdf)

        # Exclude models that already exist (single call across all TSOs)
        if existing_models:
            replacement_df = _exclude_existing_models(replacement_df, existing_models)

        if replacement_df.is_empty():
            logger.warning("No replacement models found, replacement list is empty, possibly due to incorrect schedules")
            return []

        selected_models = _select_best_replacement_models(replacement_df,
                                                          target_time_horizon=time_horizon,
                                                          target_date=scenario_date_utc,
                                                          config=config)

        _log_replacement_results(selected_models, tso_list)

        if selected_models.is_empty():
            return []

    except Exception as e:
        logger.error(f"Error finding replacement models for TSOs {tso_list}: {e}")
        return []

    # Map winning rows back to their untouched original ES documents via __row_idx, so
    # get_content() gets every field exactly as Elasticsearch returned it.
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
    valid_models_df = valid_models_df.with_columns(
        pl.lit(target_timehorizon).alias("target_time_horizon")
    )

    business_priority_df = pl.DataFrame({
        "normalized_time_horizon": list_business_priority,
        "priority_business": list(range(len(list_business_priority))),
    }, schema={"normalized_time_horizon": pl.Utf8, "priority_business": pl.Int64}).unique(subset=["normalized_time_horizon"], keep="first")
    valid_models_df = valid_models_df.join(business_priority_df, on="normalized_time_horizon", how="left")

    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").map_elements(
            lambda x: parser.parse(x).replace(tzinfo=None) if x is not None else None,
            return_dtype=pl.Datetime
        ).alias("pmd:scenarioDate")
    )

    hour_priority_df = pl.DataFrame({
        "__hour_str": list_hour_priority,
        "priority_hour": list(range(len(list_hour_priority))),
    }, schema={"__hour_str": pl.Utf8, "priority_hour": pl.Int64}).unique(subset=["__hour_str"], keep="first")
    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%H:%M").alias("__hour_str")
    ).join(hour_priority_df, on="__hour_str", how="left").drop("__hour_str")

    day_priority_df = pl.DataFrame({
        "__day_str": list_time_priority,
        "priority_day": list(range(len(list_time_priority))),
    }, schema={"__day_str": pl.Utf8, "priority_day": pl.Int64}).unique(subset=["__day_str"], keep="first")
    valid_models_df = valid_models_df.with_columns(
        pl.col("pmd:scenarioDate").dt.strftime("%Y-%m-%d").alias("__day_str")
    ).join(day_priority_df, on="__day_str", how="left").drop("__day_str")

    valid_models_df = valid_models_df.drop_nulls(subset=["pmd:TSO", "priority_hour", "priority_day", "priority_business"])

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
        prev_year = dt.year - 1
    else:
        prev_month = dt.month - 1
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
