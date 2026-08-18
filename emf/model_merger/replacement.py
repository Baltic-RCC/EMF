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
from emf.model_merger.merge_functions import filter_replacements_by_acnp

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
    from emf.model_merger.model_merger import ModelEntity

    # Build the flat, priority-ordered request list: forced first, then normal missing-model replacement
    requests = []
    if replace_tso:
        for tso in replace_tso:
            data_source = DataSource.PDN if tso in local_import_models else DataSource.OPDM
            logger.info(f"Forced replacement requested for {data_source} TSO (ignoring current models): {tso}")
            requests.append(ReplacementRequest(tso=tso, data_source=data_source, forced=True))

    if model_replacement:
        for tso in missing_models:
            requests.append(ReplacementRequest(tso=tso, data_source=DataSource.OPDM))
        if local_import_models:
            for tso in missing_local_import:
                requests.append(ReplacementRequest(tso=tso, data_source=DataSource.PDN))

    replaced_tsos_tracker = set()
    any_success = False

    for request in requests:
        if request.tso in replaced_tsos_tracker:
            logger.info(f"Replacement for {request.tso} [{request.data_source}] skipped - "
                       f"already satisfied by a higher priority request")
            continue

        label = f"{'forced ' if request.forced else ''}{request.data_source}"
        logger.info(f"Attempting {label} replacement for: {request.tso}")

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
                logger.info(f"{label} replacement succeeded for {request.tso} "
                           f"({[m['pmd:fileName'] for m in replacement_models]})")
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
                logger.warning(f"No {label} replacement available for: {request.tso}")

        except Exception as error:
            logger.error(f"{label} replacement failed for {request.tso}: {error}")

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


def _select_best_replacement_models(replacement_df: pd.DataFrame, target_time_horizon, target_date,
                                    config) -> pd.DataFrame:
    """Select the best replacement model for each TSO based on 4-step priority rules."""
    if replacement_df.empty:
        return replacement_df

    def apply_priority_selection(candidates):
        """Apply priority-based selection to candidate models."""
        return (
            candidates[candidates["priority_business"] == candidates["priority_business"].min()]
            .pipe(lambda df: df[df["priority_hour"] == df["priority_hour"].min()])
            .pipe(lambda df: df[df["priority_day"] == df["priority_day"].min()])
            .pipe(lambda df: df[df["pmd:versionNumber"] == df["pmd:versionNumber"].max()])
            .pipe(lambda df: df[df["pmd:creationDate"] == df["pmd:creationDate"].max()])
        )

    # Convert target_date to datetime if it's a string
    if isinstance(target_date, str):
        target_date = parser.parse(target_date)
    target_date_only = target_date.date()

    selected_models = []

    for tso in replacement_df["pmd:TSO"].unique():
        tso_models = replacement_df[replacement_df["pmd:TSO"] == tso].copy()
        best_model = None

        # STEP 1: Use an IGM of the same time horizon of the same energy delivery day
        step1_candidates = tso_models[
            (tso_models["normalized_time_horizon"] == target_time_horizon) &
            (tso_models["pmd:scenarioDate"].apply(lambda x: parser.parse(x).date() == target_date_only))]
        if not step1_candidates.empty:
            best_model = apply_priority_selection(step1_candidates)
            logger.debug(f"STEP 1: Found same time horizon and same day replacement for {tso}")

        # STEP 2: If not available, use an IGM from the same energy delivery day (other time horizon)
        if best_model is None or best_model.empty:
            step2_candidates = tso_models[
                (tso_models["pmd:scenarioDate"].apply(lambda x: parser.parse(x).date() == target_date_only)) &
                (tso_models["normalized_time_horizon"] != target_time_horizon)
                ]
            if not step2_candidates.empty:
                best_model = apply_priority_selection(step2_candidates)
                logger.debug(f"STEP 2: Found same day (different time horizon) replacement for {tso}")

        # STEP 3: If not available, use an IGM from the same time horizon of older models of the same day type
        if best_model is None or best_model.empty:
            step3_candidates = tso_models[
                (tso_models["normalized_time_horizon"] == target_time_horizon)
            ]
            if not step3_candidates.empty:
                best_model = apply_priority_selection(step3_candidates)
                logger.debug(f"STEP 3: Found same time horizon, same day type replacement for {tso}")

        # STEP 4: If not available, use older files of a different day type (original logic)
        if best_model is None or best_model.empty:
            best_model = apply_priority_selection(tso_models)
            logger.debug(f"STEP 4: Used priority-based selection for {tso}")

        if len(best_model) > 1:
            logger.warning(f"Replacement filtering unreliable for: '{tso}'")
            best_model = best_model.iloc[:1]

        selected_models.append(best_model)

    result = pd.concat(selected_models, ignore_index=True) if selected_models else pd.DataFrame()
    # Drop the matching-only helper column so it doesn't leak into the returned replacement model dicts
    return result.drop(columns=["normalized_time_horizon"], errors="ignore")


def _exclude_existing_models(replacement_df: pd.DataFrame, existing_models: list) -> pd.DataFrame:
    """Exclude replacement models that match existing models exactly (same TSO, timestamp, time horizon)."""
    if replacement_df.empty or not existing_models:
        return replacement_df

    # Create a set of existing model identifiers (TSO, scenarioDate, timeHorizon)
    existing_identifiers = set()
    for model in existing_models:
        tso = model.get('pmd:TSO')
        scenario_date = model.get('pmd:scenarioDate')
        time_horizon = model.get('pmd:timeHorizon')

        if tso and scenario_date and time_horizon:
            # Normalize scenario_date format for comparison
            if isinstance(scenario_date, str):
                try:
                    scenario_date = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
                except:
                    pass
            existing_identifiers.add((tso, scenario_date, time_horizon))

    if not existing_identifiers:
        return replacement_df

    # Filter out replacement models that match existing ones
    mask = ~replacement_df.apply(
        lambda row: (
                        row['pmd:TSO'],
                        row['pmd:scenarioDate'],
                        row['pmd:timeHorizon']
                    ) in existing_identifiers,
        axis=1
    )

    filtered_df = replacement_df[mask]

    # Log if any models were excluded
    excluded_count = len(replacement_df) - len(filtered_df)
    if excluded_count > 0:
        logger.info(f"Excluded {excluded_count} replacement models that match existing models")

    return filtered_df


def _log_replacement_results(replacement_df: pd.DataFrame, original_tso_list: list) -> None:
    """Log warnings for missing and unreplaced TSOs."""
    if replacement_df.empty:
        return

    unique_tsos = replacement_df["pmd:TSO"].unique().tolist()

    tso_missing = [tso for tso in original_tso_list if tso not in unique_tsos]
    if tso_missing:
        logger.warning(f"No replacement models found for TSO(s): {tso_missing}")


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

    selected_models_per_tso = []

    for tso in tso_list:
        try:
            # Build and execute query for a single TSO at a time
            query, query_filter = _build_replacement_query([tso], data_source, config, time_horizon)
            model_df = pd.DataFrame(query_data(query, query_filter))

            if model_df.empty:
                logger.warning(f"No replacement models found in Elastic for TSO: {tso}")
                continue

            # Process replacement candidates
            replacement_df = create_replacement_table(scenario_date_utc, time_horizon, model_df, config)

            # Apply ACNP filtering if provided
            if acnp_dict:
                replacement_df = filter_replacements_by_acnp(replacement_df, acnp_dict, acnp_threshold, conform_load_factor)

            # Exclude models that already exist
            if existing_models:
                replacement_df = _exclude_existing_models(replacement_df, existing_models)

            if replacement_df.empty:
                logger.error(f"No replacement models found, replacement list is empty for TSO: {tso}, possibly due to incorrect schedules")
                continue

            # Select best model for this TSO
            selected_model = _select_best_replacement_models(replacement_df,
                                                              target_time_horizon=time_horizon,
                                                              target_date=scenario_date_utc,
                                                              config=config)
            if not selected_model.empty:
                selected_models_per_tso.append(selected_model)

        except Exception as e:
            logger.error(f"Error finding replacement models for TSO {tso}: {e}")
            continue

    if not selected_models_per_tso:
        return []

    selected_models = pd.concat(selected_models_per_tso, ignore_index=True)
    _log_replacement_results(selected_models, tso_list)

    # Load content for each model
    replacement_models = selected_models.to_dict(orient='records')
    for i, model in enumerate(replacement_models):
        replacement_models[i] = get_content(model)

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


def create_replacement_table(target_timestamp, target_timehorizon, valid_models_df, conf):
    """

    Args:
        target_timestamp: target_timestamp
        target_timehorizon: target_timehorizon
        valid_models_df: valid_models_df
        conf: conf

    Returns: replacement table with priorities for the matching timestamps

    """
    list_hour_priority, list_time_priority, list_business_priority = make_lists_priority(target_timestamp,
                                                                                         target_timehorizon,
                                                                                         conf)  # make list of relevant Timestamps

    # Bucket intraday hour codes ('01'..'24') under 'ID' for priority/selection matching only.
    # Keep the original pmd:timeHorizon untouched, since it is returned as-is on the replacement model.
    valid_models_df['normalized_time_horizon'] = valid_models_df['pmd:timeHorizon'].apply(
        lambda x: 'ID' if x in [f'{i:02}' for i in range(1, 25)] else x)

    # Add target time horizon for reference in selection logic
    valid_models_df['target_time_horizon'] = target_timehorizon

    valid_models_df["priority_business"] = valid_models_df["normalized_time_horizon"].apply(
        lambda x: list_business_priority.index(x) if x in list_business_priority else None)
    valid_models_df["pmd:scenarioDate"] = valid_models_df["pmd:scenarioDate"].apply(
        lambda x: parser.parse(x).strftime("%Y-%m-%dT%H:%M:%SZ"))
    valid_models_df["priority_hour"] = valid_models_df["pmd:scenarioDate"].apply(lambda x:
                                                                                 list_hour_priority.index(
                                                                                     datetime.strptime(x,
                                                                                                       "%Y-%m-%dT%H:%M:%SZ").strftime(
                                                                                         "%H:%M"))
                                                                                 if datetime.strptime(x,
                                                                                                      "%Y-%m-%dT%H:%M:%SZ").strftime(
                                                                                     "%H:%M") in list_hour_priority else None)
    valid_models_df["priority_day"] = valid_models_df["pmd:scenarioDate"].apply(lambda x:
                                                                                list_time_priority.index(
                                                                                    datetime.strptime(x,
                                                                                                      "%Y-%m-%dT%H:%M:%SZ").strftime(
                                                                                        "%Y-%m-%d"))
                                                                                if datetime.strptime(x,
                                                                                                     "%Y-%m-%dT%H:%M:%SZ").strftime(
                                                                                    "%Y-%m-%d") in list_time_priority else None)
    valid_models_df = valid_models_df.dropna(subset=["priority_hour", "priority_day", "priority_business"])

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
