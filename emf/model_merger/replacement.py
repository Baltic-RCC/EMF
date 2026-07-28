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
        # Build and execute query
        query, query_filter = _build_replacement_query(tso_list, data_source, config, time_horizon)
        model_df = pd.DataFrame(query_data(query, query_filter))

        if model_df.empty:
            logger.warning(f"No replacement models found in Elastic for TSO(s): {tso_list}")
            return []

        # Process replacement candidates
        scenario_date_utc = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
        replacement_df = create_replacement_table(scenario_date_utc, time_horizon, model_df, config)

        # Apply ACNP filtering if provided
        if acnp_dict:
            replacement_df = filter_replacements_by_acnp(replacement_df, acnp_dict, acnp_threshold, conform_load_factor)

        # Exclude models that already exist
        if existing_models:
            replacement_df = _exclude_existing_models(replacement_df, existing_models)

        if replacement_df.empty:
            logger.error("No replacement models found, replacement list is empty, possibly due to incorrect schedules")
            return []

        # Select best models and load content
        selected_models = _select_best_replacement_models(replacement_df,
                                                          target_time_horizon=time_horizon,
                                                          target_date=scenario_date_utc,
                                                          config=config)
        _log_replacement_results(selected_models, tso_list)

        if selected_models.empty:
            return []

        # Load content for each model
        replacement_models = selected_models.to_dict(orient='records')
        for i, model in enumerate(replacement_models):
            replacement_models[i] = get_content(model)

        return replacement_models

    except Exception as e:
        logger.error(f"Error finding replacement models for TSOs {tso_list}: {e}")
        return []


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
