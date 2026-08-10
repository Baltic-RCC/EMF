import config
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from emf.common.integrations import elastic

logger = logging.getLogger(__name__)

# ENTSO-E StatusType (Table 31) codes identifying docStatus.value, used by both PEVF (1D) and
# CGMA (2D) documents - CGMA also publishes an "initial" and a "final" version, per EMF
# requirements specification section 6.7.2.3
DOC_STATUS_FIELD = "docStatus.value"
DOC_STATUS_PRELIMINARY = "A01"  # "Intermediate"
DOC_STATUS_FINAL = "A02"  # "Final"


def query_hvdc_schedules(time_horizon: str,
                         scenario_timestamp: str | datetime) -> dict | None:
    """
    Method to get HVDC schedules (business type - B63 for PEVF, B67 - for CGMA)
    :param time_horizon: time horizon of schedules
    :param scenario_timestamp: scenario timestamp in utc. Example: '2023-08-08T23:30:00Z'
    :return: DC schedules in dict format
    """
    # Create Elastic client
    service = elastic.Elastic()

    # Get area name to eic mapping
    try:
        area_eic_codes = service.get_docs_by_query(index='config-areas', query={'match_all': {}}, size=500)
        hvdc_eic_codes = service.get_docs_by_query(index='config-bds-lines', query={'match_all': {}}, size=500)
        area_eic_map = area_eic_codes.set_index('area.eic')['area.code'].to_dict()
        hvdc_eic_map = hvdc_eic_codes.set_index('IdentifiedObject.energyIdentCodeEic')['IdentifiedObject.description'].to_dict()
    except Exception as e:
        logger.warning(f"Eic mapping configuration retrieval failed, using default: {e}")
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())
        hvdc_eic_map = {}

    # Define utc end time from timestamp
    utc_end = datetime.fromisoformat(scenario_timestamp) + timedelta(minutes=15)

    # Define business type by time horizon
    business_type = "B63" if time_horizon in ["1D", "ID"] else "B67"

    # Define metadata dictionary
    metadata = {
        "@time_horizon": time_horizon,
        "TimeSeries.businessType": business_type,
    }

    # Get HVDC schedules
    schedules_df = service.query_schedules_from_elk(
        index="emfos-schedules*",
        utc_start=scenario_timestamp,
        utc_end=utc_end.isoformat(),
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Map HVDC names
    schedules_df["hvdc_name"] = schedules_df["TimeSeries.connectingLine_RegisteredResource.mRID"].map(hvdc_eic_map)

    # Filter to the latest revision number
    schedules_df.revisionNumber = schedules_df.revisionNumber.astype(int)
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.connectingLine_RegisteredResource.mRID", "hvdc_name"]
    schedules_df = schedules_df[_cols]
    schedules_df.rename(columns={"TimeSeries.connectingLine_RegisteredResource.mRID": "registered_resource"},
                        inplace=True)
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


def _fetch_acnp_schedules(service, time_horizon: str, scenario_timestamp: str, area_eic_map: dict,
                          area_name_map: dict, status_field: str | None = None,
                          status_value: str | None = None) -> pd.DataFrame | None:
    """
    Query and map ACNP (B64) schedules for a single time horizon, optionally narrowed down to a
    status (status_field/status_value, e.g. PEVF's docStatus.value). Only accepted schedules
    (Reason.code A88 "Time series matched") are returned.
    """
    # Define utc end time from timestamp
    utc_end = datetime.fromisoformat(scenario_timestamp) + timedelta(minutes=15)

    # Define metadata dictionary
    metadata = {
        "@time_horizon": time_horizon,
        "TimeSeries.businessType": "B64",
    }
    if status_field and status_value:
        metadata[status_field] = status_value

    # Get AC area schedules
    schedules_df = service.query_schedules_from_elk(
        index="emfos-schedules*",
        utc_start=scenario_timestamp,
        utc_end=utc_end.isoformat(),
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Keep only accepted (valid) schedules, treating an absent Reason code as valid
    if "Reason.code" in schedules_df.columns:
        schedules_df = schedules_df[
            schedules_df["Reason.code"].isna() | (schedules_df["Reason.code"] == "A88")]

    if schedules_df.empty:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)
    # Rename party names to IGM TSO names
    schedules_df["TimeSeries.in_Domain.party"] = schedules_df["in_domain"].map(area_name_map)
    schedules_df["TimeSeries.out_Domain.party"] = schedules_df["out_domain"].map(area_name_map)

    # Filter to the latest revision number
    schedules_df.revisionNumber = schedules_df.revisionNumber.astype(int)
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    return schedules_df


def _missing_acnp_tsos(schedules_df: pd.DataFrame, tso_list: list) -> tuple:
    """Return the TSOs from tso_list that have no in_Domain / out_Domain schedule entry."""
    present_in = set(schedules_df["TimeSeries.in_Domain.party"].dropna())
    present_out = set(schedules_df["TimeSeries.out_Domain.party"].dropna())

    missing_in = [tso for tso in tso_list if tso not in present_in]
    missing_out = [tso for tso in tso_list if tso not in present_out]

    return missing_in, missing_out


def _acnp_replacement_steps(time_horizon: str) -> list[dict]:
    """
    Replacement steps for a missing ACNP schedule, in priority order, per the EMF requirements
    specification section 6.7.2 (referenced in issues #294 and #478):

    Intraday (ID) - section 6.7.2.1: PEVF final Day-Ahead reference program (same day).
    Day-Ahead (1D) - section 6.7.2.2: PEVF preliminary (same day) -> CGMA final (same day) ->
    PEVF final (previous energy delivery day).
    Two-Days-Ahead (2D) - section 6.7.2.3: CGMA initial reference program (same day) -> PEVF
    final Day-Ahead reference program (same day) - the only other horizon we have to fall back
    to, standing in for "the previous time horizon" the spec refers to.

    Other horizons have no defined chain and are returned unchanged.
    """
    if time_horizon == "ID":
        return [
            {"time_horizon": "1D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_FINAL,
             "day_offset": 0},
        ]

    if time_horizon == "1D":
        return [
            {"time_horizon": "1D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_PRELIMINARY,
             "day_offset": 0},
            {"time_horizon": "2D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_FINAL,
             "day_offset": 0},
            {"time_horizon": "1D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_FINAL,
             "day_offset": -1},
        ]

    if time_horizon == "2D":
        return [
            {"time_horizon": "2D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_PRELIMINARY,
             "day_offset": 0},
            {"time_horizon": "1D", "status_field": DOC_STATUS_FIELD, "status_value": DOC_STATUS_FINAL,
             "day_offset": 0},
        ]

    return []


def replace_missing_acnp_schedules(schedules_df: pd.DataFrame,
                                   service,
                                   time_horizon: str,
                                   scenario_timestamp: str,
                                   area_eic_map: dict,
                                   area_name_map: dict,
                                   merged_model: object = None) -> pd.DataFrame:
    """
    Fill in ACNP schedules missing for some TSOs following the ID/1D/2D replacement chains.
    Horizons with no defined chain are returned unchanged.
    """
    tso_list = list(set(area_name_map.values()))
    missing_in, missing_out = _missing_acnp_tsos(schedules_df, tso_list)
    replaced_entity = []

    for step in _acnp_replacement_steps(time_horizon):
        if not missing_in and not missing_out:
            break

        step_timestamp = (datetime.fromisoformat(scenario_timestamp) + timedelta(days=step["day_offset"])).isoformat()
        replacement_df = _fetch_acnp_schedules(service, step["time_horizon"], step_timestamp, area_eic_map,
                                               area_name_map, status_field=step["status_field"],
                                               status_value=step["status_value"])
        if replacement_df is None:
            continue

        replacements = pd.concat([
            replacement_df[replacement_df["TimeSeries.in_Domain.party"].isin(missing_in)],
            replacement_df[replacement_df["TimeSeries.out_Domain.party"].isin(missing_out)],
        ]).drop_duplicates()

        if not replacements.empty:
            replaced_tsos = sorted(
                (set(replacements["TimeSeries.in_Domain.party"]) & set(missing_in)) |
                (set(replacements["TimeSeries.out_Domain.party"]) & set(missing_out))
            )
            logger.info(f"Replacing missing ACNP schedules for time horizon '{time_horizon}' with "
                       f"{step['status_field']}='{step['status_value']}' "
                       f"(time horizon '{step['time_horizon']}', day offset {step['day_offset']}) for TSO(s): "
                       f"{replaced_tsos}")
            replaced_entity.extend([{"tso": tso, "time_horizon": step["time_horizon"],
                                     "day_offset": step["day_offset"], "status_field": step["status_field"],
                                     "status_value": step["status_value"]} for tso in replaced_tsos])
            schedules_df = pd.concat([schedules_df, replacements], ignore_index=True)
            missing_in, missing_out = _missing_acnp_tsos(schedules_df, tso_list)

    missing_tsos = sorted(set(missing_in) | set(missing_out))
    if missing_tsos:
        logger.warning(f"No replacement ACNP schedules found for: {missing_tsos}")

    if merged_model is not None:
        merged_model.acnp_schedule_replaced_entity.extend(replaced_entity)
        if missing_tsos:
            merged_model.acnp_schedule_missing.extend(missing_tsos)
            merged_model.acnp_schedule_replaced = False
        elif replaced_entity:
            merged_model.acnp_schedule_replaced = True

    return schedules_df


def query_acnp_schedules(time_horizon: str,
                         scenario_timestamp: str | datetime,
                         merged_model: object = None) -> dict | None:
    """
    Method to get ACNP schedules (business type - B64). For the ID, 1D and 2D horizons, schedules
    missing for some TSOs are replaced following the chains, see replace_missing_acnp_schedules.
    :param time_horizon: time horizon of schedules
    :param scenario_timestamp: scenario timestamp in utc. Example: '2023-08-08T23:30:00Z'
    :param merged_model: optional MergedModel instance to log the replacement outcome onto
    :return: AC schedules in dict format
    """
    # Create Elastic client
    service = elastic.Elastic()

    # Get area name to eic mapping
    try:
        area_eic_codes = service.get_docs_by_query(index='config-areas', query={'match_all': {}}, size=500)
        area_eic_map = area_eic_codes.set_index('area.eic')['area.code'].to_dict()
        area_name_map = area_eic_codes.set_index('area.code')['party.name'].to_dict()
    except Exception as e:
        logger.warning(f"Eic mapping configuration retrieval failed, using default: {e}")
        # Using default mapping table from config
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())
        area_name_map = {}

    main_status_value = DOC_STATUS_FINAL if time_horizon in ("1D", "2D") else None
    schedules_df = _fetch_acnp_schedules(service, time_horizon, scenario_timestamp, area_eic_map, area_name_map,
                                         status_field=DOC_STATUS_FIELD if main_status_value else None,
                                         status_value=main_status_value)

    if schedules_df is None:
        schedules_df = pd.DataFrame(
            columns=["value", "in_domain", "out_domain", "TimeSeries.in_Domain.party", "TimeSeries.out_Domain.party"])

    schedules_df = replace_missing_acnp_schedules(schedules_df, service, time_horizon, scenario_timestamp,
                                                  area_eic_map, area_name_map, merged_model=merged_model)

    if schedules_df.empty:
        return None

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.in_Domain.party", "TimeSeries.out_Domain.party"]
    schedules_df = schedules_df[_cols]
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


def calculate_ac_net_position(ac_schedules):
    if ac_schedules:
        acnp = pd.DataFrame(ac_schedules)
        acnp = (
            pd.concat([
                (acnp.loc[acnp['TimeSeries.in_Domain.party'].notna()].set_index('TimeSeries.in_Domain.party')['value'].mul(-1)),
                (acnp.loc[acnp['TimeSeries.out_Domain.party'].notna()].set_index('TimeSeries.out_Domain.party')['value'])
            ]).groupby(level=0).sum())
        acnp = acnp.to_dict()
    else:
        acnp = None

    return acnp