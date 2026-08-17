import logging
from datetime import datetime

import croniter

from emf.common.helpers.time import parse_duration, timezone, parse_datetime

logger = logging.getLogger(__name__)

# (settings key, run["properties"] key). Left blank in settings, each leaves the run's own
# configured default in process_conf.json untouched rather than overwriting it with "".
_PASSTHROUGH_PROPERTIES = (
    ("RUN_REPLACEMENT", "replacement"),
    ("RUN_SCALING", "scaling"),
    ("OUTAGE_UPDATE", "outage_update"),
    ("FORCE_OUTAGE_FIX", "force_outage_fix"),
    ("UPLOAD_TO_OPDM", "upload_to_opdm"),
    ("UPLOAD_TO_MINIO", "upload_to_minio"),
    ("SEND_MERGE_REPORT", "send_merge_report"),
    ("POST_TEMP_FIXES", "post_temp_fixes"),
    ("LVL8_REPORTING", "lvl8_reporting"),
    ("TASK_MERGING_ENTITY", "merging_entity"),
)


def select_run(process_config: list, run_type: str) -> tuple[dict, dict]:
    """Find the (process, run) pair whose run @id matches run_type exactly."""
    for process in process_config:
        for run in process.get("runs", []):
            if run["@id"].rsplit("/runs/", 1)[-1] == run_type:
                return process, run
    raise ValueError(f"No run found matching RUN_TYPE '{run_type}' in process configuration")


def select_timeframe(timeframe_config: list, time_frame: str) -> dict:
    """Find the timeframe entry whose @id matches time_frame exactly."""
    for entry in timeframe_config:
        if entry["@id"].rsplit("/", 1)[-1] == time_frame:
            return entry
    raise ValueError(f"No timeframe configuration found for time_frame '{time_frame}'")


def build_manual_run_config(process_config_json: list, timeframe_config_json: list, settings: dict) -> tuple[list, list, str]:
    """
    Selects the single run a manual trigger targets and applies the manual-mode overrides from
    `settings` (the manual-only keys from task_generator.properties - pass worker.py's globals()
    after parse_app_properties has populated it).

    Returns (process_config_json, timeframe_config_json, timestamp), trimmed to that one run and
    ready to pass into generate_tasks().
    """
    run_type = settings["RUN_TYPE"]

    process, run = select_run(process_config_json, run_type)
    process_config_json = [{**process, "runs": [run]}]
    timeframe_config_json = [select_timeframe(timeframe_config_json, run["time_frame"])]

    original_run_at = run["run_at"]
    run["run_at"] = "* * * * *"

    run["properties"]["included"] = [tso.strip() for tso in settings["INCLUDED_TSO"].split(",")] if settings["INCLUDED_TSO"] else []
    run["properties"]["excluded"] = [tso.strip() for tso in settings["EXCLUDED_TSO"].split(",")] if settings["EXCLUDED_TSO"] else []
    run["properties"]["local_import"] = [tso.strip() for tso in settings["LOCAL_IMPORT"].split(",")] if settings["LOCAL_IMPORT"] else []
    run["properties"]["replace_tso"] = [tso.strip() for tso in settings["REPLACE_TSO"].split(",")] if settings["REPLACE_TSO"] else []

    # Blank is meaningful here (treated as 'auto' downstream by set_task_version), so - unlike the
    # passthrough properties below - this always overwrites the run's configured default.
    run["properties"]["version"] = settings["TASK_VERSION"]

    for setting_key, property_key in _PASSTHROUGH_PROPERTIES:
        if settings[setting_key]:
            run["properties"][property_key] = settings[setting_key]

    now_reference_run_types = {rt.strip() for rt in settings["MANUAL_NOW_REFERENCE_RUN_TYPES"].split(",") if rt.strip()}
    day_shift_run_types = {rt.strip() for rt in settings["MANUAL_DAY_SHIFT_RUN_TYPES"].split(",") if rt.strip()}

    # If a single timestamp override is defined
    if settings["TIMESTAMP"]:
        timeframe_config_json[0]["reference_time_start"] = settings["TASK_REFERENCE_TIME"]
        timeframe_config_json[0]["reference_time_end"] = settings["TASK_REFERENCE_TIME"]
        timeframe_config_json[0]["period_start"] = "PT0M"
        timeframe_config_json[0]["period_end"] = "PT1H"
        # :00 seconds trips generate_tasks()'s cron boundary check into running twice - same fix
        # already used below for the reconstructed timestamp.
        timestamp = parse_datetime(settings["TIMESTAMP"]).replace(second=1).isoformat()
    elif run_type in now_reference_run_types:
        # Same :00-seconds boundary issue as the TIMESTAMP override above - astronomically unlikely
        # with a real clock, but cheap to guard against consistently.
        timestamp = datetime.now(tz=timezone(process["time_zone"])).replace(second=1).isoformat()
    else:
        day_start = datetime.now(tz=timezone(process["time_zone"])).replace(hour=0, minute=0, second=0, microsecond=0)
        if run_type in day_shift_run_types:
            day_start = day_start + parse_duration("-P1D")
        timestamp = croniter.croniter(original_run_at, day_start).get_next(datetime).replace(second=1).isoformat()

    return process_config_json, timeframe_config_json, timestamp
