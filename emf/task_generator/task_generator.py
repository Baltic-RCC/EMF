import aniso8601
from datetime import datetime
import config
from uuid import uuid4
import croniter
import json
import logging
from os import getlogin
from emf.common.time_helper import parse_duration, convert_to_utc, convert_to_timezone, timezone, reference_times, utcnow
from emf.common.integrations.elastic import Elastic
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.task_generator.task_generator)


def generate_tasks(task_window_duration: str,
                   task_window_reference: str,
                   process_conf: dict,
                   timeframe_conf: dict,
                   timetravel_now: str | None = None,
                   process_time_shift: str | None = None,
                   ):
    """
    Generates a sequence of tasks based on the given process configuration and time frame definitions.

    Args:
        task_window_duration (str): ISO-8601 duration string specifying the duration of the time window
            within which the tasks are generated.
        task_window_reference (str): a string identifying the reference point in time for the task window,
            as defined in the `reference_times` dict imported from `time_helper.py`.
        process_conf (dict): JSON file specifying the process configuration, as described in the
            documentation.
        timeframe_conf (dict): JSON file specifying the time frame definitions, as described in the
            documentation.

    Yields:
        dict: A dictionary representing a task instance, as described in the documentation.

    Raises:
        ValueError: If the configuration is invalid or incomplete.

    """

    # TODO - add validation against schema

    # Convert the list of time frames to a dictionary for easier access.
    time_frames = {time_frame["@id"].split("/")[-1]: time_frame for time_frame in timeframe_conf}

    # Load the process configuration from the specified file.
    for process in process_conf:

        # Loop through each run in the process configuration.
        for run in process["runs"]:

            # Get the time zone for the run (use the process time zone if not specified).
            time_zone = run.get("time_zone", process["time_zone"])

            now = datetime.now(tz=timezone(time_zone))

            if timetravel_now:
                now = aniso8601.parse_datetime(timetravel_now)

            logger.info(f"Now: {now}")

            # Calculate the start and end of the task window for the run.
            run_window_start = reference_times[task_window_reference](now)
            run_window_end = run_window_start + parse_duration(task_window_duration)

            # Get the time frame configuration for the current run.
            time_frame = time_frames[run["time_frame"]]

            # Set up a cron iterator for the run.
            runs = croniter.croniter(run["run_at"], run_window_start)

            # Get the next run timestamp.
            run_timestamp = runs.get_next(datetime)
            if (previous_timestamp := runs.get_prev(datetime)) == now:
                run_timestamp = previous_timestamp
            else:
                _ = runs.get_next(datetime)

            logger.info(f"Next run of {run['@id']} at: {run_timestamp}")

            if not (run_window_start <= run_timestamp <= run_window_end):
                logger.info(f"Run at {run_timestamp} not in window [{run_window_start}/{run_window_end}]: {run['@id']}")

            # Loop through each timestamp in the current run.
            while run_timestamp <= run_window_end:
                logger.info(f"Run at {run_timestamp} in window [{run_window_start}/{run_window_end}]: {run['@id']}")

                # Get the reference time for the current timestamp in the time frame.
                reference_time_start = reference_times[time_frame["reference_time_start"]](run_timestamp)
                reference_time_end = reference_times[time_frame["reference_time_end"]](run_timestamp)

                # Change reference time according to time shift config.
                if process_time_shift:
                    reference_time_start = reference_time_start + parse_duration(process_time_shift)
                    reference_time_end = reference_time_end + parse_duration(process_time_shift)

                # Calculate the start and end of the period for the current task.
                job_period_start = reference_time_start + parse_duration(time_frame["period_start"])
                job_period_end = reference_time_end + parse_duration(time_frame["period_end"])

                # Convert the period start and end times to UTC.
                job_period_start_utc = convert_to_utc(job_period_start)
                job_period_end_utc = convert_to_utc(job_period_end)

                # Calculate the open and close times for the gate (the window of time before the job period start time during which the job/tasks must be done).
                gate_open_utc = job_period_start_utc - parse_duration(run["gate_open"])
                gate_close_utc = job_period_start_utc - parse_duration(run["gate_close"])

                # Set up a cron iterator for the data timestamps.
                timestamps_utc = croniter.croniter(run["data_timestamps"], job_period_start_utc)

                # Get the next data timestamp.
                timestamp_utc = timestamps_utc.next(datetime)

                # Generate a unique ID for the job (a collection of tasks generated for a single run instance).
                job_id = str(uuid4())

                # Loop through each data timestamp in the current period.
                while timestamp_utc < job_period_end_utc:

                    # Shift must be applied in local time, due to daylight saving
                    schedule_start_utc = convert_to_utc(convert_to_timezone(timestamp_utc) + parse_duration(TASK_SCHEDULE_SHIFT))
                    schedule_end_utc = schedule_start_utc + parse_duration("PT15M")

                    task_id = str(uuid4())
                    task_timestamp = utcnow().isoformat()

                    logger.info(f"Task {timestamp_utc} in window [{job_period_start_utc}/{job_period_end_utc}] for job: {job_id}")

                    task = {
                        "@context": "https://example.com/task_context.jsonld",
                        "@type": "Task",
                        "@id": f"urn:uuid:{task_id}",
                        "process_id": run.get("process_id", process.get("@id", None)),
                        "run_id": run.get("@id", None),
                        "job_id": f"urn:uuid:{job_id}",
                        "task_type": "automatic",
                        "task_initiator": getlogin(),
                        "task_priority": run.get("priority", process.get("priority", "normal")),  # "low", "normal", "high"
                        "task_creation_time": task_timestamp,
                        "task_update_time": "",
                        "task_status": "",
                        "task_status_trace": [],
                        "task_dependencies": [],
                        "task_tags": [],
                        "task_retry_count": 0,
                        "task_timeout": "PT1H",
                        "task_gate_open": gate_open_utc.isoformat(),
                        "task_gate_close": gate_close_utc.isoformat(),
                        "job_period_start": job_period_start_utc.isoformat(),
                        "job_period_end": job_period_end_utc.isoformat(),
                        "task_properties": {
                            "timestamp_utc": f"{timestamp_utc:%Y-%m-%dT%H:%M}",
                            "reference_schedule_start_utc": f"{schedule_start_utc:%Y-%m-%dT%H:%M}",
                            "reference_schedule_end_utc": f"{schedule_end_utc:%Y-%m-%dT%H:%M}",
                            "reference_schedule_time_horizon": TASK_SCHEDULE_TIME_HORIZON
                        }
                    }

                    # Update properties
                    task["task_properties"].update(process.get("properties", {}))
                    task["task_properties"].update(run.get("properties", {}))

                    # Update tags
                    task["task_tags"].extend(process.get("tags", []))
                    task["task_tags"].extend(run.get("tags", []))

                    # Check if task already exists, then set version number accordingly
                    set_task_version(task=task)

                    # Update task status
                    update_task_status(task=task, status_text="created")

                    # Return Task
                    yield task

                    logger.debug(json.dumps(task, indent=4))

                    # Next Task
                    timestamp_utc = timestamps_utc.get_next(datetime)

                # Next Run
                run_timestamp = runs.get_next(datetime)


def flatten_dict(nested_dict: dict, parent_key: str = '', separator: str = '.'):
    """
    Flattens a nested dictionary.

    Parameters:
    - nested_dict (dict): The dictionary to flatten.
    - parent_key (str): The base key string used for recursion.
    - separator (str): The separator between parent and child keys.

    Returns:
    - dict: A flattened dictionary where nested keys are concatenated into a single string.
    """
    items = []
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, separator=separator).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}[{i}]", separator=separator).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
    return dict(items)


def filter_and_flatten_dict(nested_dict: dict, keys: list):
    """
    Creates a new flat dictionary from specified keys.

    Parameters:
    - nested_dict (dict): The original nested dictionary.
    - keys (list): The list of keys to include in the new flat dictionary.

    Returns:
    - dict: A new flat dictionary with only the specified keys.
    """
    flattened = flatten_dict(nested_dict)
    return {key: flattened[key] for key in keys if key in flattened}


def update_task_status(task: dict, status_text: str, publish: bool = True):
    """Update task status
    Will update task_update_time
    Will update task_status
    Will append new status to task_status_trace"""

    logger.info(f"Updating Task status to: {status_text}")

    utc_now = datetime.utcnow().isoformat()

    task["task_update_time"] = utc_now
    task["task_status"] = status_text

    task["task_status_trace"].append({
        "status": status_text,
        "timestamp": utc_now
    })

    # TODO - better handling if elk is not available, possibly set elk connection timeout really small or refactor the sending to happen via rabbit
    if publish:
        try:
            for task in tasks:
                Elastic.send_to_elastic(
                    index=TASK_ELK_INDEX,
                    json_message=task,
                    id=task.get("@id")
                )
        except Exception as e:
            logger.warning(f"Task publication to Elastic failed with error: {e}")


def set_task_version(task: dict):

    # Check versioning mode
    auto_versioning_enabled = False
    if task['task_properties']['version'] == 'AUTO':
        logger.debug("Task versioning set to AUTO mode")
        auto_versioning_enabled = True

    query = {
        "bool": {
            "must": [
                {"match": {"task_properties.timestamp_utc": task['task_properties']['timestamp_utc']}},
                {"term": {"task_properties.time_horizon.keyword": task['task_properties']['time_horizon']}},
                {"term": {"task_properties.merge_type.keyword": task['task_properties']['merge_type']}},
            ]
        }
    }

    service = Elastic()
    try:
        updated_version = None
        tasks_df = service.get_docs_by_query(index=TASK_ELK_INDEX, query=query)

        if tasks_df.empty:
            logger.info(f"No previous runs found for task, using version from configuration: {task['task_properties']['version']}")
            if auto_versioning_enabled:
                logger.info("Task versioning mode 'AUTO', defaulting to: '001'")
                updated_version = '001'
        else:
            latest_version = tasks_df['task_properties.version'].max()
            logger.info(f"Latest available task version: {latest_version}")
            if auto_versioning_enabled:
                updated_version = str(int(latest_version) + 1).zfill(3)
            elif int(latest_version) >= int(task['task_properties']['version']):
                logger.warning("Latest available version is equal or higher than defined in task, increasing from latest")
                updated_version = str(int(latest_version) + 1).zfill(3)
            else:
                logger.info("Using version for task configuration")
                updated_version = task['task_properties']['version']

        if updated_version:
            task['task_properties']['version'] = updated_version
            logger.info(f"Version set to: {updated_version}")

    except Exception as e:
        logger.warning("Elastic query for task versioning unsuccessful, version not updated")
        logger.warning(f"Exception traceback: {e}")


if __name__ == "__main__":
    import sys
    import pandas
    logging.basicConfig(
        stream=sys.stdout,
        format='%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s',
        level=logging.DEBUG
    )

    task_window_duration = "P1D"
    task_window_reference = "currentDayStart"
    timeframe_conf = config.paths.task_generator.timeframe_conf
    process_conf = config.paths.task_generator.process_conf

    timeframe_config_json = json.load(timeframe_conf)
    process_config_json = json.load(process_conf)

    tasks = list(generate_tasks(task_window_duration, task_window_reference, process_config_json, timeframe_config_json))

    tasks_table = pandas.json_normalize(tasks)
    print(tasks_table["process_id"].value_counts())
    print(tasks_table["run_id"].value_counts())

# https://example.com/runs/IntraDayCGM/1      24
# https://example.com/runs/DayAheadCGM        24
# https://example.com/runs/DayAheadRMM        24
# https://example.com/runs/TwoDaysAheadCGM    24
# https://example.com/runs/IntraDayRMM/1      24
# https://example.com/runs/TwoDaysAheadRMM    24
# https://example.com/runs/IntraDayCGM/2      16
# https://example.com/runs/IntraDayRMM/2      16
# https://example.com/runs/IntraDayCGM/3       8
# https://example.com/runs/IntraDayRMM/3       8
