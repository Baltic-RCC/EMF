from time_helper import datetime, parse_duration, convert_to_utc, timezone, reference_times
from uuid import uuid4
import croniter
from pathlib import Path
import json
import logging
from os import getlogin

logger = logging.getLogger()


def generate_tasks(task_window_duration:str, task_window_reference:str, process_conf:str, timeframe_conf:str):
    """
    Generates a sequence of tasks based on the given process configuration and time frame definitions.

    Args:
        task_window_duration (str): ISO-8601 duration string specifying the duration of the time window
            within which the tasks are generated.
        task_window_reference (str): a string identifying the reference point in time for the task window,
            as defined in the `reference_times` dict imported from `time_helper.py`.
        process_conf (str): path to a JSON file specifying the process configuration, as described in the
            documentation.
        timeframe_conf (str): path to a JSON file specifying the time frame definitions, as described in the
            documentation.

    Yields:
        dict: A dictionary representing a task instance, as described in the documentation.

    Raises:
        ValueError: If the configuration is invalid or incomplete.

    """

    # TODO - add validation against schema

    # Load the time frame configuration from the specified file.
    time_frames = json.loads(Path(timeframe_conf).read_text())

    # Convert the list of time frames to a dictionary for easier access.
    time_frames = {time_frame["@id"].split("/")[-1]: time_frame for time_frame in time_frames}

    # Load the process configuration from the specified file.
    process = json.loads(Path(process_conf).read_text())

    # Loop through each run in the process configuration.
    for run in process["runs"]:

        # TODO -  check if configuration is valid for given period

        # Get the time zone for the run (use the process time zone if not specified).
        time_zone = run.get("time_zone", process["time_zone"])

        # Calculate the start and end of the task window for the run.
        run_window_start = reference_times[task_window_reference](datetime.now(tz=timezone(time_zone)))
        run_window_end = run_window_start + parse_duration(task_window_duration)

        # Get the time frame configuration for the current run.
        time_frame = time_frames[run["time_frame"]]

        # Set up a cron iterator for the run.
        runs = croniter.croniter(run["run_at"], run_window_start)

        # Get the next run timestamp.
        run_timestamp = runs.next(datetime)

        # Loop through each timestamp in the current run.
        while run_timestamp < run_window_end:

            # Get the reference time for the current timestamp in the time frame.
            reference_time = reference_times[time_frame["reference_time"]](run_timestamp)

            # Calculate the start and end of the period for the current task.
            job_period_start = reference_time + parse_duration(time_frame["period_start"])
            job_period_end = job_period_start + parse_duration(time_frame["period_duration"])

            # Convert the period start and end times to UTC.
            job_period_start_utc = convert_to_utc(job_period_start)
            job_period_end_utc = convert_to_utc(job_period_end)

            # Calculate the open and close times for the gate (the window of time before the job period start time during wich the job/tasks must be done).
            gate_open_utc = job_period_start_utc - parse_duration(run["gate_open"])
            gate_close_utc = job_period_start_utc - parse_duration(run["gate_close"])

            # Set up a cron iterator for the data timestamps.
            timestamps_utc = croniter.croniter(run["data_timestamps"], job_period_start_utc)
            print(job_period_start_utc)

            # Get the next data timestamp.
            timestamp_utc = timestamps_utc.next(datetime)
            print(timestamp_utc)

            # Generate a unique ID for the job (a collection of tasks generated for a single run instance).
            job_id = str(uuid4())

            # Loop through each data timestamp in the current period.
            while timestamp_utc < job_period_end_utc:

                task_id = str(uuid4())
                task_timestamp = datetime.utcnow().isoformat()

                logger.info(f"Generating task with ID: {task_id} for {run.get('@id', None)}")

                task = {
                      "@context": "https://example.com/task_context.jsonld",
                      "@type": "Task",
                      "@id": f"urn:uuid:{task_id}",
                      "process_id": process.get("@id", None),
                      "run_id": run.get("@id", None),
                      "job_id": f"urn:uuid:{job_id}",
                      "task_type": "automatic",
                      "task_initiator": getlogin(),
                      "task_priority": run.get("priority", process.get("priority", "normal")),  # "low", "normal", "high"
                      "task_creation_time": task_timestamp,
                      "task_status": "created",
                      "task_status_trace": [{"status": "created", "timestamp": task_timestamp}],
                      "task_dependencies": [],
                      "task_tags": [],
                      "task_retry_count": 0,
                      "task_timeout": "PT1H",
                      "task_gate_open": gate_open_utc.isoformat(),
                      "task_gate_close": gate_close_utc.isoformat(),
                      "job_period_start": job_period_start_utc.isoformat(),
                      "job_period_end": job_period_end_utc.isoformat(),
                      "task_properties": {
                        "timestamp_utc": timestamp_utc.isoformat()
                      }
                    }

                # Update properties
                task["task_properties"].update(process.get("properties", {}))
                task["task_properties"].update(run.get("properties", {}))

                # Update tags
                task["task_tags"].extend(process.get("tags", []))
                task["task_tags"].extend(run.get("tags", []))

                # Return Task
                yield task

                logger.debug(json.dumps(task, indent=4))

                # Next Task
                timestamp_utc = timestamps_utc.next(datetime)
            # Next Run
            run_timestamp = runs.next(datetime)


if __name__ == "__main__":
    import sys
    import pandas
    logging.basicConfig(
        stream=sys.stdout,
        format='%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s',
        level=logging.INFO
    )

    task_window_duration = "P1D"
    task_window_reference = "currentDayStart"
    timeframe_conf = "../../config/task_generator/timeframe_conf.json"
    process_conf = "../../config/task_generator/process_conf.json"

    tasks = list(generate_tasks(task_window_duration, task_window_reference, process_conf, timeframe_conf))

    tasks_table = pandas.json_normalize(tasks)
    print(tasks_table["run_id"].value_counts())

    # Result should be like this
    # https://example.com/runs/DayAheadCGM        24
    # https://example.com/runs/TwoDaysAheadCGM    24
    # https://example.com/runs/IntraDayCGM/1       8
    # https://example.com/runs/IntraDayCGM/2       8
    # https://example.com/runs/IntraDayCGM/3       8
