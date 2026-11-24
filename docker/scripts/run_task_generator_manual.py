"""
Manual task generation against the local stack (docker/compose.yaml + docker/compose.services.yaml).

Runs emf/task_generator/worker.py in TASK_GENERATION_MODE = manual with the parameters set below and publishes the
generated tasks to the local RabbitMQ, where they are routed by process_id:
    CGM_CREATION runs -> emf.merge.tasks.cgm    RMM_CREATION runs -> emf.merge.tasks.rmm

Every key below is a setting from config/task_generator/task_generator.properties, passed as an environment
variable of the same name (which overrides the properties file). A blank value ("") keeps the value from the
properties file - for the passthrough run properties that means the run's default from process_conf.json.

Usage, from the repository root with the project virtualenv:
    .venv\\Scripts\\python.exe docker\\scripts\\run_task_generator_manual.py            # generate and publish
    .venv\\Scripts\\python.exe docker\\scripts\\run_task_generator_manual.py --list     # list RUN_TYPE values
"""
import json
import os
import runpy
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# --- Manual task parameters ------------------------------------------------------------------------------------------
TASK_PARAMETERS = {
    # Run to trigger: the last part of a run @id in config/task_generator/process_conf.json (see --list),
    # e.g. DayAheadCGM, TwoDaysAheadCGM, IntraDayCGM/1, DayAheadRMM, IntraDayRMM/EOD, WeekAheadRMM
    "RUN_TYPE": "DayAheadCGM",

    # Comma separated TSO lists, e.g. "AST,ELERING,LITGRID". INCLUDED_TSO is required for RMM run types
    "INCLUDED_TSO": "",
    "EXCLUDED_TSO": "",
    "LOCAL_IMPORT": "",             # TSOs whose models are taken from local import instead of OPDM
    "REPLACE_TSO": "",              # TSOs whose models are replaced

    # Scenario time. Blank: the run's last scheduled slot (or "now" for MANUAL_NOW_REFERENCE_RUN_TYPES).
    # Set TIMESTAMP (e.g. "2026-11-25T10:30+0100") to generate tasks for one hour from it - then
    # TASK_REFERENCE_TIME is required: currentHourStart, currentDayStart, currentWeekStart, currentMonthStart,
    # currentYearStart
    "TIMESTAMP": "",
    "TASK_REFERENCE_TIME": "",
    "PROCESS_TIME_SHIFT": "",       # ISO 8601 duration applied to the run time, e.g. "P1D", "-PT2H"

    "TASK_VERSION": "",             # blank or "auto" - next version found in Elasticsearch; or e.g. "002"
    "TASK_MERGING_ENTITY": "",      # e.g. BALTICRCC
    "TASK_INITIATOR": "",           # who triggered the task; blank - USER/USERNAME environment variable

    # Run properties ("True"/"False"); blank keeps the run's default from process_conf.json
    "RUN_REPLACEMENT": "",
    "RUN_SCALING": "",
    "OUTAGE_UPDATE": "",
    "FORCE_OUTAGE_FIX": "",
    "UPLOAD_TO_OPDM": "False",      # OPDM is not part of the local stack
    "UPLOAD_TO_MINIO": "True",
    "SEND_MERGE_REPORT": "True",
    "POST_TEMP_FIXES": "",
    "LVL8_REPORTING": "False",      # sent through EDX, not part of the local stack
    "DEBUG": "",
}

# --- Local stack connection (see docker/README.md) -------------------------------------------------------------------
API_KEY_FILE = REPO_ROOT / "docker" / ".secrets" / "elasticsearch_api_key"

CONNECTION = {
    "TASK_GENERATION_MODE": "manual",
    # Exchange from docker/rabbitmq/emfos.def.json - routes tasks to the merger queues by process_id
    "RMQ_EXCHANGE": "emf.merge.tasks",
    "RMQ_SERVER": "localhost",
    "RMQ_PORT": "5672",
    "RMQ_USERNAME": "emf",
    "RMQ_PASSWORD": "emf",
    "ELK_SERVER": "http://localhost:9200",
    "ELK_SSL_VERIFY": "False",
}


def list_run_types():
    processes = json.loads((REPO_ROOT / "config" / "task_generator" / "process_conf.json").read_text())
    for process in processes:
        print(process["@id"])
        for run in process.get("runs", []):
            properties = run.get("properties", {})
            print(f"    {run['@id'].rsplit('/runs/', 1)[-1]:<20} time_frame={run['time_frame']:<5} "
                  f"merge_type={properties.get('merge_type', '')}")


def main():
    if "--list" in sys.argv[1:]:
        list_run_types()
        return

    if not API_KEY_FILE.is_file():
        sys.exit(f"{API_KEY_FILE} not found - start the local stack first (podman compose up -d in docker/)")

    os.environ.update(CONNECTION)
    os.environ["ELK_TOKEN"] = API_KEY_FILE.read_text().strip()
    for key, value in TASK_PARAMETERS.items():
        if value:
            os.environ[key] = value
        else:
            # Blank falls back to the properties file; drop a value inherited from the shell
            os.environ.pop(key, None)

    print("Manual task parameters:")
    for key, value in TASK_PARAMETERS.items():
        if value:
            print(f"  {key} = {value}")

    # Settings are read when the EMF modules are imported, so the environment has to be set before running the worker
    os.chdir(REPO_ROOT)
    sys.path.insert(0, str(REPO_ROOT))
    runpy.run_path(str(REPO_ROOT / "emf" / "task_generator" / "worker.py"), run_name="__main__")


if __name__ == "__main__":
    main()
