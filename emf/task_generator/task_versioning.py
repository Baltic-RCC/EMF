import logging

import pandas as pd
import config
from emf.common.integrations.elastic import Elastic
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.task_generator.task_generator)


def set_task_version(task: dict):
    """
    Sets task['task_properties']['version'], either by incrementing the latest version found in
    Elastic for this timestamp_utc/time_horizon/merge_type combination ('auto' mode - blank counts
    as 'auto' too, since that's the shipped default for manually-triggered tasks), or by using the
    version already provided in the task's configuration.
    """

    task_version = task['task_properties']['version']

    auto_versioning_enabled = task_version.strip().lower() in ('', 'auto')
    if auto_versioning_enabled:
        logger.debug("Task versioning set to automatic")

    try:
        tasks_df = _get_matching_tasks(task)
    except Exception as error:
        # Elastic itself was unreachable/errored - degrade gracefully as before.
        logger.warning(f"Elastic query for task versioning unsuccessful: {error}")
        if auto_versioning_enabled:
            task['task_properties']['version'] = None
            logger.error("Elastic query for task versioning unsuccessful, version not set")
        else:
            logger.warning("Elastic query for task versioning unsuccessful, using provided value")
        return

    try:
        if tasks_df.empty:
            logger.info("No previous runs found for this task")
            set_version = '001' if auto_versioning_enabled else str(int(task_version)).zfill(3)
        else:
            # Get latest task available version from ELK
            latest_version = pd.to_numeric(tasks_df['task_properties.version']).max()
            logger.info(f"Latest available task version: {latest_version}")

            if auto_versioning_enabled:
                set_version = str(int(latest_version) + 1).zfill(3)
            elif int(latest_version) >= int(task_version):
                logger.warning("Latest version is equal or lower than task config, incrementing from latest")
                set_version = str(int(latest_version) + 1).zfill(3)
            else:
                logger.info("Using version from task config")
                set_version = str(int(task_version)).zfill(3)

        task['task_properties']['version'] = set_version
        logger.info(f"Version set to: '{set_version}'")

    except Exception as error:
        logger.error(f"Task versioning failed unexpectedly: {error}", exc_info=True)
        task['task_properties']['version'] = None if auto_versioning_enabled else task_version


def _get_matching_tasks(task: dict) -> pd.DataFrame:
    """Query Elastic for previous tasks with the same timestamp_utc/time_horizon/merge_type."""

    service = Elastic()
    query = {
        "bool": {
            "must": [
                {"match": {"task_properties.timestamp_utc": task['task_properties']['timestamp_utc']}},
                {"term": {"task_properties.time_horizon.keyword": task['task_properties']['time_horizon']}},
                {"term": {"task_properties.merge_type.keyword": task['task_properties']['merge_type']}},
            ]
        }
    }
    tasks_df = service.get_docs_by_query(index=TASK_ELK_INDEX, query=query)

    # Filter out non-integer version values.
    if not tasks_df.empty:
        num = pd.to_numeric(tasks_df["task_properties.version"], errors="coerce")
        tasks_df = tasks_df[num.notna() & (num % 1 == 0)]

    return tasks_df
