import logging
from datetime import datetime
import config
from emf.common.integrations.elastic import Elastic
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.task_generator.task_generator)


def update_task_status(task: dict, status_text: str, publish: bool = True):
    """Update task status
    Will update task_update_time
    Will update task_status
    Will append new status to task_status_trace"""

    logger.info(f"Updating task status to: {status_text}")

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
            Elastic.send_to_elastic(index=TASK_ELK_INDEX, json_message=task, id=task.get("@id"))
        except Exception as e:
            logger.error(f"Task publication to Elastic failed with error: {e}")


if __name__ == "__main__":
    pass