from emf.common.integrations.object_storage import *
from emf.common.integrations.object_storage.models import query_data
from functools import partial
import logging
logger = logging.getLogger(__name__)


def publish_tasks(tasks: list, index=ELASTIC_TASKS_INDEX):

    for task in tasks:
        logger.info(f"Adding task to {ELASTIC_TASKS_INDEX}")
        elastic_service.send_to_elastic(
            index=index,
            json_message=task,
            id=task.get("@id")
        )


query_tasks = partial(query_data, index=ELASTIC_TASKS_INDEX)

