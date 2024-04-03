import json
import config
import logging
import sys
from emf.task_generator.task_generator import generate_tasks
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties

logging.basicConfig(stream=sys.stdout,
                    format="%(levelname) -10s %(asctime) -10s %(name) -35s %(funcName) -30s %(lineno) -5d: %(message)s",
                    level=logging.INFO)

logger = logging.getLogger()

parse_app_properties(globals(), config.paths.task_generator.task_generator)

tasks = list(generate_tasks(TASK_WINDOW_DURATION, TASK_WINDOW_REFERENCE, PROCESS_CONF, TIMEFRAME_CONF))

if tasks:
    rabbit_service = rabbit.BlockingClient()
    logger.info(f"Sending tasks to Rabbit exchange '{RMQ_EXCHANGE}'")
    for task in tasks:
        rabbit_service.publish(json.dumps(task), RMQ_EXCHANGE)
else:
    logger.info("No tasks generated at current time.")
