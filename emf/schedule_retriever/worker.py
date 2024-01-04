import logging
import config
import uuid
from emf.common.logging import custom_logger
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic
from emf.common.converters import iec_schedule_to_ndjson

# Initialize custom logger
custom_logger.initialize_custom_logger(extra={'worker': 'schedule-retriever', 'worker_uuid': str(uuid.uuid4())})
logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.schedule_retriever.schedule_retriever)

# Transfer schedules from EDX to Elk
message_types = EDX_MESSAGE_TYPE.split(",")
elk_handler = elastic.Handler(index=ELK_INDEX_PATTERN, id_from_metadata=True)
service = edx.EDX(converter=iec_schedule_to_ndjson, handler=elk_handler, message_types=message_types)
service.run()


if __name__ == "__main__":
    pass
