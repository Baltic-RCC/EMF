import logging
import config
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elastic
from emf.common.converters import iec_schedule_to_ndjson

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.schedule_retriever.schedule_retriever)


def transfer_schedules_from_opde_to_elk():
    message_types = EDX_MESSAGE_TYPE.split(",")
    elk_handler = elastic.Handler(index=ELK_INDEX_PATTERN)
    service = edx.EDX(converter=iec_schedule_to_ndjson, handler=elk_handler, message_types=message_types)
    service.run()


if __name__ == "__main__":
    # Testing
    import sys
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Get schedules from OPDE
    transfer_schedules_from_opde_to_elk()
