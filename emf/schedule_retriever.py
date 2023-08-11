import logging
import config
import pandas as pd
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elk_batch_send, elastic
from emf.common.converters import iec_schedule_to_ndjson

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.schedule_retriever.schedule_retriever)

def get_schedules_from_opde():
    elk_handler = elk_batch_send.Handler(url=ELK_SERVER, index=ELK_INDEX_PATTERN)
    service = edx.EDX(converter=iec_schedule_to_ndjson, handler=elk_handler, message_type=EDX_MESSAGE_TYPE)
    service.run()


def get_schedules_from_elk(index: str, metadata: dict):  # TODO we can leave flexible metadata list as input or limit to only relevant by separate arguments what is only needed to get schedules
    """
    businessType:
        B63 - HVDC schedule
        B64 - Balanced AC NP
    :param index:
    :param metadata:
    :return:
    """
    service = elastic.Elk(server="http://test-rcc-logs-master.elering.sise:9200")

    query = {"bool": {"must": [{f"match": {k: v}} for k, v in metadata.items()]}}

    try:
        schedules_df = service.get_docs_by_query(index=index, size=1000, query=query)
    except Exception as e:
        logger.warning(f"Query returned error -> {e}")
        schedules_df = pd.DataFrame()

    # TODO maybe better to make arguments that only request schedule for one timestamp
    # TODO groupby TimeSeries.connectingLine_RegisteredResource.mRID -> EIC of link which match to powsybl
    # TODO qeustion how to map direction. In PEVF all values are positive at each end. in powsybl we have element

    return schedules_df


if __name__ == "__main__":
    # Testing
    import sys
    logging.basicConfig(
        format='%(levelname) -10s %(asctime) -20s %(name) -35s %(funcName) -35s %(lineno) -5d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Get schedules from OPDE
    # get_schedules_from_opde()

    # Get schedules from ELK
    metadata = {'utc_start': '2023-08-08T23:00:00', 'utc_end': '2023-08-09T00:00:00', 'TimeSeries.businessType': 'B63'}
    # metadata = {'utc_start': '2023-08-08T23:00:00', 'utc_end': '2023-08-09T00:00:00'}
    result = get_schedules_from_elk(index="test-schedules-2023", metadata=metadata)