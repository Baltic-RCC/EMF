import logging
import config
import pandas as pd
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elk_batch_send, elastic
from emf.common.converters import iec_schedule_to_ndjson

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.schedule_retriever.schedule_retriever)


def transfer_schedules_from_opde_to_elk():
    elk_handler = elk_batch_send.Handler(url=ELK_SERVER, index=ELK_INDEX_PATTERN)
    service = edx.EDX(converter=iec_schedule_to_ndjson, handler=elk_handler, message_type=EDX_MESSAGE_TYPE)
    service.run()


def query_schedules_from_elk(metadata: dict) -> pd.DataFrame | None:
    """
    Method to get schedule from ELK by given metadata dictionary
    :param metadata: dictionary or metadata
    :return: dataframe
    """
    # Create service
    service = elastic.Elk(server=ELK_SERVER)

    # Build Elk query from given metadata
    query = {"bool": {"must": [{f"match": {k: v}} for k, v in metadata.items()]}}

    # Query documents
    try:
        schedules_df = service.get_docs_by_query(index=ELK_INDEX_PATTERN, size=1000, query=query)
    except Exception as e:
        logger.warning(f"Query returned error -> {e}")
        return None

    # TODO groupby TimeSeries.connectingLine_RegisteredResource.mRID -> EIC of link which match to powsybl
    # TODO qeustion how to map direction. In PEVF all values are positive at each end. in powsybl we have element
    # TODO think how to get latest schedule (possible two queries first one to get latest element and seconf by last element creation time

    return schedules_df


def query_hvdc_schedules(process_type: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Method to get HVDC schedules (business type - B63)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param start: start time in utc. Example: '2023-08-08T23:00:00'
    :param end: end time in utc. Example: '2023-08-09T00:00:00'
    :return:
    """

    metadata = {
        "process.processType": process_type,
        "utc_start": start,
        "utc_end": end,
        "TimeSeries.businessType": "B63",
    }

    return query_schedules_from_elk(metadata=metadata)


def query_acnp_schedules(process_type: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Method to get ACNP schedules (business type - B64)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param start: start time in utc. Example: '2023-08-08T23:00:00'
    :param end: end time in utc. Example: '2023-08-09T00:00:00'
    :return:
    """

    metadata = {
        "process.processType": process_type,
        "utc_start": start,
        "utc_end": end,
        "TimeSeries.businessType": "B64",
    }

    return query_schedules_from_elk(metadata=metadata)


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
    result = query_hvdc_schedules(process_type="A01", start="2023-08-08T23:00:00", end="2023-08-09T00:00:00")