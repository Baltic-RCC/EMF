import logging
import config
import pandas as pd
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import edx, elk_batch_send, elastic
from emf.common.converters import iec_schedule_to_ndjson

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.schedule_retriever.schedule_retriever)

area_to_eic_map = {
    "10YLT-1001A0008Q": "LT",
    "10YLV-1001A00074": "LV",
    "10Y1001A1001A39I": "EESTI",
    "10YPL-AREA-----S": "PL",
    "10YSE-1--------K": "SE",
    "10YDK-2--------M": "DK2",
}


def transfer_schedules_from_opde_to_elk():
    message_types = EDX_MESSAGE_TYPE.split(",")
    elk_handler = elk_batch_send.Handler(url=ELK_SERVER, index=ELK_INDEX_PATTERN)
    service = edx.EDX(converter=iec_schedule_to_ndjson, handler=elk_handler, message_types=message_types)
    service.run()


def query_schedules_from_elk(metadata: dict) -> pd.DataFrame | None:
    """
    Method to get schedule from ELK by given metadata dictionary
    :param metadata: dict of metadata
    :return: dataframe
    """
    # Create service
    service = elastic.Elk(server=ELK_SERVER)

    # Build Elk query from given metadata
    query = {"bool": {"must": [{f"match": {k: v}} for k, v in metadata.items()]}}

    # Query documents
    try:
        schedules_df = service.get_docs_by_query(index=ELK_INDEX_PATTERN, size=1000, query=query)
        if schedules_df.empty:
            return None
    except Exception as e:
        logger.warning(f"Query returned error -> {e}")
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_to_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_to_eic_map)

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

    # Get HVDC schedules
    schedules_df = query_schedules_from_elk(metadata=metadata)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.connectingLine_RegisteredResource.mRID"]
    schedules_df = schedules_df[_cols]
    schedules_df.rename(columns={"TimeSeries.connectingLine_RegisteredResource.mRID": "registered_resource"}, inplace=True)
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


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

    # Get AC area schedules
    schedules_df = query_schedules_from_elk(metadata=metadata)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain"]
    schedules_df = schedules_df[_cols]
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


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

    # Get schedules from ELK
    result = query_hvdc_schedules(process_type="A01", start="2023-08-23T16:00:00", end="2023-08-23T17:00:00")
    # TODO start time is less or equal end time is greater or equal
    print(result)