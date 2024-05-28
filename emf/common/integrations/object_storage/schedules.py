import config
import datetime
from typing import Dict, List, Union
from emf.common.integrations.object_storage import elastic_service


def query_hvdc_schedules(process_type: str,
                         utc_start: str | datetime,
                         utc_end: str | datetime,
                         area_eic_map: Dict[str, str] | None = None) -> dict | None:
    """
    Method to get HVDC schedules (business type - B63)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param utc_start: start time in utc. Example: '2023-08-08T23:00:00Z'
    :param utc_end: end time in utc. Example: '2023-08-09T00:00:00Z'
    :param area_eic_map: dictionary of geographical region names and control area eic code
    :return: schedules in dict format
    """
    # Define area name to eic mapping table
    if not area_eic_map:
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())

    # Define metadata dictionary
    metadata = {
        "process.processType": process_type,
        "TimeSeries.businessType": "B63",
    }

    # Get HVDC schedules
    schedules_df = elastic_service.query_schedules_from_elk(
        index=ELK_INDEX_PATTERN,
        utc_start=utc_start,
        utc_end=utc_end,
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # TODO filter out data by reason code that take only verified tada

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.connectingLine_RegisteredResource.mRID"]
    schedules_df = schedules_df[_cols]
    schedules_df.rename(columns={"TimeSeries.connectingLine_RegisteredResource.mRID": "registered_resource"},
                        inplace=True)
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


def query_acnp_schedules(process_type: str,
                         utc_start: str | datetime,
                         utc_end: str | datetime,
                         area_eic_map: Dict[str, str] | None = None) -> dict | None:
    """
    Method to get ACNP schedules (business type - B64)
    :param process_type: time horizon of schedules; A01 - Day-ahead, A18 - Intraday
    :param utc_start: start time in utc. Example: '2023-08-08T23:00:00Z'
    :param utc_end: end time in utc. Example: '2023-08-09T00:00:00Z'
    :return:
    """
    # Define area name to eic mapping table
    if not area_eic_map:
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())

    metadata = {
        "process.processType": process_type,
        "TimeSeries.businessType": "B64",
    }

    # Get AC area schedules
    schedules_df = elastic_service.query_schedules_from_elk(
        index=ELK_INDEX_PATTERN,
        utc_start=utc_start,
        utc_end=utc_end,
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Filter to the latest revision number
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain"]
    schedules_df = schedules_df[_cols]
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict