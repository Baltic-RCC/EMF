import config
import logging
from datetime import datetime, timedelta
from emf.common.integrations import elastic

logger = logging.getLogger(__name__)


def query_hvdc_schedules(time_horizon: str,
                         scenario_timestamp: str | datetime) -> dict | None:
    """
    Method to get HVDC schedules (business type - B63 for PEVF, B67 - for CGMA)
    :param time_horizon: time horizon of schedules
    :param scenario_timestamp: scenario timestamp in utc. Example: '2023-08-08T23:30:00Z'
    :return: DC schedules in dict format
    """
    # Create Elastic client
    service = elastic.Elastic()

    # Get area name to eic mapping
    try:
        area_eic_codes = service.get_docs_by_query(index='config-areas', query={'match_all': {}}, size=500)
        hvdc_eic_codes = service.get_docs_by_query(index='config-bds-lines', query={'match_all': {}}, size=500)
        area_eic_map = area_eic_codes.set_index('area.eic')['area.code'].to_dict()
        hvdc_eic_map = hvdc_eic_codes.set_index('IdentifiedObject.energyIdentCodeEic')['IdentifiedObject.description'].to_dict()
    except Exception as e:
        logger.warning(f"Eic mapping configuration retrieval failed, using default: {e}")
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())
        hvdc_eic_map = {}

    # Define utc start/end times from timestamp
    utc_start = datetime.fromisoformat(scenario_timestamp) - timedelta(minutes=30)
    utc_end = datetime.fromisoformat(scenario_timestamp) + timedelta(minutes=30)

    # Define business type by time horizon
    business_type = "B63" if time_horizon in ["1D", "ID"] else "B67"

    # Define metadata dictionary
    metadata = {
        "@time_horizon": time_horizon,
        "TimeSeries.businessType": business_type,
    }

    # Get HVDC schedules
    schedules_df = service.query_schedules_from_elk(
        index="emfos-schedules*",
        utc_start=utc_start.isoformat(),
        utc_end=utc_end.isoformat(),
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Map HVDC names
    schedules_df["hvdc_name"] = schedules_df["TimeSeries.connectingLine_RegisteredResource.mRID"].map(hvdc_eic_map)

    # Filter to the latest revision number
    schedules_df.revisionNumber = schedules_df.revisionNumber.astype(int)
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # TODO filter out data by reason code that take only verified data

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain", "TimeSeries.connectingLine_RegisteredResource.mRID", "hvdc_name"]
    schedules_df = schedules_df[_cols]
    schedules_df.rename(columns={"TimeSeries.connectingLine_RegisteredResource.mRID": "registered_resource"},
                        inplace=True)
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict


def query_acnp_schedules(time_horizon: str,
                         scenario_timestamp: str | datetime) -> dict | None:
    """
    Method to get ACNP schedules (business type - B64)
    :param time_horizon: time horizon of schedules
    :param scenario_timestamp: scenario timestamp in utc. Example: '2023-08-08T23:30:00Z'
    :return: AC schedules in dict format
    """
    # Create Elastic client
    service = elastic.Elastic()

    # Get area name to eic mapping
    try:
        area_eic_codes = service.get_docs_by_query(index='config-areas', query={'match_all': {}}, size=500)
        area_eic_map = area_eic_codes.set_index('area.eic')['area.code'].to_dict()
    except Exception as e:
        logger.warning(f"Eic mapping configuration retrieval failed, using default: {e}")
        # Using default mapping table from config
        import json
        with open(config.paths.cgm_worker.default_area_eic_map, "rb") as f:
            area_eic_map = json.loads(f.read())

    # Define utc start/end times from timestamp
    utc_start = datetime.fromisoformat(scenario_timestamp) - timedelta(minutes=30)
    utc_end = datetime.fromisoformat(scenario_timestamp) + timedelta(minutes=30)

    # Define metadata dictionary
    metadata = {
        "@time_horizon": time_horizon,
        "TimeSeries.businessType": "B64",
    }

    # Get AC area schedules
    schedules_df = service.query_schedules_from_elk(
        index="emfos-schedules*",
        utc_start=utc_start.isoformat(),
        utc_end=utc_end.isoformat(),
        metadata=metadata,
        period_overlap=True,
    )

    if schedules_df is None:
        return None

    # Map eic codes to area names
    schedules_df["in_domain"] = schedules_df["TimeSeries.in_Domain.mRID"].map(area_eic_map)
    schedules_df["out_domain"] = schedules_df["TimeSeries.out_Domain.mRID"].map(area_eic_map)

    # Filter to the latest revision number
    schedules_df.revisionNumber = schedules_df.revisionNumber.astype(int)
    schedules_df = schedules_df[schedules_df.revisionNumber == schedules_df.revisionNumber.max()]

    # Get relevant structure and convert to dictionary
    _cols = ["value", "in_domain", "out_domain"]
    schedules_df = schedules_df[_cols]
    schedules_dict = schedules_df.to_dict('records')

    return schedules_dict