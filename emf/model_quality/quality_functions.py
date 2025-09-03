from io import BytesIO
from zipfile import ZipFile
import logging
import config
from emf.common.config_parser import parse_app_properties
from emf.model_quality.quality_rules import *

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)


def generate_quality_report(handler, network, object_type, model_metadata, rule_sets, tieflow_data=None):

    report = {}

    if object_type == "CGM" and model_metadata['pmd:Area'] == 'BA':

        report = check_generator_quality(report, network)
        report = check_lt_pl_crossborder(report, network, tieflow_data=tieflow_data, border_limit=BORDER_LIMIT)
        report = check_crossborder_inconsistencies(report, network)
        report = check_outage_inconsistencies(report, network, handler, model_metadata)
        report = check_reactive_power_limits(report, network)

        report = set_quality_flag(report, object_type, rule_sets)

    elif object_type == "IGM":

        tso = model_metadata[0]['pmd:TSO']
        if tso in ['LITGRID', 'AST', 'ELERING']:
            report = check_line_limits(report, network, handler, limit_temperature=LINE_LIMIT_TEMPERATURE)
        else:
            report.update({"line_rating_mismatch": None, "line_rating_check": None})
        report = check_line_impedance(report, network)
        report = set_quality_flag(report, object_type, rule_sets)

    else:
        report.update({"quality": 'no status'})

    return report


def set_common_metadata(model_metadata, object_type):
    metadata = {}
    if object_type == "IGM":
        opdm_object = model_metadata[0]
        metadata['object_type'] = object_type
        metadata['@scenario_timestamp'] = opdm_object['pmd:scenarioDate']
        metadata['@time_horizon'] = opdm_object['pmd:timeHorizon']
        metadata['@version'] = int(opdm_object['pmd:versionNumber'])
        metadata['content_reference'] = opdm_object['pmd:content-reference']
        metadata['tso'] = opdm_object['pmd:TSO']
        metadata['minio_bucket'] = opdm_object['minio-bucket']

    elif object_type == "CGM":
        opdm_object = model_metadata
        metadata['object_type'] = object_type
        metadata['@scenario_timestamp'] = opdm_object['pmd:scenarioDate']
        metadata['@time_horizon'] = opdm_object['pmd:timeHorizon']
        metadata['@version'] = int(opdm_object['pmd:versionNumber'])
        metadata['merge_type'] = opdm_object['pmd:Area']
        metadata['content_reference'] = opdm_object['pmd:content-reference']
        # metadata['minio_bucket'] = opdm_object.get('minio-bucket', 'opde-confidential-models')
        metadata['minio_bucket'] = opdm_object.get('minio-bucket')

    return metadata


# TODO temp function, later use common one
def get_uap_outages_from_scenario_time(handler, time_horizon, model_timestamp, index='opc-outages-baltics*'):

    import datetime

    logger.info(f"Retrieving outages from ELK index: '{index}'")

    # now represents the time of the run, in P0W case it should be current time
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%dT%H:%M") + "Z"

    if time_horizon.isdigit() or time_horizon in ['ID', '1D', '2D']:
        time_horizon = 'WK'

    if time_horizon == 'WK':
        merge_type_list = ['week']
        filter_range = '2w'
    elif time_horizon == 'MO':
        merge_type_list = ['week', 'month']
        filter_range = '4w'
    elif time_horizon == 'YR':
        merge_type_list = ['year']
        filter_range = '4M'
    else:
        raise TypeError('Incorrect time horizon')

    query = {
        "bool": {
            "must": [
                {"exists": {"field": "name"}},
                {"terms": {"Merge": merge_type_list}},
            ],
            "filter": [{"range": {"reportParsedDate": {"lte": now, "gte": f"now-{filter_range}"}}}],
        }
    }
    response = handler.elastic_service.get_docs_by_query(index=index, query=query, size=10000, return_df=True)
    outage_df = pd.DataFrame()

    eic_mrid_map = handler.elastic_service.get_docs_by_query(index='config-network', query={"match_all": {}}, size=10000, return_df=True)

    if not response.empty:

        # Get only latest report data
        response['reportParsedDate'] = pd.to_datetime(response['reportParsedDate'])
        response = response[response['reportParsedDate'] == response['reportParsedDate'].max()]
        # Only keep latest outages
        duplicated_outages = response[response.duplicated('eic', keep=False)]
        latest_duplicate = duplicated_outages.groupby('eic')['date_of_last_change'].idxmax()
        response = response.loc[response.index.isin(latest_duplicate) | ~response['eic'].duplicated(keep=False)]

        response = response[response['outage_type'].isin(['OUT'])]

        response = response.sort_values(by=['eic', 'start_date', 'end_date']).reset_index(drop=True)
        last_end_time = {}

        # Remove outage duplicate if there is time overlap
        for _, row in response.iterrows():
            eic = row['eic']
            start_time = row['start_date']
            end_time = row['end_date']

            if eic not in last_end_time or start_time > last_end_time[eic]:
                outage_df = pd.concat([outage_df, pd.DataFrame([row])], ignore_index=True)
                last_end_time[eic] = end_time

    BRELL_LINES = ['10T-LT-RU-00001W', '10T-LT-RU-00002U', '10T-LT-RU-00003S', '10T-LV-RU-00001A',
                   '10T-LV-RU-00001A', '10T-BY-LT-000053', '10T-BY-LT-00001B', '10T-BY-LT-000029',
                   '10T-EE-RU-00001M', '10T-EE-RU-00002K', '10T-EE-RU-00003I', '10T-BY-LT-000045']

    outage_df = outage_df[~outage_df['eic'].isin(BRELL_LINES)].copy()

    model_scenario_time = datetime.datetime.fromisoformat(model_timestamp)
    if model_scenario_time.tzinfo is None:
        logger.warning("model_scenario_time is timezone naive, assuming UTC")
        model_scenario_time = model_scenario_time.replace(tzinfo=datetime.timezone.utc)
    elif model_scenario_time.tzinfo != datetime.timezone.utc:
        logger.warning(f"Converting model_scenario_time from {model_scenario_time.tzinfo} to UTC")
        model_scenario_time = model_scenario_time.astimezone(datetime.timezone.utc)

    outage_df['start_date'] = pd.to_datetime(outage_df['start_date'], utc=True)
    outage_df['end_date'] = pd.to_datetime(outage_df['end_date'], utc=True)


    relevant_mask = ((outage_df['start_date'] <= model_scenario_time) & (outage_df['end_date'] >= model_scenario_time))

    # Use .loc for boolean indexing and .copy() to avoid SettingWithCopyWarning later
    relevant_outages = outage_df.loc[relevant_mask].copy()

    result = relevant_outages.merge(eic_mrid_map, on='eic', how='left')

    missing_outages = ', '.join(result[result['mrid'].isna()]['name'].to_list())
    if missing_outages:
        logger.error(f"Missing mrid of outages: {missing_outages}")

    return result


def process_zipped_cgm(zipped_bytes, processed=None):
    if processed is None:
        processed = []
    with ZipFile(BytesIO(zipped_bytes)) as zf:
        for name in zf.namelist():
            with zf.open(name) as file:
                content = file.read()
                if name.endswith('.zip'):
                    process_zipped_cgm(content, processed)
                elif name.endswith('.xml'):
                    file_object = BytesIO(content)
                    file_object.name = name
                    processed.append(file_object)

    return processed


def set_quality_flag(report, object_type, rule_dict):

    if object_type == 'CGM':
        rule_list = rule_dict.get('cgm_rule_set')
    elif object_type == 'IGM':
        rule_list = rule_dict.get('igm_rule_set')
    else:
        rule_list = []

    rule_list = [(rule + '_check') for rule in rule_list]
    rule_flags = [report[flag] for flag in rule_list if flag in report]

    if all(flag is True for flag in rule_flags):
        report.update({"quality": 'good'})
    elif any(flag is None for flag in rule_flags) and any(flag is not False for flag in rule_flags):
        report.update({"quality": 'semi-good'})
    else:
        report.update({"quality": 'bad'})

    return report