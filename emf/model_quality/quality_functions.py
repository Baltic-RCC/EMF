from io import BytesIO
from zipfile import ZipFile
import logging
import config
import pandas as pd
from emf.common.config_parser import parse_app_properties
from model_statistics import get_tieflow_data, type_tableview_merge

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)


def generate_quality_report(network, object_type, model_metadata):

    report = {}

    if object_type == "CGM" and model_metadata['pmd:Area'] == 'BA':

        # Check Kruonis generators
        generators = network.type_tableview('SynchronousMachine').rename_axis('Terminal').reset_index()
        kruonis_generators = generators[generators['IdentifiedObject.name'].str.contains('KHAE_G')]

        if not kruonis_generators.empty:
            gen_count = kruonis_generators[kruonis_generators['RotatingMachine.p'] > 0].shape[0]
            flag = gen_count < 3
            report.update({"kruonis_generators": gen_count, "kruonis_check": flag})
        else:
            report.update({"kruonis_generators": None, "kruonis_check": False})


         # Check LT-PL crossborder flow
        try:
            tie_flows = get_tieflow_data(network)
            tie_flows = tie_flows[tie_flows['cross_border'] == 'LT-PL']
            tie_flows = tie_flows[tie_flows['IdentifiedObject.name_TieFlow'] == 'LIETUVA']
            tie_flow_1 = tie_flows[tie_flows['IdentifiedObject.shortName_EquivalentInjection'] == 'XEL_AL11']
            tie_flow_2 = tie_flows[tie_flows['IdentifiedObject.shortName_EquivalentInjection'] == 'XEL_AL12']
            tie_flow = float((tie_flow_1['SvPowerFlow.p'].iloc[0] + tie_flow_2['SvPowerFlow.p'].iloc[0]) / 2)
            report.update({"lt_pl_flow": tie_flow, "lt_pl_xborder_check": abs(tie_flow)< float(BORDER_LIMIT)})
        except:
            report.update({"lt_pl_flow": None, "lt_pl_xborder_check": False})

        # Check cross-border line inconsistencies
        # pairing_keys = d_lines.groupby('pairing_key')['connected'].nunique()
        # mismatch = len(pairing_keys[pairing_keys > 1].index.tolist())
        # flag = mismatch < 1
        # report.update({"xb_mismatch": mismatch, "xb_consitency_check": flag})

        # TODO Check model outage mismatch with outage plan
        # model_outages = pd.DataFrame(get_model_outages(network=network))

        report['object_type'] = object_type

    elif object_type == "IGM":
        # TODO define IGM quality rules
        report.update({"quality": "No Status"})
    else:
        logger.error("Incorrect object type metadata")

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
def query_elk_uap(index, time_horizon=None):

    from datetime import datetime

    logger.info(f"Retrieving outages from ELK index: '{index}'")

    # now represents the time of the run, in P0W case it should be current time
    now = datetime.now()
    now = now.strftime("%Y-%m-%dT%H:%M") + "Z"

    if time_horizon == 'WK':
        merge_type_list = ['week']
    elif time_horizon == 'MO':
        merge_type_list = ['week', 'month']
    elif time_horizon == 'YR':
        merge_type_list = ['year']

    query = {
        "bool": {
            "must": [
                {"exists": {"field": "name"}},
                {"terms": {"Merge": merge_type_list}},
            ],
            "filter": [{"range": {"reportParsedDate": {"lte": now, "gte": "now-2w"}}}],
        }
    }
    response = get_docs_by_query(index=index, query=query, size=10000, return_df=True)
    result = pd.DataFrame()

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
                result = pd.concat([result, pd.DataFrame([row])], ignore_index=True)
                last_end_time[eic] = end_time

    return result


def process_zipped_cgm(zipped_bytes, processed=[]):

    with ZipFile(BytesIO(zipped_bytes)) as zf:
        for name in zf.namelist():
            with zf.open(name) as file:
                content = file.read()
                if name.endswith('.zip'):
                    process_zipped_cgm(content)
                elif name.endswith('.xml'):
                    file_object = BytesIO(content)
                    file_object.name = name
                    processed.append(file_object)

    return processed
