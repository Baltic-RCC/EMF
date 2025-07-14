from io import BytesIO
from zipfile import ZipFile
import logging

logger = logging.getLogger(__name__)

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
            control_areas = (network.type_tableview('ControlArea')
                             .rename_axis('ControlArea')
                             .reset_index())[['ControlArea', 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                              'IdentifiedObject.energyIdentCodeEic', 'IdentifiedObject.name']]
        except KeyError:
            control_areas = network.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
            ssh_areas = network.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
            control_areas = control_areas.merge(ssh_areas, on='ControlArea')[
                ['ControlArea', 'ControlArea.netInterchange',
                 'ControlArea.pTolerance',
                 'IdentifiedObject.energyIdentCodeEic',
                 'IdentifiedObject.name']]
        tie_flows = (network.type_tableview('TieFlow')
                     .rename_axis('TieFlow').rename(columns={'TieFlow.ControlArea': 'ControlArea',
                                                             'TieFlow.Terminal': 'Terminal'})
                     .reset_index())[['ControlArea', 'Terminal', 'TieFlow.positiveFlowIn']]
        tie_flows = tie_flows.merge(control_areas[['ControlArea']], on='ControlArea')
        try:
            terminals = (network.type_tableview('Terminal')
                         .rename_axis('Terminal').reset_index())[['Terminal', 'ACDCTerminal.connected']]
        except KeyError:
            terminals = (network.type_tableview('Terminal')
                         .rename_axis('Terminal').reset_index())[['Terminal']]
        tie_flows = tie_flows.merge(terminals, on='Terminal')
        try:
            power_flows_pre = (network.type_tableview('SvPowerFlow')
                               .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                               .reset_index())[['Terminal', 'SvPowerFlow.p']]
            tie_flows = tie_flows.merge(power_flows_pre, on='Terminal', how='left')
        except Exception:
            logger.error(f"Was not able to get tie flows from original models")
        power_flows_post = (network.type_tableview('SvPowerFlow')
                            .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                            .reset_index())[['Terminal', 'SvPowerFlow.p']]

        tie_flows = tie_flows.merge(power_flows_post, on='Terminal', how='left',
                                    suffixes=('_pre', '_post'))

        # TODO double check correct limit value
        # BORDER_LIMIT = 250
        # d_lines = network.get_dangling_lines(all_attributes=True)
        # LT_PL_lines = d_lines[d_lines['name'].str.contains('Alytus-Elk')]
        # if LT_PL_lines:
        #     flow_sum = LT_PL_lines['p'].sum()
        #     flag = flow_sum < BORDER_LIMIT
        #     report.update({"lt_pl_flow": flow_sum, "lt_pl_xborder_check": flag})
        # else:

        # TODO fix border flow
        report.update({"lt_pl_flow": None, "lt_pl_xborder_check": False})

        # TODO remake into Triplets
        # Check cross-border line inconsistencies
        # TODO log all line info
        # pairing_keys = d_lines.groupby('pairing_key')['connected'].nunique()
        # mismatch = len(pairing_keys[pairing_keys > 1].index.tolist())
        # flag = mismatch < 1
        # report.update({"xb_mismatch": mismatch, "xb_consitency_check": flag})

        # TODO Check model outage mismatch with outage plan
        # model_outages = pd.DataFrame(get_model_outages(network=network))

    # TODO define IGM quality rules
    elif object_type == "IGM":
        report = model_metadata[0]
        model_metadata[0].pop('opde:Component')
        try:
            model_metadata[0].pop('opde:Dependencies')
        except:
            model_metadata[0].pop('opde:DependsOn')

        report.update({"quality": "No Status"})

    return report


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
