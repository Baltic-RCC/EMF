import logging
import pandas as pd
import config
import json
import pypowsybl as pp
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic, minio_api
from emf.common.integrations.object_storage import models
from io import BytesIO
from zipfile import ZipFile, ZIP_DEFLATED
from triplets.rdf_parser import load_all_to_dataframe
from emf.model_quality.model_statistics import get_system_metrics

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.model_quality.model_quality)


class HandlerModelQuality:

    def __init__(self):
        self.minio_service = minio_api.ObjectStorage()
        self.elastic_service = elastic.Elastic()

    def handle(self, message: bytes, properties: dict, **kwargs):

        logger.info(f"Loaded {message}")

        # Load OPDM metadata objects from binary to json
        model_meta = json.loads(message)
        object_type = properties.headers['opde:Object-Type']

        if object_type == 'CGM':
            model_data = self.minio_service.download_object(model_meta.get('minio-bucket'),
                                                              model_meta.get('content_reference'))
            logger.info(f"Loading merged model: {model_meta['name']}")
            unzipped = process_zipped_cgm(model_data)
            network = load_all_to_dataframe(unzipped)

        # TODO test if IGM retrieving works as intended
        elif object_type == 'IGM':
            model_meta = models.get_content(metadata=model_meta['opdm_object'])
            logger.info(f"Loading  model...")
            try:
                data = BytesIO(model_meta["opdm:Profile"]["DATA"])
                network = load_all_to_dataframe(data)
            except:
                logger.error("Failed to load IGM")
                network = pd.DataFrame
        else:
            logger.error("Incorrect or missing metadata")
            network = pd.DataFrame

        if not network.empty:
            qa_report = generate_quality_report(network, object_type, model_meta)
            try:
                # TODO move statistics function to quality functions file or move statistics file to quality directory
                model_statistics = get_system_metrics(network)
            except:
                logger.error("Failed to get model statistics")
        else:
            raise TypeError("Model was not loaded correctly, either missing in MinIO or incorrect data")

        if model_statistics:
            model_statistics.update({k: v for k, v in model_meta.items() if k.startswith('@')})
            model_statistics.update(properties.headers)
            try:
                response = self.elastic_service.send_to_elastic(index=ELK_STATISTICS_INDEX, json_message=model_statistics)
            except Exception as error:
                logger.error(f"Statistics report sending to Elastic failed: {error}")

            logger.info(f"Statistics report sent to elastic index {ELK_STATISTICS_INDEX}")
        else:
            raise TypeError("Statistics report generator failed, data not sent")

        # Send validation report to Elastic
        if qa_report:
            try:
                response = self.elastic_service.send_to_elastic(index=ELK_QUALITY_INDEX, json_message=qa_report)
            except Exception as error:
                logger.error(f"Validation report sending to Elastic failed: {error}")

            logger.info(f"Queality report sent to elastic index {ELK_QUALITY_INDEX}")
        else:
            raise TypeError("Quality report generator failed, data not sent")

        return message, properties


def generate_quality_report(network, object_type, model_meta):

    report = {}

    if object_type == "CGM" and model_meta['merge_type'] == 'BA':

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
        report = report

    return report


def load_cgm(network, parameters):

    if bytes == type(network):
        try:
            network = unzip_zipped_profiles(BytesIO(network))
        except Exception as error:
            logger.error(error)
        network = pp.network.load_from_binary_buffer(network, parameters=parameters)
    if str == type(network):
        try:
            network = unzip_zipped_profiles(BytesIO(network))
            network = pp.network.load_from_binary_buffer(network, parameters=parameters)
        except Exception as error:
            logger.error(error)
            network = pp.network.load(network, parameters=parameters)

    return network


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


def unzip_zipped_profiles(path_or_buffer: str):
    """
    Extracts zipped profiles of a model.
    Detects automatically if the zipped folder is read from file bath or BytesIO buffer.
    :param path_or_buffer: path of zipped model or BytesIO buffer
    :return: output_zip_buffer
    """
    # Checks if path_or_buffer is string
    if isinstance(path_or_buffer, str):
        with open(path_or_buffer, 'rb') as original_zip_file:
            original_zip_buffer = BytesIO(original_zip_file.read())

    # Checks if path_or_buffer is BytesIO class
    elif isinstance(path_or_buffer, BytesIO):
        original_zip_buffer = path_or_buffer
    else:
        logger.error(f"Provided variable is nor string nor BytesIO object")

    output_zip_buffer = BytesIO()

    # Read the original zipped folder from provided buffer
    with ZipFile(original_zip_buffer, 'r') as original_zip:
        # Create a new zip file where we will store the unzipped XMLs
        with ZipFile(output_zip_buffer, 'w', ZIP_DEFLATED) as new_zip:
            # Iterate trough each file in the original zip
            for file_name in original_zip.namelist():
                # Read the zipped xml file
                with original_zip.open(file_name) as zipped_xml_file:
                    with ZipFile(zipped_xml_file) as zipped_xml:
                        # Extract each file and add it to the new zip
                        for xml_file_name in zipped_xml.namelist():
                            xml_data = zipped_xml.read(xml_file_name)
                            # Add unzipped XML file to the new zip
                            new_zip.writestr(xml_file_name, xml_data)

    output_zip_buffer.seek(0)
    output_zip_buffer.name = 'model'

    return output_zip_buffer


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


if __name__ == "__main__":
    # TESTING
    import sys
    from emf.common.integrations.opdm import OPDM
    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    #logging.getLogger('powsybl').setLevel(1)

    opdm = OPDM()
    # latest_boundary = opdm.get_latest_boundary()
    available_models = opdm.get_latest_models_and_download(time_horizon='2D',
                                                           scenario_date="2025-06-12T09:30",
                                                           tso="AST")


    opdm_metadata = json.loads(message)
