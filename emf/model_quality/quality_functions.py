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