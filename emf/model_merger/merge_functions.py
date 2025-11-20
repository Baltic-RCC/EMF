import json
import math
import numpy as np
from xml.sax.expatreader import version
import pandas as pd
import pypowsybl
import logging
import sys
import datetime
import triplets
import uuid
import config
import xml.etree.ElementTree as ET
from emf.common.config_parser import parse_app_properties
from emf.common.integrations import elastic
from emf.model_merger import temporary
from emf.common.helpers.time import parse_datetime
from emf.common.helpers.loadflow import get_model_outages, get_network_elements
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets, filename_from_opdm_metadata


logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.post_processing)


def is_valid_uuid(uuid_value):
    """
    Checks if input is uuid value
    For merged SV profile the output uuid can be combination of several existing uuids
    :param uuid_value: input value
    :return
    """
    try:
        uuid.UUID(str(uuid_value))
        return True
    except ValueError:
        return False


def export_merged_model(network: pypowsybl.network,
                        opdm_object_meta: dict,
                        profiles: list[str] | None = None,
                        cgm_convention: bool = True,
                        ):

    # Define which profiles to export
    if profiles:
        profiles = ",".join(profiles)
    else:
        profiles = "SV,SSH,TP,EQ"

    # Define whether export using CGM official structure
    cgm_export_flag = "False"
    if cgm_convention:
        cgm_export_flag = "True"

    # Define base name for exported files
    file_base_name = filename_from_opdm_metadata(metadata=opdm_object_meta)

    # Define CGMES export parameters
    parameters = {
        "iidm.export.cgmes.modeling-authority-set": opdm_object_meta['pmd:modelingAuthoritySet'],
        "iidm.export.cgmes.base-name": file_base_name,
        "iidm.export.cgmes.profiles": profiles,
        # For missing instances like "SupplyStation"
        "iidm.export.cgmes.topology-kind": 'NODE_BREAKER',
        # cgmes-fix-all-invalid-ids fixes non-standard uuid's. Can cause danglingReference errors
        # "iidm.export.cgmes.naming-strategy": "cgmes-fix-all-invalid-ids",  # identity, cgmes, cgmes-fix-all-invalid-ids
        "iidm.export.cgmes.export-sv-injections-for-slacks": "False",
        # False sets all boundary flows to zero causing Kirchhoff 1st law and SvPowerFlowBranchInstances2 errors
        # "iidm.export.cgmes.export-boundary-power-flows": "False",
        "iidm.export.cgmes.cgm_export": cgm_export_flag,
    }

    # Export to bytes object
    bytes_object = network.save_to_binary_buffer(format="CGMES", parameters=parameters)
    bytes_object.name = f"{file_base_name}_{uuid.uuid4()}.zip"
    logger.info(f"Exported merged model to {bytes_object.name}")

    # TODO set correct naming of exported files
    # a = triplets.rdf_parser.find_all_xml([bytes_object])

    return bytes_object


def create_merged_model_opdm_object(object_id: str,
                                    time_horizon: str,
                                    merging_entity: str,
                                    merging_area: str,
                                    scenario_date: datetime.datetime,
                                    mas: str,
                                    version: str = "001",
                                    profile: str = "SV",
                                    content_type: str = "CGMES",
                                    ):
    opdm_object_meta = {
        'opde:Object-Type': 'CGM',
        'pmd:fullModel_ID': object_id,
        'pmd:creationDate': f"{datetime.datetime.now(datetime.UTC):%Y-%m-%dT%H:%M:%S.%fZ}",
        'pmd:timeHorizon': time_horizon,
        'pmd:cgmesProfile': profile,
        'pmd:contentType': content_type,
        'pmd:modelPartReference': "-".join([merging_entity, merging_area]),
        'pmd:mergingEntity': merging_entity,
        'pmd:Area': merging_area,
        'pmd:validFrom': f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}",
        'pmd:modelingAuthoritySet': mas,
        "pmd:isFullModel": "true",
        'pmd:scenarioDate': f"{parse_datetime(scenario_date):%Y-%m-%dT%H:%M:00Z}",
        'pmd:modelid': object_id,
        'pmd:description': f"""<MDE>
                                <BP>{time_horizon}</BP>
                                <TOOL>pypowsybl_{pypowsybl.__version__}</TOOL>
                                <RSC>{merging_entity}</RSC>
                                <TXT>Model: Simplification of reality for given need.</TXT>
                            </MDE>""",
        'pmd:versionNumber': f"{int(version):03d}",
    }

    return opdm_object_meta


def update_header_from_opdm_object(data: pd.DataFrame, opdm_object: dict):
    return triplets.cgmes_tools.update_FullModel_from_dict(data, metadata={
        "Model.version": f"{int(opdm_object['pmd:versionNumber']):03d}",
        "Model.created": f"{parse_datetime(opdm_object['pmd:creationDate']):%Y-%m-%dT%H:%M:%S.%fZ}",
        "Model.mergingEntity": opdm_object['pmd:mergingEntity'],
        "Model.domain": opdm_object['pmd:Area'],
        "Model.scenarioTime": f"{parse_datetime(opdm_object['pmd:scenarioDate']):%Y-%m-%dT%H:%M:00Z}",
        "Model.description": opdm_object['pmd:description'],
        "Model.processType": opdm_object['pmd:timeHorizon'],
    })


def update_merged_model_sv(sv_data: bytes, opdm_object_meta: dict):

    # Load SV profile data
    sv_data = pd.read_RDF([sv_data])

    # Update rdfxml header from opdm object metadata
    sv_data = update_header_from_opdm_object(data=sv_data, opdm_object=opdm_object_meta)

    # Update file name at 'label' key
    sv_data.set_VALUE_at_KEY(key='label', value=filename_from_opdm_metadata(opdm_object_meta, file_type="xml"))

    sv_data = triplets.cgmes_tools.update_FullModel_from_filename(sv_data)

    # Check and fix SV id if necessary
    updated_sv_id_map = {}
    for old_id in sv_data.query("KEY == 'Type' and VALUE == 'FullModel'").ID.unique():
        if not is_valid_uuid(old_id):
            new_id = str(uuid.uuid4())
            updated_sv_id_map[old_id] = new_id
            logger.warning(f"SV profile id {old_id} is not valid, assigning: {new_id}")
    sv_data = sv_data.replace(updated_sv_id_map)

    return sv_data


def load_ssh(input_data: pd.DataFrame | list):
    """
    Loads in ssh profiles from list of profiles or takes the slice from dataframe
    :param input_data: list of profiles or dataframe
    :return dataframe of ssh data
    """
    if not isinstance(input_data, pd.DataFrame):
        ssh_data = load_opdm_objects_to_triplets(input_data, "SSH")
    else:
        ssh_files = (input_data[(input_data['KEY'] == 'label') &
                                (input_data['VALUE'].str.upper().str.contains('SSH'))][['INSTANCE_ID']]
                     .drop_duplicates())
        ssh_data = input_data.merge(ssh_files, on='INSTANCE_ID')
    ssh_data = triplets.cgmes_tools.update_FullModel_from_filename(ssh_data)
    return ssh_data


def create_updated_ssh(models_as_triplets: pd.DataFrame | list,
                       sv_data: pd.DataFrame,
                       opdm_object_meta: dict,
                       input_models: list = None,
                       ):
    # TODO rewrite to use pypowsybl exported SSH

    ### SSH ##

    # Load original SSH data to created updated SSH
    ssh_file_data = input_models or models_as_triplets
    ssh_data = load_ssh(ssh_file_data)

    # Update SSH Model.scenarioTime
    ssh_data.set_VALUE_at_KEY('Model.scenarioTime', opdm_object_meta['pmd:scenarioDate'])

    # Load full original data to fix issues
    # data = load_opdm_data(original_models)
    # terminals = data.type_tableview("Terminal")

    # Update SSH data from SV
    ssh_update_map = [
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.p",
            "to_attribute": "EnergyConsumer.p",
        },
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.q",
            "to_attribute": "EnergyConsumer.q",
        },
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.p",
            "to_attribute": "RotatingMachine.p",
        },
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.q",
            "to_attribute": "RotatingMachine.q",
        },
        {
            "from_class": "SvTapStep",
            "from_ID": "SvTapStep.TapChanger",
            "from_attribute": "SvTapStep.position",
            "to_attribute": "TapChanger.step",
        },
        {
            "from_class": "SvShuntCompensatorSections",
            "from_ID": "SvShuntCompensatorSections.ShuntCompensator",
            "from_attribute": "SvShuntCompensatorSections.sections",
            "to_attribute": "ShuntCompensator.sections",
        }
    ]
    # Load terminal from original data
    terminals = models_as_triplets.type_tableview("Terminal")

    # Update
    for update in ssh_update_map:
        # logger.info(f"Updating: {update['from_attribute']} -> {update['to_attribute']}")
        source_data = sv_data.type_tableview(update['from_class']).reset_index(drop=True)

        # Merge with terminal, if needed
        if terminal_reference := [column_name if ".Terminal" in column_name else None for column_name in source_data.columns][0]:
            source_data = source_data.merge(terminals, left_on=terminal_reference, right_on='ID')
            logger.debug(f"Added Terminals to {update['from_class']}")

        ssh_data = ssh_data.update_triplet_from_triplet(source_data.rename(columns={
            update['from_ID']: 'ID',
            update['from_attribute']: update['to_attribute']}
        )[['ID', update['to_attribute']]].set_index('ID').tableview_to_triplet(), add=False)

    # Generate new UUID for updated SSH
    updated_ssh_id_map = {}
    for OLD_ID in ssh_data.query("KEY == 'Type' and VALUE == 'FullModel'").ID.unique():
        NEW_ID = str(uuid.uuid4())
        updated_ssh_id_map[OLD_ID] = NEW_ID
        logger.info(f"Assigned new UUID for updated SSH: {OLD_ID} -> {NEW_ID}")

    # Update SSH ID-s
    ssh_data = ssh_data.replace(updated_ssh_id_map)

    # Update in SV SSH references
    sv_data = sv_data.replace(updated_ssh_id_map)

    # Add SSH supersedes reference to old SSH
    ssh_supersedes_data = pd.DataFrame([{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
    ssh_supersedes_data['INSTANCE_ID'] = ssh_data.query("KEY == 'Type'").merge(ssh_supersedes_data.ID)['INSTANCE_ID']
    ssh_data = ssh_data.update_triplet_from_triplet(ssh_supersedes_data)

    # Update SSH metadata
    ssh_data = update_header_from_opdm_object(ssh_data, opdm_object_meta)

    # Update SSH filenames
    filename_mask = "{scenarioTime:%Y%m%dT%H%MZ}_{processType}_{mergingEntity}-{domain}-{forEntity}_{messageType}_{version:03d}"
    ssh_data = triplets.cgmes_tools.update_filename_from_FullModel(ssh_data, filename_mask=filename_mask)

    return sv_data, ssh_data, opdm_object_meta


def ensure_paired_equivalent_injection_compatibility(network: pypowsybl.network):
    """Where there are paired boundary points, equivalent injections need to be modified to comply
    LEVEL7 rule PairedEICompatibility

    Set P and Q to 0 - so that no additional consumption or production is on tie line
    """
    logger.info("Configuring paired boundary points equivalent injections: p0/q0 = 0.0")
    dangling_lines = network.get_dangling_lines(all_attributes=True)
    paired_dangling_lines = dangling_lines[dangling_lines['paired'] == True]
    if paired_dangling_lines.empty:
        logger.warning(f"No paired dangling lines found in network model")
        return network

    # Set p0/q0 to 0 for all paired dangling lines
    _updated_p0 = pd.Series(0, index=paired_dangling_lines.index)
    _updated_q0 = pd.Series(0, index=paired_dangling_lines.index)
    network.update_dangling_lines(id=paired_dangling_lines.index, p0=_updated_p0, q0=_updated_q0)

    return network


def ensure_paired_boundary_line_connectivity(network: pypowsybl.network):
    logger.info("Aligning paired boundary lines connection status")
    dangling_lines = network.get_dangling_lines(all_attributes=True)
    paired_dangling_lines = dangling_lines[dangling_lines['paired'] == True]
    if paired_dangling_lines.empty:
        logger.warning(f"No paired dangling lines found in network model")
        return network

    # Identify dangling line pairs where the 'connected' status is inconsistent within each pairing_key group
    mask = paired_dangling_lines.groupby('pairing_key')['connected'].transform(lambda s: s.nunique() > 1)
    mismatched_dangling_lines = paired_dangling_lines[mask]
    logger.info(f"Boundary lines with non-matching connection status: {mismatched_dangling_lines['pairing_key'].unique().tolist()}")

    # Set all mismatched lines to disconnected (False)
    _connected = pd.Series(data=False, index=mismatched_dangling_lines.index)
    network.update_dangling_lines(id=mismatched_dangling_lines.index, connected=_connected)

    # Log each change
    for i, row in mismatched_dangling_lines.iterrows():
        logger.info(f"Changed status of dangling line {row['name']}: {row['connected']} -> False")

    return network


def generate_merge_report(merged_model: object, task: dict):
    """
    Creates JSON type report of pypowsybl loadflow results

    Args:
        merged_model: merged pypowsybl network
        task: task object dict
    Returns:
        dict: report of merge results
    """
    report = merged_model.__dict__

    # Pop out pypowsybl network
    network = report.pop('network')

    # Include task data
    report.update({'@timestamp': task.get('@timestamp'),
                   '@process_id': task.get('process_id'),
                   '@run_id': task.get('run_id'),
                   '@job_id': task.get('job_id'),
                   '@task_id': task.get('@id'),
                   '@time_horizon': task['task_properties'].get('time_horizon'),
                   '@scenario_timestamp': task['task_properties'].get('timestamp_utc'),
                   '@version': int(task['task_properties'].get('version')),
                   'merge_type': task['task_properties'].get('merge_type'),
                   'merge_entity': task['task_properties'].get('merging_entity'),
                   })

    # Include buses count in each component
    buses = get_network_elements(network, pypowsybl.network.ElementType.BUS)
    buses_by_component = buses.connected_component.value_counts()
    for component in report['loadflow']:
        component['buses'] = buses_by_component.to_dict().get(component['connected_component_num'])

    # Count network components/islands
    report['component_count'] = len(report['loadflow'])

    # Set trustability tag
    report.update(evaluate_trustability(report, task['task_properties']))

    return report


def evaluate_trustability(report, properties) -> dict:

    reason = None
    if properties["merge_type"] == "BA":
        # Evaluate model trustability based on defined config and report keys
        report_keys = ['scaled', 'replaced', 'outages']
        property_keys = ['scaling', 'replacement']

        # Inline logic functions
        key_true = lambda key: lambda d: bool(d.get(key))
        all_ = lambda *rules: lambda d: all(rule(d) for rule in rules)
        all_none = lambda *keys, exclude=None: lambda d: all(d.get(k) is None for k in keys if k != exclude)

        # Compose conditions
        config_all_true = all_(*(key_true(k) for k in property_keys))
        success_all_true = all_(*(key_true(k) for k in report_keys))
        success_all_none = all_none(*report_keys, exclude='scaled') # Scaling is never in None state

        # Evaluate logic
        config_enabled = config_all_true(properties)
        success_all_true = success_all_true(report)
        success_all_none = success_all_none(report)
        scaled_correctly = report['scaled']

        reason_map = {
            "scaled": "scaling failed",
            "replaced": "replacement failed",
            "outages": "outage fixing failed",
        }

        # Decide trust level
        if config_enabled and success_all_none and scaled_correctly:
            trustability = "trusted"
        elif config_enabled and success_all_true:
            trustability = "semi-trusted"
        else:
            trustability = "untrusted"
            if not config_enabled:
                reason = "config is disabled"
            else:
                # From reason map get correct reason
                for key, value in report.items():
                    if key in reason_map and not report[key]:
                        reason = reason_map[key]
    else:
        trustability = 'not_evaluated'

    return {"trustability": trustability, "untrustability_reason": reason}


def filter_models(models: list, included_models: list | str = None, excluded_models: list | str = None, filter_on: str = 'pmd:TSO'):
    """
    Filters the list of models to include or to exclude specific tsos if they are given.
    If included is defined, excluded is not used
    :param models: list of igm models
    :param included_models: list or string of tso names, if given, only matching models are returned
    :param excluded_models: list or string of tso names, if given, matching models will be discarded
    :return updated list of igms
    """

    included_models = [included_models] if isinstance(included_models, str) else included_models
    excluded_models = [excluded_models] if isinstance(excluded_models, str) else excluded_models

    if included_models:
        logger.info(f"Models to be included: {included_models}")
    elif excluded_models:
        logger.info(f"Models to be excluded: {excluded_models}")
    else:
        logger.info(f"Including all available models: {[model['pmd:TSO'] for model in models]}")
        return models

    filtered_models = []

    for model in models:

        if included_models:
            if model[filter_on] not in included_models:
                logger.info(f"Excluded {model[filter_on]}")
                continue

        elif excluded_models:
            if model[filter_on] in excluded_models:
                logger.info(f"Excluded {model[filter_on]}")
                continue

        logger.info(f"Included {model[filter_on]}")
        filtered_models.append(model)

    return filtered_models


def filter_models_by_acnp(models: list, merged_model,  acnp_dict, acnp_threshold, conform_load_factor):

    def is_within_acnp_deadband(model):
        tso = model.get('pmd:TSO')
        if not tso or tso not in acnp_dict:
            logger.error(f"TSO '{tso}' not found in acnp dict, skipping filtering")
            return True
        acnp = acnp_dict[tso]
        return abs(float(model['ac_net_position']) - float(acnp)) <= float(acnp_threshold)

    def is_within_conformload_deadband(model):
        tso = model.get('pmd:TSO')
        if not tso or tso not in acnp_dict:
            logger.error(f"TSO '{tso}' not found in acnp dict, skipping filtering")
            return True
        acnp = acnp_dict[tso]
        expected_load = model['sum_conform_load'] * float(conform_load_factor)
        return expected_load > abs(float(model['ac_net_position']) - float(acnp))

    logger.info("Excluding models with incorrect ACNP")
    excluded_tso_ids = set()

    # ACNP deadband filter
    filtered_models = [model for model in models if is_within_acnp_deadband(model)]
    excluded_tsos= [
        {'tso': model['pmd:TSO'], 'reason': 'acnp-outside-schedule-deadband'}
        for model in models
        if model['pmd:TSO'] not in [fm['pmd:TSO'] for fm in filtered_models]
        and model['pmd:TSO'] not in excluded_tso_ids
    ]
    if excluded_tsos:
        excluded_tso_ids.update(model['tso'] for model in excluded_tsos)
        logger.warning(f"Exluded TSO due to incorrect schedules: {excluded_tso_ids}")
        merged_model.excluded.extend(excluded_tsos)

    # Conformload filter
    final_models = [model for model in filtered_models if is_within_conformload_deadband(model)]
    excluded_tsos= [
        {'tso': model['pmd:TSO'], 'reason': 'conform-load-outside-schedule-difference'}
        for model in filtered_models
        if model['pmd:TSO'] not in [fm['pmd:TSO'] for fm in final_models]
        and model['pmd:TSO'] not in excluded_tso_ids
    ]
    if excluded_tsos:
        excluded_tso_ids.update(model['tso'] for model in excluded_tsos)
        logger.warning(f"Exluded TSO due to incorrect conform load: {excluded_tso_ids}")
        merged_model.excluded.extend(excluded_tsos)

    return final_models


def filter_replacements_by_acnp(models: pd.DataFrame, acnp_dict, acnp_threshold, conform_load_factor):

    models = models[
        (models['pmd:TSO'].apply(lambda x: x not in acnp_dict)) |
        ((models['ac_net_position'] - models['pmd:TSO'].apply(lambda x: acnp_dict.get(x, np.nan))).abs() <= float(acnp_threshold))]
    models = models[
        (models['pmd:TSO'].apply(lambda x: x not in acnp_dict)) |
        (models['sum_conform_load'] * float(conform_load_factor) > (models['ac_net_position'] - models['pmd:TSO'].apply(lambda x: acnp_dict.get(x, np.nan))).abs())]

    return models


def update_model_outages(merged_model: object, tso_list: list, scenario_datetime: str, time_horizon: str):

    area_map = {"LITGRID": "Lithuania", "AST": "Latvia", "ELERING": "Estonia"}
    outage_areas = [area_map.get(item, item) for item in tso_list]

    elk_service = elastic.Elastic()

    # Get outage eic-mrid mapping
    mrid_map = elk_service.get_docs_by_query(index='config-network*', query={"match_all": {}}, size=10000)
    mrid_map['mrid'] = mrid_map['mrid'].str.lstrip('_')

    # Get latest UAP parse date
    if time_horizon == 'MO':
        merge_type = "Month"
    elif time_horizon == 'YR':
        merge_type = "Year"
    else:
        merge_type = "Week"

    body = {"size": 1, "query": {"bool": {"must": [{"match": {"Merge": merge_type}}]}},
            "sort": [{"reportParsedDate": {"order": "desc"}}], "fields": ["reportParsedDate"]}
    last_uap_version = elk_service.client.search(index='opc-outages-baltics*', body=body)['hits']['hits'][0]['fields']['reportParsedDate'][0]

    # Query for latest outage UAP
    uap_query = {"bool": {"must": [{"match": {"reportParsedDate": f"{last_uap_version}"}},
                                   {"match": {"Merge": merge_type}}]}}
    uap_outages = elk_service.get_docs_by_query(index='opc-outages-baltics*', query=uap_query, size=10000)
    uap_outages = uap_outages.merge(mrid_map[['eic', 'mrid']], how='left', on='eic', indicator=True).rename(columns={"mrid": 'grid_id'})
    unmapped_outages = uap_outages[uap_outages['_merge'] == 'left_only']

    if not unmapped_outages.empty:
        logger.warning(f"Unable to map following outage mRIDs: {unmapped_outages['name'].values}")

    # Filter outages according to model scenario date and replaced area
    filtered_outages = uap_outages[(uap_outages['start_date'] <= scenario_datetime) & (uap_outages['end_date'] >= scenario_datetime)]
    filtered_outages = filtered_outages[filtered_outages['Area'].isin(outage_areas)]
    mapped_outages = filtered_outages[~filtered_outages['grid_id'].isna()]

    # Get disconnected elements in network model
    model_outages = pd.DataFrame(get_model_outages(network=merged_model.network))
    mapped_model_outages = pd.merge(model_outages, mrid_map, left_on='grid_id', right_on='mrid', how='inner')
    model_area_map = {"LITGRID": "LT", "AST": "LV", "ELERING": "EE"}
    model_outage_areas = [model_area_map.get(item, item) for item in tso_list]
    filtered_model_outages = mapped_model_outages[mapped_model_outages['country'].isin(model_outage_areas)]

    # Include cross-border lines for reconnection (both dangling lines)
    dangling_lines = get_network_elements(network=merged_model.network,
                                          element_type=pypowsybl.network.ElementType.DANGLING_LINE).reset_index(names=['grid_id'])
    border_lines = dangling_lines[dangling_lines['pairing_key'].isin(model_outages['pairing_key'])]
    relevant_border_lines = border_lines[border_lines['country'].isin(['LT', 'LV', 'EE'])]
    additional_dangling_lines = dangling_lines[dangling_lines['pairing_key'].isin(relevant_border_lines['pairing_key'])]

    # Merged dataframe of network elements to be reconnected
    filtered_model_outages = pd.concat([filtered_model_outages, additional_dangling_lines]).drop_duplicates(subset='grid_id')
    filtered_model_outages = filtered_model_outages.where(pd.notnull(filtered_model_outages), None)

    # rename columns
    filtered_model_outages = filtered_model_outages.copy()[['name', 'grid_id', 'eic']].rename(columns={'grid_id': 'mrid'})
    mapped_outages = mapped_outages.copy()[['name', 'grid_id', 'eic']].rename(columns={'grid_id': 'mrid'})

    logger.info("Updating outages on merged model")

    # Reconnecting outages from network-config list
    outages_updated = {}
    for index, outage in filtered_model_outages.iterrows():
        try:
            if merged_model.network.connect(outage['mrid']):
                logger.info(f"Successfully reconnected: {outage['name']} [mrid: {outage['mrid']}]")
                merged_model.outages = True
                outage_dict = outage.to_dict()
                outage_dict.update({'status': 'connected'})
                outages_updated[outage_dict['mrid']] = outage_dict
            else:
                if uap_outages['grid_id'].str.contains(outage['mrid']).any():
                    logger.info(f"Element is already connected: {outage['name']} [mrid: {outage['mrid']}]")
                else:
                    logger.error(f"Failed to connect element: {outage['name']} [mrid: {outage['mrid']}]")
                    merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
        except Exception as e:
            logger.error((e, outage['name']))
            merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
            merged_model.outages = False
            continue

    # Applying outages from UAP
    for index, outage in mapped_outages.iterrows():
        try:
            if merged_model.network.disconnect(outage['mrid']):
                logger.info(f"Successfully disconnected: {outage['name']} [mrid: {outage['mrid']}]")
                merged_model.outages = True
                outage_dict = outage.to_dict()
                outage_dict.update({'status': 'disconnected'})
                outages_updated[outage_dict['mrid']] = outage_dict
            else:
                if uap_outages['grid_id'].str.contains(outage['mrid']).any():
                    logger.info(f"Element is already in outage: {outage['name']} [mrid: {outage['mrid']}]")
                else:
                    logger.error(f"Failed to disconnect element: {outage['name']} [mrid: {outage['mrid']}]")
                    merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
        except Exception as e:
            logger.error((e, outage['name']))
            merged_model.outages_unmapped.extend([{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
            merged_model.outages = False
            continue

    # Keep only important keys of updated outages
    merged_model.outages_updated = list(outages_updated.values())

    if merged_model.outages_unmapped:
        merged_model.outages = False

    return merged_model


def set_intraday_time_horizon(scenario_datetime, task_creation_time):
    """
    Finds time difference between task creation time and scenario timestamp. Converts it to hours and finds the hour
    number corresponding to intraday run (number of hours that scenario timestamp is ahead from task creation time)
    Here are multiple ways to calculate (must keep in mind start and end times/dates)
    1) Ceil: f"{math.ceil((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
    2) Round: f"{int((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
    3) Floor: f"{math.floor((_scenario_datetime - _task_creation_time).seconds / 3600):02d}"
    Take into account date change
    4) Min(max(Ceil)): max(math.ceil((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600), 1)
    5) Min(max(Round)): max(int((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600), 1)
    6) Min(max(Floor)): max(math.floor((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600), 1)
    :param scenario_datetime: scenario timestamp for intraday run
    :param task_creation_time: timestamp when the task was created
    :return: time horizon for intraday run as a string
    """
    max_time_horizon_value = 36
    calculated_time_horizon = '01'  # DEFAULT VALUE, CHANGE THIS
    _task_creation_time = parse_datetime(task_creation_time, keep_timezone=False)
    _scenario_datetime = parse_datetime(scenario_datetime, keep_timezone=False)
    time_diff = _scenario_datetime - _task_creation_time
    if 0 <= time_diff.days <= 1:
        time_horizon_actual = math.floor((time_diff.days * 24 * 3600 + time_diff.seconds) / 3600)
        # just in case cut it to bigger than 1 once again
        time_horizon_actual = max(time_horizon_actual, 1)
        if time_horizon_actual <= max_time_horizon_value:
            calculated_time_horizon = f"{time_horizon_actual:02d}"
    return calculated_time_horizon


def check_net_interchanges(cgm_sv_data, cgm_ssh_data, original_models, fix_errors: bool = False, threshold: float = None):
    """
    An attempt to calculate the net interchange 2 values and check them against those provided in ssh profiles
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param original_models: igms in triplets
    :param fix_errors: injects new calculated flows into merged ssh profiles
    :param threshold: specify threshold if needed
    :return (updated) ssh profiles
    """
    try:
        control_areas = (original_models.type_tableview('ControlArea')
                         .rename_axis('ControlArea')
                         .reset_index())[['ControlArea', 'ControlArea.netInterchange', 'ControlArea.pTolerance',
                                          'IdentifiedObject.energyIdentCodeEic', 'IdentifiedObject.name']]
    except KeyError:
        control_areas = original_models.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        ssh_areas = cgm_ssh_data.type_tableview('ControlArea').rename_axis('ControlArea').reset_index()
        control_areas = control_areas.merge(ssh_areas, on='ControlArea')[['ControlArea', 'ControlArea.netInterchange',
                                                                          'ControlArea.pTolerance',
                                                                          'IdentifiedObject.energyIdentCodeEic',
                                                                          'IdentifiedObject.name']]
    tie_flows = (original_models.type_tableview('TieFlow')
                 .rename_axis('TieFlow').rename(columns={'TieFlow.ControlArea': 'ControlArea',
                                                         'TieFlow.Terminal': 'Terminal'})
                 .reset_index())[['ControlArea', 'Terminal', 'TieFlow.positiveFlowIn']]
    tie_flows = tie_flows.merge(control_areas[['ControlArea']], on='ControlArea')
    try:
        terminals = (original_models.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal', 'ACDCTerminal.connected']]
    except KeyError:
        terminals = (original_models.type_tableview('Terminal')
                     .rename_axis('Terminal').reset_index())[['Terminal']]
    tie_flows = tie_flows.merge(terminals, on='Terminal')
    try:
        power_flows_pre = (original_models.type_tableview('SvPowerFlow')
                           .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                           .reset_index())[['Terminal', 'SvPowerFlow.p']]
        tie_flows = tie_flows.merge(power_flows_pre, on='Terminal', how='left')
    except Exception as error:
        logger.warning(f"Was not able to get tie flows from original models with exception: {error}")
    power_flows_post = (cgm_sv_data.type_tableview('SvPowerFlow')
                        .rename(columns={'SvPowerFlow.Terminal': 'Terminal'})
                        .reset_index())[['Terminal', 'SvPowerFlow.p']]

    tie_flows = tie_flows.merge(power_flows_post, on='Terminal', how='left',
                                suffixes=('_pre', '_post'))
    try:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p_pre', 'SvPowerFlow.p_post']]
                              .agg(lambda x: pd.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
    except KeyError:
        tie_flows_grouped = ((tie_flows.groupby('ControlArea')[['SvPowerFlow.p']]
                              .agg(lambda x: pd.to_numeric(x, errors='coerce').sum()))
                             .rename_axis('ControlArea').reset_index())
        tie_flows_grouped = tie_flows_grouped.rename(columns={'SvPowerFlow.p': 'SvPowerFlow.p_post'})
    tie_flows_grouped = control_areas.merge(tie_flows_grouped, on='ControlArea')
    if threshold and threshold > 0:
        tie_flows_grouped['Exceeded'] = (abs(tie_flows_grouped['ControlArea.netInterchange']
                                             - tie_flows_grouped['SvPowerFlow.p_post']) > threshold)
    else:
        tie_flows_grouped['Exceeded'] = (abs(tie_flows_grouped['ControlArea.netInterchange']
                                             - tie_flows_grouped['SvPowerFlow.p_post']) >
                                         tie_flows_grouped['ControlArea.pTolerance'])
    net_interchange_errors = tie_flows_grouped[tie_flows_grouped.eval('Exceeded')]
    if not net_interchange_errors.empty:
        logger.warning(f"Found {len(net_interchange_errors.index)} possible net interchange_2 problems over {threshold}")
        # Apply modification
        if fix_errors:
            logger.warning(f"Updating {len(net_interchange_errors.index)} interchanges to new values")
            new_areas = cgm_ssh_data.type_tableview('ControlArea').reset_index()[['ID',
                                                                                  'ControlArea.pTolerance', 'Type']]
            new_areas = new_areas.merge(net_interchange_errors[['ControlArea', 'SvPowerFlow.p_post']]
                                        .rename(columns={'ControlArea': 'ID',
                                                         'SvPowerFlow.p_post': 'ControlArea.netInterchange'}), on='ID')
            cgm_ssh_data = triplets.rdf_parser.update_triplet_from_tableview(cgm_ssh_data, new_areas)

    return cgm_ssh_data


def check_non_boundary_equivalent_injections(cgm_sv_data,
                                             cgm_ssh_data,
                                             original_models,
                                             threshold: float = 0,
                                             fix_errors: bool = False):
    """
    Checks equivalent injections that are not on boundary topological nodes
    :param cgm_sv_data: merged SV profile
    :param cgm_ssh_data: merged SSH profile
    :param original_models: igms in triplets
    :param threshold: threshold for checking
    :param fix_errors: if true then copies values from sv profile to ssh profile
    :return cgm_ssh_data
    """
    boundary_nodes = original_models.query('KEY == "TopologicalNode.boundaryPoint" & VALUE == "true"')[['ID']]
    terminals = (original_models.type_tableview('Terminal').rename_axis('SvPowerFlow.Terminal').reset_index()
                 .merge(boundary_nodes.rename(columns={'ID': 'Terminal.TopologicalNode'}),
                        on='Terminal.TopologicalNode', how='outer', indicator=True))[['SvPowerFlow.Terminal',
                                                                                      'Terminal.ConductingEquipment',
                                                                                      '_merge']]
    terminals = terminals[terminals['_merge'] == 'left_only'][['SvPowerFlow.Terminal', 'Terminal.ConductingEquipment']]
    return check_all_kind_of_injections(cgm_sv_data=cgm_sv_data,
                                        cgm_ssh_data=cgm_ssh_data,
                                        original_models=original_models,
                                        injection_name='EquivalentInjection',
                                        fields_to_check={'SvPowerFlow.p': 'EquivalentInjection.p'},
                                        threshold=threshold,
                                        terminals=terminals,
                                        fix_errors=fix_errors)


def check_all_kind_of_injections(cgm_sv_data,
                                 cgm_ssh_data,
                                 original_models,
                                 injection_name: str = 'ExternalNetworkInjection',
                                 fields_to_check: dict = None,
                                 fix_errors: bool = False,
                                 threshold: float = 0,
                                 terminals: pd.DataFrame = None,
                                 report_sum: bool = True):
    """
    Compares the given cgm ssh injection values to the corresponding sv powerflow values in cgm sv
    :param cgm_sv_data: merged SV profile
    :param cgm_ssh_data: merged SSH profile
    :param original_models: igms in triplets
    :param injection_name: name of the injection
    :param fields_to_check: dictionary where key is the field in powerflow and value is the field in injection
    :param fix_errors: if true then copies values from sv profile to ssh profile
    :param threshold: max allowed mismatch
    :param terminals: optional, can give dataframe of terminals as input
    :param report_sum: if true prints sum of injections and powerflows to console
    :return cgm_ssh_data
    """
    if not fields_to_check:
        return cgm_ssh_data

    fixed_fields = ['ID']
    try:
        original_injections = original_models.type_tableview(injection_name).reset_index()
        injections = cgm_ssh_data.type_tableview(injection_name).reset_index()
    except AttributeError:
        logger.info(f"SSH profile doesn't contain data about {injection_name}")
        return cgm_ssh_data
    try:
        injections_reduced = injections[[*fixed_fields, *fields_to_check.values()]]
        original_injections_reduced = original_injections[[*fixed_fields, *fields_to_check.values()]]
    except KeyError as ke:
        logger.info(f"{injection_name} tableview got error: {ke}")
        return cgm_ssh_data
    injections_reduced = injections_reduced.merge(original_injections_reduced, on='ID', suffixes=('', '_org'))
    if terminals is None:
        terminals = (original_models.type_tableview('Terminal')
                     .rename_axis('SvPowerFlow.Terminal')
                     .reset_index())[['SvPowerFlow.Terminal', 'Terminal.ConductingEquipment']]
    flows = (cgm_sv_data.type_tableview('SvPowerFlow')
             .reset_index())[[*['SvPowerFlow.Terminal'], *fields_to_check.keys()]]
    terminals = terminals.merge(flows, on='SvPowerFlow.Terminal')
    terminals = terminals.merge(injections_reduced, left_on='Terminal.ConductingEquipment', right_on='ID')

    filtered_list = []
    for flow_field, injection_field in fields_to_check.items():
        filtered_list.append(terminals[abs(terminals[injection_field] - terminals[flow_field]) > threshold])
        if report_sum:
            logger.info(f"IGM {injection_field} = {terminals[injection_field + '_org'].sum()} vs "
                        f"CGM {injection_field} = {terminals[injection_field].sum()} vs "
                        f"CGM {flow_field} = {terminals[flow_field].sum()}")
    if not filtered_list:
        return cgm_ssh_data

    filtered = pd.concat(filtered_list).drop_duplicates().reset_index(drop=True)
    if not filtered.empty:
        logger.warning(f"Found {len(filtered.index)} mismatches between {injection_name} and flow values on terminals")
        # Apply modification
        if fix_errors:
            logger.info(f"Updating {injection_name} values from terminal flow values")
            injections_update = injections.merge(filtered[[*fixed_fields, *fields_to_check.keys()]])
            injections_update = injections_update.drop(columns=fields_to_check.values())
            injections_update = injections_update.rename(columns=fields_to_check)
            cgm_ssh_data = triplets.rdf_parser.update_triplet_from_tableview(data=cgm_ssh_data,
                                                                             tableview=injections_update,
                                                                             update=True,
                                                                             add=False)
    return cgm_ssh_data


def run_post_merge_processing(input_models: list,
                              exported_model: bytes,
                              opdm_object_meta: dict,
                              enable_temp_fixes: bool,
                              task_properties: dict = None,
                              ):

    # Load original input models to triplets
    input_models_triplets = load_opdm_objects_to_triplets(opdm_objects=input_models)

    # Apply corrections to SV profile
    sv_data = update_merged_model_sv(sv_data=exported_model, opdm_object_meta=opdm_object_meta)

    # Create update SSH
    sv_data, ssh_data, opdm_object_meta = create_updated_ssh(models_as_triplets=input_models_triplets,
                                                             input_models = input_models,
                                                             sv_data=sv_data,
                                                             opdm_object_meta=opdm_object_meta)
    fix_net_interchange_errors = False
    if task_properties is not None:
        fix_net_interchange_errors = task_properties.get('fix_net_interchange2', fix_net_interchange_errors)

    # Run temporary modifications on exported model
    # Temporary fixes are applied to SV and SSH profiles
    if enable_temp_fixes:
        # TODO need to revise constantly
        sv_data = temporary.remove_equivalent_shunt_section(sv_data, input_models_triplets)
        sv_data = temporary.add_missing_sv_tap_steps(sv_data, ssh_data)
        sv_data = temporary.remove_small_islands(sv_data, int(SMALL_ISLAND_SIZE))
        sv_data = temporary.remove_duplicate_sv_voltages(cgm_sv_data=sv_data, original_data=input_models_triplets)
        sv_data = temporary.check_and_fix_dependencies(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data, original_data=input_models_triplets)
        # TODO following SSH profile fix should be removed once pypowsybl SSH export will be used
        ssh_data = temporary.set_paired_boundary_injections_to_zero(original_models=input_models_triplets,
                                                                    cgm_ssh_data=ssh_data)

    # Run injections check and apply modification if defined in configuration
    injection_threshold = float(INJECTION_THRESHOLD)
    net_interchange_threshold = float(NET_INTERCHANGE_THRESHOLD)
    fix_injection_errors = json.loads(str(FIX_INJECTION_ERRORS).lower())
    fix_other_errors = json.loads(str(FIX_OTHER_ERRORS).lower())

    ssh_data = check_all_kind_of_injections(cgm_ssh_data=ssh_data,
                                            cgm_sv_data=sv_data,
                                            original_models=input_models_triplets,
                                            injection_name='EnergySource',
                                            threshold=injection_threshold,
                                            fields_to_check={'SvPowerFlow.p': 'EnergySource.activePower'},
                                            fix_errors=fix_injection_errors)
    ssh_data = check_all_kind_of_injections(cgm_ssh_data=ssh_data,
                                            cgm_sv_data=sv_data,
                                            original_models=input_models_triplets,
                                            injection_name='ExternalNetworkInjection',
                                            fields_to_check={'SvPowerFlow.p': 'ExternalNetworkInjection.p'},
                                            threshold=injection_threshold,
                                            fix_errors=fix_injection_errors)
    ssh_data = check_non_boundary_equivalent_injections(cgm_sv_data=sv_data,
                                                        cgm_ssh_data=ssh_data,
                                                        original_models=input_models_triplets,
                                                        threshold=injection_threshold,
                                                        fix_errors=fix_injection_errors)
    sv_data = temporary.check_for_disconnected_terminals(cgm_sv_data=sv_data,
                                                         original_models=input_models_triplets,
                                                         fix_errors=fix_other_errors)
    try:
        ssh_data = check_net_interchanges(cgm_sv_data=sv_data,
                                          cgm_ssh_data=ssh_data,
                                          original_models=input_models_triplets,
                                          fix_errors=fix_net_interchange_errors,
                                          threshold=net_interchange_threshold)
    except KeyError:
        logger.warning(f"No fields for net interchange correction")
    try:
        ssh_data = temporary.check_energized_boundary_nodes(cgm_sv_data=sv_data,
                                                            cgm_ssh_data=ssh_data,
                                                            original_models=input_models_triplets,
                                                            fix_errors=fix_other_errors)
    except AttributeError:
        logger.warning(f"Unable to check energized boundary nodes")

    return sv_data, ssh_data, opdm_object_meta


def lvl8_report_cgm(merge_report: dict):

    # Create <QAReport> root
    qa_attribs = {
        'created': datetime.datetime.strptime(merge_report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%SZ'),
        'schemeVersion': "2.0",
        'serviceProvider': merge_report["merge_entity"],
        'xmlns': "http://entsoe.eu/checks"
    }
    qa_root = ET.Element("QAReport", attrib=qa_attribs)

    # Add RuleViolations
    violations_list = [
        {
            'ruleId': "CGMConvergence",
            'validationLevel': "8",
            'severity': "WARNING",
            'Message': "Power flow could not be calculated for CGM with default settings."
        },
        {
            'ruleId': "CGMConvergenceRelaxed",
            'validationLevel': "8",
            'severity': "ERROR",
            'Message': "Power flow could not be calculated for CGM with EU_RELAXED settings."
        },
        {
            'ruleId': "CGMConvergenceRelaxed",
            'validationLevel': "8",
            'severity': "ERROR",
            'Message': "Error on Scaling"
        }
    ]
    # TODO:pick the correct setting based on retruned LF setting and convergance from model. Set model quality indicator based on violations
    violations = list()
    if merge_report["loadflow_status"] == 'CONVERGED':
        if merge_report["loadflow_settings"] == 'EU_DEFAULT':
            logger.info(f"Merge successful with default settings included in lvl8 report")
            quality_indicator_cgm = "Valid"
        else:
            violations.append(violations_list[0])
            quality_indicator_cgm = "Warning - non fatal inconsistencies"
    else:
        violations.append(violations_list[1])
        quality_indicator_cgm = "Invalid - inconsistent data"
        
    #if scaling is failed then set error from error list
    if not merge_report['scaled']:
        violations.append(violations_list[2])
        quality_indicator_cgm="Invalid - inconsistent data"

    # Create <CGM>
    cgm_attribs = {
        'created': datetime.datetime.strptime(merge_report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%SZ'),
        'resource': merge_report['network_meta']['fullModel_ID'],  # TODO get here correct content ID
        'scenarioTime': datetime.datetime.fromisoformat(merge_report["@scenario_timestamp"]).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'version': str(merge_report["@version"]),
        'processType': merge_report["time_horizon_id"] if merge_report["@time_horizon"] == 'ID' else merge_report["@time_horizon"],
        'qualityIndicator': quality_indicator_cgm
    }
    cgm = ET.SubElement(qa_root, "CGM", attrib=cgm_attribs)

    try:
        for v in violations:
            rv = ET.SubElement(cgm, "RuleViolation", {
                'ruleId': v['ruleId'],
                'validationLevel': v['validationLevel'],
                'severity': v['severity']
            })
            msg = ET.SubElement(rv, "Message")
            msg.text = v['Message']
    except:
        logger.info(f"No violations present in merge")

    # TODO:pick the TSOs from QA report. Missing parameters below for all IGMs
    for i in merge_report['merge_included_entity'] + merge_report['replaced_entity']:
        igm = ET.SubElement(cgm, "IGM", {
            'created': i["creation_timestamp"],
            'scenarioTime': datetime.datetime.fromisoformat(i['scenario_timestamp']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'tso': i['tso'],
            'version': str(i['version']),
            'processType': i['time_horizon'],
            'qualityIndicator': i['quality_indicator'],
        })
        resource_igm = ET.SubElement(igm, "resource")
        resource_igm.text = i['model_sv_id']

    # Add EMFInformation
    ET.SubElement(cgm, "EMFInformation", {
        'mergingEntity': merge_report["merge_entity"],
        'cgmType': merge_report["merge_type"]
    })

    # Generate final XML
    qa_report_lvl8 = ET.tostring(qa_root, encoding='utf-8', xml_declaration=True)

    return qa_report_lvl8


if __name__ == "__main__":

    from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
    from emf.common.helpers.loadflow import load_network_model
    from emf.common.loadflow_tool import loadflow_settings

    logging.basicConfig(
        format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    time_horizon = '1D'
    scenario_date = "2024-05-22T11:30"
    merging_area = "EU"
    merging_entity = "BALTICRSC"
    mas = 'http://www.baltic-rsc.eu/OperationalPlanning'
    version = "104"

    valid_models = get_latest_models_and_download(time_horizon, scenario_date, valid=True)
    latest_boundary = get_latest_boundary()

    merged_model = load_network_model(valid_models + [latest_boundary])
    solved_model = pypowsybl.loadflow.run_ac(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT)

    # Export to OPDM
    from emf.common.integrations.opdm import OPDM

    opdm_client = OPDM()
    publication_responses = []
    for instance_file in serialized_data:
        logger.info(f"Publishing {instance_file.name} to OPDM")
        publication_response = opdm_client.publication_request(instance_file, "CGMES")

        publication_responses.append(
            {"name": instance_file.name,
             "response": publication_response}
        )
