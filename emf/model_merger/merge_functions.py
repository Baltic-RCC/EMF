import math
import pandas as pd
import pypowsybl
import logging
import sys
import datetime
import triplets
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List
from emf.common.integrations import elastic
from emf.common.helpers.time import parse_datetime
from emf.common.helpers.loadflow import get_model_outages, get_network_elements
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets, filename_from_opdm_metadata
from emf.common.helpers.utils import sanitize_nan, is_valid_uuid

logger = logging.getLogger(__name__)


@dataclass
class TaskConfig:
    # ponytail: model_merger.py unpacks this via astuple() positionally - keep field order in sync with that unpack
    task_properties: dict
    task_creation_time: str
    included_models: list
    excluded_models: list
    local_import_models: list
    replace_tso: list
    time_horizon: str
    scenario_datetime: str
    schedule_start: str
    schedule_end: str
    schedule_time_horizon: str
    merging_area: str
    merging_entity: str
    mas: str
    version: str
    model_replacement: bool
    model_scaling: bool
    outage_update: bool
    force_outage_fix: bool
    model_upload_to_opdm: bool
    model_upload_to_minio: bool
    model_merge_report_send_to_elk: bool
    additional_processing: bool
    lvl8_reporting: bool

    @staticmethod
    def from_task(task: dict) -> "TaskConfig":
        task_properties = task.get('task_properties', {})
        return TaskConfig(
            task_properties=task_properties,
            task_creation_time=task.get('task_creation_time', ""),
            included_models=task_properties.get('included', []),
            excluded_models=task_properties.get('excluded', []),
            local_import_models=task_properties.get('local_import', []),
            replace_tso=task_properties.get('replace_tso', []),
            time_horizon=task_properties["time_horizon"],
            scenario_datetime=task_properties["timestamp_utc"],
            schedule_start=task_properties.get("reference_schedule_start_utc"),
            schedule_end=task_properties.get("reference_schedule_end_utc"),
            schedule_time_horizon=task_properties.get("reference_schedule_time_horizon"),
            merging_area=task_properties["merge_type"],
            merging_entity=task_properties["merging_entity"],
            mas=task_properties["mas"],
            version=task_properties["version"],
            model_replacement=task_properties["replacement"],
            model_scaling=task_properties["scaling"],
            outage_update=task_properties["outage_update"],
            force_outage_fix=task_properties['force_outage_fix'],
            model_upload_to_opdm=task_properties["upload_to_opdm"],
            model_upload_to_minio=task_properties["upload_to_minio"],
            model_merge_report_send_to_elk=task_properties["send_merge_report"],
            additional_processing=task_properties['post_temp_fixes'],
            lvl8_reporting=task_properties['lvl8_reporting'],
        )


@dataclass
class MergedModel:
    network: pypowsybl.network = None
    network_meta: dict | None = None
    time_horizon: str = None
    time_horizon_id: str = field(default_factory=str)
    name: str = None
    loadflow_status: str | None = None
    loadflow_settings: str | None = None
    duration_s: float | None = None
    content_reference: str | None = None

    # Status flags
    scaled: bool = None
    replaced: bool = None
    outages: bool = None
    acnp_schedule_replaced: bool = None
    uploaded_to_opde: bool = False
    uploaded_to_minio: bool = False


    # Extended data
    loadflow: List = field(default_factory=list)
    included: List = field(default_factory=list)
    excluded: List = field(default_factory=list)
    scaled_entity: List = field(default_factory=list)
    scaled_hvdc: List = field(default_factory=list)
    replaced_entity: List = field(default_factory=list)
    replacement_reason: List = field(default_factory=list)
    outages_updated: List = field(default_factory=list)
    acnp_schedule_replaced_entity: List = field(default_factory=list)
    acnp_schedule_missing: List = field(default_factory=list)
    outages_unmapped: List = field(default_factory=list)
    merge_included_entity: List = field(default_factory=list)


@dataclass(init=False)
class ModelEntity:
    data_source: str = "OPDM"
    quality_indicator: str = "Valid"
    tso: str = None
    time_horizon: str = None
    scenario_timestamp: str = None
    model_sv_id: str = None
    version: int = None
    quality_indicator: str = "Valid"
    creation_timestamp: str = None
    file_name: str = None

    def __init__(self, quality_indicator: str, data_source: str | None = None, **kwargs):
        self.data_source = data_source or kwargs.get('data-source', 'unknown')
        self.quality_indicator = quality_indicator
        self.tso = kwargs.get('pmd:TSO', 'unknown')
        self.time_horizon = kwargs.get('pmd:timeHorizon', 'unknown')
        self.scenario_timestamp = kwargs.get('pmd:scenarioDate', 'unknown')
        self.model_sv_id = kwargs.get('pmd:fullModel_ID', 'unknown')
        self.version = int(kwargs.get('pmd:version', 999))
        self.creation_timestamp = kwargs.get('pmd:creationDate', 'unknown')
        self.file_name = kwargs.get('pmd:fileName', 'unknown')


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
        },
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.p",
            "to_attribute": "EquivalentInjection.p"
        },
        {
            "from_class": "SvPowerFlow",
            "from_ID": "Terminal.ConductingEquipment",
            "from_attribute": "SvPowerFlow.q",
            "to_attribute": "EquivalentInjection.q"
        }
    ]
    # Load terminal from original data
    terminals = models_as_triplets.type_tableview("Terminal")

    # Update
    for update in ssh_update_map:
        # logger.info(f"Updating: {update['from_attribute']} -> {update['to_attribute']}")
        source_data = sv_data.type_tableview(update['from_class']).reset_index(drop=True)

        # Merge with terminal, if needed
        if terminal_reference := \
        [column_name if ".Terminal" in column_name else None for column_name in source_data.columns][0]:
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
    ssh_supersedes_data = pd.DataFrame(
        [{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
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
    boundary_lines = network.get_boundary_lines(all_attributes=True)
    paired_boundary_lines = boundary_lines[boundary_lines['paired'] == True]
    if paired_boundary_lines.empty:
        logger.warning(f"No paired boundary lines found in network model")
        return network

    # Set p0/q0 to 0 for all paired boundary lines
    _updated_p0 = pd.Series(0, index=paired_boundary_lines.index)
    _updated_q0 = pd.Series(0, index=paired_boundary_lines.index)
    network.update_boundary_lines(id=paired_boundary_lines.index, p0=_updated_p0, q0=_updated_q0)

    return network


def ensure_paired_boundary_line_connectivity(network: pypowsybl.network):
    logger.info("Aligning paired boundary lines connection status")
    boundary_lines = network.get_boundary_lines(all_attributes=True)
    # Add cim:Tieflow attribute to boundary lines
    boundary_lines['isTieflow'] = boundary_lines.index.isin(network.get_areas_boundaries()["element"])
    paired_boundary_lines = boundary_lines[boundary_lines['paired'] == True]
    if paired_boundary_lines.empty:
        logger.warning(f"No paired boundary lines found in network model")
        return network

    # Identify boundary lines pairs where the 'connected' status is inconsistent within each pairing_key group
    group = paired_boundary_lines.groupby('pairing_key')
    mask_connected = group['connected'].transform(lambda s: s.nunique() > 1)
    mask_tieflow = group['isTieflow'].transform(lambda s: s.nunique() > 1)

    mismatched_boundary_lines_con = paired_boundary_lines[mask_connected]
    logger.info(
        f"Boundary lines with non-matching connection status: {mismatched_boundary_lines_con['pairing_key'].unique().tolist()}")

    mismatched_boundary_lines_tie = paired_boundary_lines[mask_tieflow]
    logger.info(
        f"Boundary lines with non-matching cim:Tieflow: {mismatched_boundary_lines_tie['pairing_key'].unique().tolist()}")

    mismatched_boundary_lines = pd.concat([mismatched_boundary_lines_con, mismatched_boundary_lines_tie])

    # Set all mismatched lines to disconnected (False)
    _connected = pd.Series(data=False, index=mismatched_boundary_lines.index)
    network.update_boundary_lines(id=mismatched_boundary_lines.index, connected=_connected)

    # Log each change
    for i, row in mismatched_boundary_lines.iterrows():
        logger.info(f"Changed status of boundary line {row['name']}: {row['connected']} -> False")

    return network


def handle_igm_ssh_vs_cgm_ssh_error(network_pre_instance: pypowsybl.network.Network):
    """
    Implements various fixes to suppress igm ssh vs cgm ssh error
    1) Get all generators and remove them from slack distribution
    2) If generators have target_p outside the endpoints ('limits') of a curve then set it to be within
    3) Condensers p should not be modified so if it is not 0 then it sets the target_p to equal the existing p
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :return updated network_pre_instance
    """
    try:
        all_generators = network_pre_instance.get_elements(element_type=pypowsybl.network.ElementType.GENERATOR,
                                                           all_attributes=True).reset_index()

        # remove generatiors missing regulation control from regulation control
        all_generators_missing_reg_but_try_reg = all_generators[((all_generators["voltage_regulator_on"] == True) &
                                                                 (all_generators["CGMES.RegulatingControl"] == ""))]

        if not all_generators_missing_reg_but_try_reg.empty:
            logger.warning(
                f"Generators with regulation control missing but voltage_control on {len(all_generators_missing_reg_but_try_reg)}")
            # setting generators to false that do not have regulation
            network_pre_instance.update_generators(id=all_generators_missing_reg_but_try_reg["id"].values.tolist(),
                                                   voltage_regulator_on=[False] * len(
                                                       all_generators_missing_reg_but_try_reg["id"].values.tolist()))

        generators_mask = (all_generators['CGMES.synchronousMachineType'].str.contains('generator')) & (
                    all_generators['condenser'] == False) & (all_generators['target_p'] >= 0)
        not_generators = all_generators[~generators_mask]
        generators = all_generators[generators_mask]
        curve_points = (network_pre_instance
                        .get_elements(element_type=pypowsybl.network.ElementType.REACTIVE_CAPABILITY_CURVE_POINT,
                                      all_attributes=True).reset_index())
        curve_limits = (curve_points.merge(generators[['id']], on='id')
                        .groupby('id').agg(curve_p_min=('p', 'min'), curve_p_max=('p', 'max'))).reset_index()
        curve_generators = generators.merge(curve_limits, on='id')
        # low end can be zero
        curve_generators = curve_generators[(curve_generators['target_p'] > curve_generators['curve_p_max']) |
                                            ((curve_generators['target_p'] > 0) &
                                             (curve_generators['target_p'] < curve_generators['curve_p_min']))]
        if not curve_generators.empty:
            logger.warning(f"Found {len(curve_generators.index)} generators for "
                           f"which p > max(reactive capacity curve(p)) or p < min(reactive capacity curve(p))")

            # Solution 1: set max_p from curve max, it should contain p on target-p. those generators are also removed from regulation control
            upper_limit_violated = curve_generators[(curve_generators['max_p'] > curve_generators['curve_p_max'])]
            if not upper_limit_violated.empty:
                logger.warning(f"Updating max p from curve for {len(upper_limit_violated.index)} generators")
                upper_limit_violated['max_p'] = upper_limit_violated['curve_p_max']
                network_pre_instance.update_generators(
                    upper_limit_violated[['id', 'max_p']].assign(voltage_regulator_on=False).set_index('id'))

            lower_limit_violated = curve_generators[(curve_generators['min_p'] < curve_generators['curve_p_min'])]
            if not lower_limit_violated.empty:
                logger.warning(f"Updating min p from curve for {len(lower_limit_violated.index)} generators")
                lower_limit_violated.loc[:, 'min_p'] = lower_limit_violated['curve_p_min']
                network_pre_instance.update_generators(
                    lower_limit_violated[['id', 'min_p']].assign(voltage_regulator_on=False).set_index('id'))

            # Solution 2: discard generator from participating
            extensions = network_pre_instance.get_extensions('activePowerControl')
            remove_curve_generators = extensions.merge(curve_generators[['id']],
                                                       left_index=True, right_on='id')
            if not remove_curve_generators.empty:
                remove_curve_generators['participate'] = False
                network_pre_instance.update_extensions('activePowerControl',
                                                       remove_curve_generators.set_index('id'))
        condensers = all_generators[(all_generators['CGMES.synchronousMachineType'].str.contains('condenser'))
                                    & (abs(all_generators['p']) > 0)
                                    & (abs(all_generators['target_p']) == 0)]
        # Fix condensers that have p not zero by setting their target_p to equal to p
        if not condensers.empty:
            logger.warning(f"Found {len(condensers.index)} condensers for which p ~= 0 & target_p = 0")
            condensers.loc[:, 'target_p'] = condensers['p'] * (-1)
            network_pre_instance.update_generators(condensers[['id', 'target_p']].set_index('id'))
        # Remove all not generators from active power distribution
        if not not_generators.empty:
            logger.warning(f"Removing {len(not_generators.index)} machines from power distribution")
            extensions = network_pre_instance.get_extensions('activePowerControl')
            remove_not_generators = extensions.merge(not_generators[['id']], left_index=True, right_on='id')
            remove_not_generators['participate'] = False
            remove_not_generators = remove_not_generators.set_index('id')
            network_pre_instance.update_extensions('activePowerControl', remove_not_generators)

        # find all shunts missing regulating control class and turn the regulation control off for those
        all_shunts = network_pre_instance.get_elements(element_type=pypowsybl.network.ElementType.SHUNT_COMPENSATOR,
                                                       all_attributes=True)
        all_shunts_with_control_on_reg_missing = all_shunts[((all_shunts["voltage_regulation_on"] == True) &
                                                             (all_shunts["CGMES.RegulatingControl"] == ""))]

        if not all_shunts_with_control_on_reg_missing.empty:
            logger.warning(
                f"Shunts with regulation control missing but voltage_control on {len(all_shunts_with_control_on_reg_missing)}")
            # TODO set voltage_control_on to false. Atm try turning off
            network_pre_instance.update_shunt_compensators(
                id=all_shunts_with_control_on_reg_missing.index.values.tolist(),
                voltage_regulation_on=[False] * len(all_shunts_with_control_on_reg_missing.index.values.tolist()))


    except Exception as ex:
        logger.warning(f"Unable to pre-process for igm-cgm-ssh error: {ex}")

    return network_pre_instance


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

    return sanitize_nan(report)


def evaluate_trustability(report, properties) -> dict:
    reason = None
    if properties["merge_type"] == "BA":
        # Evaluate model trustability based on defined config and report keys
        report_keys = ['scaled', 'replaced', 'outages', 'acnp_schedule_replaced']
        property_keys = ['scaling', 'replacement']

        # Inline logic functions
        key_true = lambda key: lambda d: bool(d.get(key))
        all_ = lambda *rules: lambda d: all(rule(d) for rule in rules)
        all_none = lambda *keys, exclude=None: lambda d: all(d.get(k) is None for k in keys if k != exclude)

        # Compose conditions
        config_all_true = all_(*(key_true(k) for k in property_keys))
        success_all_true = all_(*(key_true(k) for k in report_keys))
        success_all_none = all_none(*report_keys, exclude='scaled')  # Scaling is never in None state

        # Evaluate logic
        config_enabled = config_all_true(properties)
        success_all_true = success_all_true(report)
        success_all_none = success_all_none(report)
        scaled_correctly = report['scaled']

        reason_map = {
            "scaled": "scaling failed",
            "replaced": "replacement failed",
            "outages": "outage fixing failed",
            "acnp_schedule_replaced": "acnp schedule replacement failed",
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


def filter_models(tsos: list, included_models: list | str = None, excluded_models: list | str = None):
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
        logger.info(f"Models to be included: {included_models} (pre-metadata-query)")
    elif excluded_models:
        logger.info(f"Models to be excluded: {excluded_models} (pre-metadata-query)")
    else:
        logger.info(f"Including all available models: {tsos} (pre-metadata-query)")
        return tsos

    filtered_tsos = []

    for tso in tsos:

        if included_models:
            if tso not in included_models:
                logger.info(f"Excluded {tso} (pre-metadata-query)")
                continue

        elif excluded_models:
            if tso in excluded_models:
                logger.info(f"Excluded {tso} (pre-metadata-query)")
                continue

        logger.info(f"Included {tso} (pre-metadata-query)")
        filtered_tsos.append(tso)

    return filtered_tsos


def filter_models_by_acnp(models: list, merged_model, acnp_dict, acnp_threshold, conform_load_factor):
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

    excluded_tso_ids = set()

    # ACNP deadband filter
    filtered_models = [model for model in models if is_within_acnp_deadband(model)]
    excluded_tsos = [
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
    excluded_tsos = [
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
    """
    Drop replacement models whose AC net position deviates too much from the
    scheduled ACNP for their TSO, or whose conform load cannot cover that
    deviation.

    Inputs are sanitized so malformed data (missing columns, non-numeric or
    missing values, an invalid acnp_dict/threshold/factor) never raises -
    rows that cannot be evaluated are kept unfiltered instead of failing.
    """
    required_columns = {'pmd:TSO', 'ac_net_position', 'sum_conform_load'}
    if models.empty or not isinstance(acnp_dict, dict) or not required_columns.issubset(models.columns):
        return models
    try:
        threshold, load_factor = float(acnp_threshold), float(conform_load_factor)
    except (TypeError, ValueError):
        return models

    acnp = pd.to_numeric(models['pmd:TSO'].map(acnp_dict), errors='coerce')
    deviation = (pd.to_numeric(models['ac_net_position'], errors='coerce') - acnp).abs()
    load = pd.to_numeric(models['sum_conform_load'], errors='coerce')

    keep = acnp.isna() | (deviation.notna() & (deviation <= threshold) & (load * load_factor > deviation))
    return models[keep]


def update_model_outages(merged_model: object, tso_list: list, scenario_datetime: str, time_horizon: str):
    # BRELL (Baltic-Russia/Belarus) EICs never have a merged neighbour - exclude them
    BRELL_XBORDER_EICS = ['10T-LT-RU-00001W', '10T-LT-RU-00002U', '10T-LT-RU-00003S', '10T-LV-RU-00001A',
                         '10T-BY-LT-000053', '10T-BY-LT-00001B', '10T-BY-LT-000029',
                         '10T-EE-RU-00001M', '10T-EE-RU-00002K', '10T-EE-RU-00003I', '10T-BY-LT-000045']
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
    last_uap_version = \
    elk_service.client.search(index='opc-outages-baltics*', body=body)['hits']['hits'][0]['fields']['reportParsedDate'][
        0]

    # Query for latest outage UAP
    uap_query = {"bool": {"must": [{"match": {"reportParsedDate": f"{last_uap_version}"}},
                                   {"match": {"Merge": merge_type}}]}}
    uap_outages = elk_service.get_docs_by_query(index='opc-outages-baltics*', query=uap_query, size=10000)

    # Filter out incorrect elements
    uap_outages['mrid'] = uap_outages['mrid'].replace("None", pd.NA)
    uap_outages = uap_outages[uap_outages["asset_type"] != "PROD"]

    # Map missing mrid by eic
    lookup = mrid_map.set_index(mrid_map[['eic', 'mrid']].columns[0])[mrid_map[['eic', 'mrid']].columns[1]]
    uap_outages.loc[:, 'mrid'] = uap_outages['mrid'].fillna(uap_outages['eic'].map(lookup))

    unmapped_outages = uap_outages[uap_outages['mrid'].isna()]
    # Exception rule for old LitPol element
    unmapped_outages = unmapped_outages[unmapped_outages['eic'] != "10T-LT-PL-000037"]
    if not unmapped_outages.empty:
        logger.warning(f"Unable to map following outage mRIDs: {unmapped_outages['name'].values}")

    # Filter outages according to model scenario date and replaced area
    filtered_outages = uap_outages[
        (uap_outages['start_date'] <= scenario_datetime) & (uap_outages['end_date'] >= scenario_datetime)]
    filtered_outages = filtered_outages[filtered_outages['Area'].isin(outage_areas)]
    mapped_outages = filtered_outages[~filtered_outages['mrid'].isna()]

    # Get disconnected elements in network model
    model_outages = pd.DataFrame(get_model_outages(network=merged_model.network))
    mapped_model_outages = pd.merge(model_outages, mrid_map, left_on='grid_id', right_on='mrid', how='inner')
    model_area_map = {"LITGRID": "LT", "AST": "LV", "ELERING": "EE"}
    model_outage_areas = [model_area_map.get(item, item) for item in tso_list]
    filtered_model_outages = mapped_model_outages[mapped_model_outages['country'].isin(model_outage_areas)]

    # Include cross-border lines for reconnection, but only when the neighbour is actually
    # paired (tie_line_id set) - unpaired means there's no neighbour to safely sync with.
    boundary_lines = get_network_elements(network=merged_model.network,
                                          element_type=pypowsybl.network.ElementType.BOUNDARY_LINE).reset_index(
        names=['grid_id'])

    additional_boundary_lines = boundary_lines.iloc[0:0]
    if 'pairing_key' in boundary_lines.columns and 'pairing_key' in model_outages.columns:
        border_lines = boundary_lines[boundary_lines['pairing_key'].isin(model_outages['pairing_key'])]
        relevant_border_lines = border_lines[border_lines['country'].isin(model_outage_areas)]
        # Removing any BRELL lines - exact EIC match, not a 'contains RU' substring guess
        relevant_border_lines = relevant_border_lines[
            ~relevant_border_lines['lineEnergyIdentificationCodeEIC'].isin(BRELL_XBORDER_EICS)]

        if 'tie_line_id' in relevant_border_lines.columns:
            is_paired = relevant_border_lines['tie_line_id'].fillna('') != ''
        else:
            is_paired = pd.Series(False, index=relevant_border_lines.index)

        unpaired = relevant_border_lines[~is_paired]
        if not unpaired.empty:
            logger.warning(f"Neighbour not paired, reconnecting local side only: "
                           f"{unpaired[['name', 'grid_id']].to_dict('records')}")

        paired_lines = relevant_border_lines[is_paired]
        additional_boundary_lines = boundary_lines[boundary_lines['pairing_key'].isin(paired_lines['pairing_key'])]

        # Paired just means present - check the neighbour's own half against the live plan
        # before reconnecting it, since the plan is the source of truth, not the local side.
        neighbour_side = additional_boundary_lines[~additional_boundary_lines['country'].isin(model_outage_areas)]
        if not neighbour_side.empty:
            now_in_outage_mrids = set(
                uap_outages.loc[
                    (uap_outages['start_date'] <= scenario_datetime) & (uap_outages['end_date'] >= scenario_datetime),
                    'mrid'
                ].dropna().str.lstrip('_'))
            # unmapped eic -> mrid is unverifiable, not confirmed-clear; treat as still-outaged
            cant_verify = neighbour_side['lineEnergyIdentificationCodeEIC'].isin(unmapped_outages['eic'])
            neighbour_still_outaged = neighbour_side[
                neighbour_side['grid_id'].isin(now_in_outage_mrids) | cant_verify]

            if not neighbour_still_outaged.empty:
                logger.warning(f"Neighbour still outaged or unverified, left untouched: "
                               f"{neighbour_still_outaged[['name', 'grid_id']].to_dict('records')}")
            additional_boundary_lines = additional_boundary_lines[
                ~additional_boundary_lines['grid_id'].isin(neighbour_still_outaged['grid_id'])]

    # Merged dataframe of network elements to be reconnected
    filtered_model_outages = pd.concat([filtered_model_outages, additional_boundary_lines]).drop_duplicates(
        subset='grid_id')
    filtered_model_outages = filtered_model_outages.where(pd.notnull(filtered_model_outages), None)

    # rename columns
    filtered_model_outages = filtered_model_outages.copy()[['name', 'grid_id', 'eic']].rename(
        columns={'grid_id': 'mrid'})
    mapped_outages = mapped_outages[['name', 'mrid', 'eic']].copy()
    mapped_outages.loc[:, 'mrid'] = mapped_outages['mrid'].str.lstrip('_')

    # Don't reconnect something the live plan still wants disconnected - it would just get
    # disconnected again by the loop below.
    filtered_model_outages = filtered_model_outages[~filtered_model_outages['mrid'].isin(mapped_outages['mrid'])]

    logger.info(f"Updating outages in merged model areas: {model_outage_areas}")

    # Reconnecting outages from network-config list
    outages_updated = {}
    reconnected, already_connected, failed_connect = [], [], []
    filtered_model_outages["eic"] = (
        filtered_model_outages["eic"].astype(object).where(filtered_model_outages["eic"].notna(), None))
    for index, outage in filtered_model_outages.iterrows():
        try:
            if merged_model.network.connect(outage['mrid']):
                merged_model.outages = True
                outage_dict = outage.to_dict()
                outage_dict.update({'status': 'connected'})
                outages_updated[outage_dict['mrid']] = outage_dict
                reconnected.append(outage['name'])
            elif uap_outages['mrid'].str.contains(("_" + outage['mrid']), regex=False).any():
                already_connected.append(outage['name'])
            else:
                failed_connect.append(outage['name'])
                merged_model.outages_unmapped.extend(
                    [{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
        except Exception as e:
            logger.error(f"Failed to reconnect element {outage['name']} [mrid: {outage['mrid']}]: {e}", exc_info=True)
            failed_connect.append(outage['name'])
            merged_model.outages_unmapped.extend(
                [{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
            merged_model.outages = False

    if reconnected:
        logger.info(f"Reconnected: {reconnected}")
    if already_connected:
        logger.info(f"Already connected: {already_connected}")
    if failed_connect:
        logger.error(f"Failed to reconnect: {failed_connect}")

    # Applying outages from UAP
    disconnected, already_outaged, failed_disconnect = [], [], []
    mapped_outages["eic"] = (mapped_outages["eic"].astype(object).where(mapped_outages["eic"].notna(), None))
    for index, outage in mapped_outages.iterrows():
        try:
            if merged_model.network.disconnect(outage['mrid']):
                merged_model.outages = True
                outage_dict = outage.to_dict()
                outage_dict.update({'status': 'disconnected'})
                outages_updated[outage_dict['mrid']] = outage_dict
                disconnected.append(outage['name'])
            elif uap_outages['mrid'].str.contains(("_" + outage['mrid']), regex=False).any():
                already_outaged.append(outage['name'])
            else:
                failed_disconnect.append(outage['name'])
                merged_model.outages_unmapped.extend(
                    [{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
        except Exception as e:
            logger.error(f"Failed to disconnect element {outage['name']} [mrid: {outage['mrid']}]: {e}", exc_info=True)
            failed_disconnect.append(outage['name'])
            merged_model.outages_unmapped.extend(
                [{"name": outage['name'], "mrid": outage['mrid'], "eic": outage['eic']}])
            merged_model.outages = False

    if disconnected:
        logger.info(f"Disconnected: {disconnected}")
    if already_outaged:
        logger.info(f"Already in outage: {already_outaged}")
    if failed_disconnect:
        logger.error(f"Failed to disconnect: {failed_disconnect}")

    # Keep only important keys of updated outages
    merged_model.outages_updated = list(outages_updated.values())

    # Safety net: re-check every paired tie line this run touched for a resulting mismatch
    # between its two halves. Not auto-corrected, just logged for visibility.
    touched_grid_ids = pd.concat([
        filtered_model_outages.get('mrid', pd.Series(dtype=str)),
        mapped_outages.get('mrid', pd.Series(dtype=str)),
    ]).dropna().unique()

    if 'pairing_key' in boundary_lines.columns and len(touched_grid_ids):
        current_boundary_lines = get_network_elements(
            network=merged_model.network, element_type=pypowsybl.network.ElementType.BOUNDARY_LINE
        ).reset_index(names=['grid_id'])

        if 'tie_line_id' in current_boundary_lines.columns:
            touched_keys = current_boundary_lines.loc[
                current_boundary_lines['grid_id'].isin(touched_grid_ids), 'pairing_key'].unique()
            paired_touched = current_boundary_lines[
                current_boundary_lines['pairing_key'].isin(touched_keys) &
                (current_boundary_lines['tie_line_id'].fillna('') != '')]

            mismatched = [pair for _, pair in paired_touched.groupby('pairing_key') if pair['connected'].nunique() > 1]
            if mismatched:
                logger.warning(f"Cross-border pairing mismatches after outage update: "
                               f"{pd.concat(mismatched)[['name', 'grid_id', 'country', 'connected']].to_dict('records')}")

    if merged_model.outages_unmapped:
        merged_model.outages = False

    # Sanitise NaN values in merge report
    merged_model.outages_updated = [{k: None if isinstance(v, float) and math.isnan(v) else v for k, v in d.items()}
                                    for d in merged_model.outages_updated]
    merged_model.outages_unmapped = [{k: None if isinstance(v, float) and math.isnan(v) else v for k, v in d.items()}
                                     for d in merged_model.outages_unmapped]

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


def lvl8_report_cgm(merge_report: dict):
    # Create <QAReport> root
    qa_attribs = {
        'created': datetime.datetime.strptime(merge_report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime(
            '%Y-%m-%dT%H:%M:%SZ'),
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

    # if scaling is failed then set error from error list
    if not merge_report['scaled']:
        violations.append(violations_list[2])
        quality_indicator_cgm = "Invalid - inconsistent data"

    # Create <CGM>
    cgm_attribs = {
        'created': datetime.datetime.strptime(merge_report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime(
            '%Y-%m-%dT%H:%M:%SZ'),
        'resource': merge_report['network_meta']['fullModel_ID'],  # TODO get here correct content ID
        'scenarioTime': datetime.datetime.fromisoformat(merge_report["@scenario_timestamp"]).strftime(
            '%Y-%m-%dT%H:%M:%SZ'),
        'version': str(merge_report["@version"]),
        'processType': merge_report["time_horizon_id"] if merge_report["@time_horizon"] == 'ID' else merge_report[
            "@time_horizon"],
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
