import config
from emf.loadflow_tool.helper import load_model, load_opdm_data, filename_from_metadata, attr_to_dict, export_model
from emf.loadflow_tool import loadflow_settings
import pypowsybl

import logging
import json
import sys
from aniso8601 import parse_datetime
from decimal import Decimal
import datetime

import triplets
import pandas

from uuid import uuid4

logger = logging.getLogger(__name__)
SV_INJECTION_LIMIT = 0.1


def run_lf(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT):

    loadflow_report = pypowsybl.report.Reporter()

    loadflow_result = pypowsybl.loadflow.run_ac(network=merged_model["network"],
                                                parameters=loadflow_settings,
                                                reporter=loadflow_report)

    loadflow_result_dict = [attr_to_dict(island) for island in loadflow_result]
    # merged_model["LOADFLOW_REPORT"] = json.loads(loadflow_report.to_json())
    merged_model["LOADFLOW_REPORT"] = str(loadflow_report)
    merged_model["LOADFLOW_RESULTS"] = loadflow_result_dict

    return merged_model


def create_opdm_object_meta(object_id,
                            time_horizon,
                            merging_entity,
                            merging_area,
                            scenario_date,
                            mas,
                            version,
                            profile,
                            content_type = "CGMES",
                            file_type ="xml"
                            ):
    opdm_object_meta = {
        'pmd:fullModel_ID': object_id,
        'pmd:creationDate': f"{datetime.datetime.utcnow():%Y-%m-%dT%H:%M:%S.%fZ}",
        'pmd:timeHorizon': time_horizon,
        'pmd:cgmesProfile': profile,
        'pmd:contentType': content_type,
        'pmd:modelPartReference': '',
        'pmd:mergingEntity': merging_entity,
        'pmd:mergingArea': merging_area,
        'pmd:validFrom': f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}",
        'pmd:modelingAuthoritySet': mas,
        'pmd:scenarioDate': f"{parse_datetime(scenario_date):%Y-%m-%dT%H:%M:00Z}",
        'pmd:modelid': object_id,
        'pmd:description': f"""<MDE>
                                <BP>{time_horizon}</BP>
                                <TOOL>pypowsybl_{pypowsybl.__version__}</TOOL>
                                <RSC>{merging_entity}</RSC>
                                <TXT>Model: Simplification of reality for given need.</TXT>
                            </MDE>""",
        'pmd:versionNumber': f"{int(version):03d}",
        'file_type': file_type
    }

    return opdm_object_meta



def update_FullModel_from_OpdmObject(data, opdm_object):
    return triplets.cgmes_tools.update_FullModel_from_dict(data, metadata={
        "Model.version": f"{int(opdm_object['pmd:versionNumber']):03d}",
        "Model.created": f"{parse_datetime(opdm_object['pmd:creationDate']):%Y-%m-%dT%H:%M:%S.%fZ}",
        "Model.mergingEntity": opdm_object['pmd:mergingEntity'],
        "Model.domain": opdm_object['pmd:mergingArea'],
        "Model.scenarioTime": f"{parse_datetime(opdm_object['pmd:scenarioDate']):%Y-%m-%dT%H:%M:00Z}",
        "Model.description": opdm_object['pmd:description'],
        "Model.processType": opdm_object['pmd:timeHorizon']
    })


def create_sv_and_updated_ssh(merged_model, original_models, scenario_date, time_horizon, version, merging_area, merging_entity, mas):

    ### SV ###
    # Set Metadata
    SV_ID = merged_model['network_meta']['id'].split("uuid:")[-1]

    opdm_object_meta = create_opdm_object_meta(SV_ID,
                                               time_horizon,
                                               merging_entity,
                                               merging_area,
                                               scenario_date,
                                               mas,
                                               version,
                                               profile="SV")


    exported_model = export_model(merged_model["network"], opdm_object_meta, ["SV"])
    logger.info(f"Exporting merged model to {exported_model.name}")

    # Load SV data
    sv_data = pandas.read_RDF([exported_model])

    # Update
    sv_data.set_VALUE_at_KEY(key='label', value=filename_from_metadata(opdm_object_meta))
    sv_data = triplets.cgmes_tools.update_FullModel_from_filename(sv_data)

    # Update metadata
    sv_data = update_FullModel_from_OpdmObject(sv_data, opdm_object_meta)

    # Update filename
    sv_data = triplets.cgmes_tools.update_filename_from_FullModel(sv_data)

    ### SSH ##

    # Load original SSH data to created updated SSH
    ssh_data = load_opdm_data(original_models, "SSH")
    ssh_data = triplets.cgmes_tools.update_FullModel_from_filename(ssh_data)

    # Update SSH Model.scenarioTime
    ssh_data.set_VALUE_at_KEY('Model.scenarioTime', opdm_object_meta['pmd:scenarioDate'])

    # Load full original data to fix issues
    data = load_opdm_data(original_models)
    terminals = data.type_tableview("Terminal")

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
    terminals = load_opdm_data(original_models).type_tableview("Terminal")

    # Update
    for update in ssh_update_map:
        logger.info(f"Updating: {update['from_attribute']} -> {update['to_attribute']}")
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
        NEW_ID = str(uuid4())
        updated_ssh_id_map[OLD_ID] = NEW_ID
        logger.info(f"Assigned new UUID for updated SSH: {OLD_ID} -> {NEW_ID}")

    # Update SSH ID-s
    ssh_data = ssh_data.replace(updated_ssh_id_map)

    # Update in SV SSH references
    sv_data = sv_data.replace(updated_ssh_id_map)

    # Add SSH supersedes reference to old SSH
    ssh_supersedes_data = pandas.DataFrame([{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
    ssh_supersedes_data['INSTANCE_ID'] = ssh_data.query("KEY == 'Type'").merge(ssh_supersedes_data.ID)['INSTANCE_ID']
    ssh_data = ssh_data.update_triplet_from_triplet(ssh_supersedes_data)

    # Update SSH metadata
    ssh_data = update_FullModel_from_OpdmObject(ssh_data, opdm_object_meta)

    # Update SSH filenames
    filename_mask = "{scenarioTime:%Y%m%dT%H%MZ}_{processType}_{mergingEntity}-{domain}-{forEntity}_{messageType}_{version:03d}"
    ssh_data = triplets.cgmes_tools.update_filename_from_FullModel(ssh_data, filename_mask=filename_mask)

    return sv_data, ssh_data


def fix_sv_shunts(sv_data, original_data):
    """Remove Shunt Sections for EQV Shunts"""

    equiv_shunt = load_opdm_data(original_data, "EQ").query("KEY == 'Type' and VALUE == 'EquivalentShunt'")
    if len(equiv_shunt) > 0:
        shunts_to_remove = sv_data.merge(
            sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").merge(equiv_shunt.ID, left_on='VALUE',
                                                                                        right_on="ID", how='inner',
                                                                                        suffixes=('', '_EQVShunt')).ID)
        if len(shunts_to_remove) > 0:
            logger.warning(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
            sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

    return sv_data


def fix_sv_tapsteps(sv_data, ssh_data):
    """Fix SV - Remove Shunt Sections for EQV Shunts"""

    ssh_tap_steps = ssh_data.query("KEY == 'TapChanger.step'")
    sv_tap_steps = sv_data.query("KEY == 'SvTapStep.TapChanger'")

    missing_sv_tap_steps = ssh_tap_steps.merge(sv_tap_steps[['VALUE']], left_on='ID', right_on="VALUE", how='left', indicator=True, suffixes=('', '_SV')).query("_merge == 'left_only'")

    tap_steps_to_be_added = []
    SV_INSTANCE_ID = sv_data.INSTANCE_ID.iloc[0]
    for tap_changer in missing_sv_tap_steps.itertuples():
        ID = str(uuid4())
        logger.warning(
            f'Missing SvTapStep for {tap_changer.ID}, adding SvTapStep {ID} and taking tap value {tap_changer.VALUE} from SSH')
        tap_steps_to_be_added.extend([
            (ID, 'Type', 'SvTapStep', SV_INSTANCE_ID),
            (ID, 'SvTapStep.TapChanger', tap_changer.ID, SV_INSTANCE_ID),
            (ID, 'SvTapStep.position', tap_changer.VALUE, SV_INSTANCE_ID),
        ])

    sv_data = pandas.concat([sv_data, pandas.DataFrame(tap_steps_to_be_added, columns=['ID', 'KEY', 'VALUE', 'INSTANCE_ID'])], ignore_index=True)
    return sv_data


def configure_paired_boundarypoint_injections(data):
    """Where there are paired boundary points, eqivalent injections need to be modified
    Set P and Q to 0 - so that no additional consumption or prduction is on tieline
    Set voltage control off - so that no additional consumption or prduction is on tieline
    Set terminal to connected - to be sure we have paired connected injections at boundary point
    """

    boundary_points = data.query("KEY == 'ConnectivityNode.boundaryPoint' and VALUE == 'true'")[["ID"]]
    #boundary_points = data.type_tableview("ConnectivityNode").reset_index().query("`ConnectivityNode.boundaryPoint` == 'true'")
    boundary_points = boundary_points.merge(data.type_tableview("Terminal").reset_index(), left_on="ID", right_on="Terminal.ConnectivityNode", suffixes=('_ConnectivityNode', '_Terminal'))

    injections = data.type_tableview('EquivalentInjection').reset_index().merge(boundary_points, left_on="ID", right_on='Terminal.ConductingEquipment', suffixes=('_ConnectivityNode', ''))

    # Get paired injections at boundary points
    paired_injections = injections.groupby("Terminal.ConnectivityNode").filter(lambda x: len(x) == 2)

    # Set terminal status
    updated_terminal_status = paired_injections[["ID_Terminal"]].copy().rename(columns={"ID_Terminal": "ID"})
    updated_terminal_status["KEY"] = "ACDCTerminal.connected"
    updated_terminal_status["VALUE"] = "true"

    # Set Regulation off
    updated_regulation_status = paired_injections[["ID"]].copy()
    updated_regulation_status["KEY"] = "EquivalentInjection.regulationStatus"
    updated_regulation_status["VALUE"] = "false"

    # Set P to 0
    updated_p_value = paired_injections[["ID"]].copy()
    updated_p_value["KEY"] = "EquivalentInjection.p"
    updated_p_value["VALUE"] = 0

    # Set Q to 0
    updated_q_value = paired_injections[["ID"]].copy()
    updated_q_value["KEY"] = "EquivalentInjection.q"
    updated_q_value["VALUE"] = 0

    return data.update_triplet_from_triplet(pandas.concat([updated_terminal_status, updated_regulation_status, updated_p_value, updated_q_value], ignore_index=True), add=False)



def export_to_cgmes_zip(triplets: list):
    namespace_map = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
        "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
        "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    }

    # with open('../../config/cgm_worker/CGMES_v2_4_15_2014_08_07.json', 'r') as file_object:
    rdf_map = json.load(config.paths.cgm_worker.CGMES_v2_4_15_2014_08_07)

    return pandas.concat(triplets, ignore_index=True).export_to_cimxml(rdf_map=rdf_map,
                                                                       namespace_map=namespace_map,
                                                                       export_undefined=False,
                                                                       export_type="xml_per_instance_zip_per_xml",
                                                                       debug=False,
                                                                       export_to_memory=True)


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


def get_opdm_data_from_models(model_data: list | pandas.DataFrame):
    """
    Check if input is already parsed to triplets. Do it otherwise
    :param model_data: input models
    :return triplets
    """
    if not isinstance(model_data, pandas.DataFrame):
        model_data = load_opdm_data(model_data)
    return model_data


def get_boundary_nodes_between_igms(model_data: list | pandas.DataFrame):
    """
    Filters out nodes that are between the igms (mentioned at least 2 igms)
    :param model_data: input models
    : return series of node ids
    """
    model_data = get_opdm_data_from_models(model_data=model_data)
    all_boundary_nodes = model_data[(model_data['KEY'] == 'TopologicalNode.boundaryPoint') &
                                    (model_data['VALUE'] == 'true')]
    # Get boundary nodes that exist in igms
    merged = pandas.merge(all_boundary_nodes,
                          model_data[(model_data['KEY'] == 'SvVoltage.TopologicalNode')],
                          left_on='ID', right_on='VALUE', suffixes=('_y', ''))
    # Get duplicates (all of them) then duplicated values. keep=False marks all duplicates True, 'first' marks first
    # occurrence to false, 'last' marks last occurrence to false. If any of them is used then in case duplicates are 2
    # then 1 is retrieved, if duplicates >3 then duplicates-1 retrieved. So, get all the duplicates and as a second
    # step, drop the duplicates
    merged = (merged[merged.duplicated(['VALUE'], keep=False)]).drop_duplicates(subset=['VALUE'])
    in_several_igms = (merged["VALUE"]).to_frame().rename(columns={'VALUE': 'ID'})
    return in_several_igms


def take_best_match_for_sv_voltage(input_data, column_name: str = 'v', to_keep: bool = True):
    """
    Returns one row for with sv voltage id for topological node
    1) Take the first
    2) If first is zero take first non-zero row if exists
    :param input_data: input dataframe
    :param column_name: name of the column
    :param to_keep: either to keep or discard a value
    """
    first_row = input_data.iloc[0]
    if to_keep:
        remaining_rows = input_data[input_data[column_name] != 0]
        if first_row[column_name] == 0 and not remaining_rows.empty:
            first_row = remaining_rows.iloc[0]
    else:
        remaining_rows = input_data[input_data[column_name] == 0]
        if first_row[column_name] != 0 and not remaining_rows.empty:
            first_row = remaining_rows.iloc[0]
    return first_row


def remove_duplicate_sv_voltages(cgm_sv_data, original_data):
    """
    Pypowsybl 1.6.0 provides multiple sets of SvVoltage values for the topological nodes that are boundary nodes (from
    each IGM side that uses the corresponding boundary node). So this is a hack that removes one of them (preferably the
    one that is zero).
    :param cgm_sv_data: merged SV profile from where duplicate SvVoltage values are removed
    :param original_data: will be used to get boundary node ids
    :return updated merged SV profile
    """
    # Check that models are in triplets
    some_data = get_opdm_data_from_models(model_data=original_data)
    # Get ids of boundary nodes that are shared by several igms
    in_several_igms = (get_boundary_nodes_between_igms(model_data=some_data))
    # Get SvVoltage Ids corresponding to shared boundary nodes
    sv_voltage_ids = pandas.merge(cgm_sv_data[cgm_sv_data['KEY'] == 'SvVoltage.TopologicalNode'],
                                  in_several_igms.rename(columns={'ID': 'VALUE'}), on='VALUE')
    # Get SvVoltage voltage values for corresponding SvVoltage Ids
    sv_voltage_values = pandas.merge(cgm_sv_data[cgm_sv_data['KEY'] == 'SvVoltage.v'][['ID', 'VALUE']].
                                     rename(columns={'VALUE': 'SvVoltage.v'}),
                                     sv_voltage_ids[['ID', 'VALUE']].
                                     rename(columns={'VALUE': 'SvVoltage.SvTopologicalNode'}), on='ID')
    # Just in case convert the values to numeric
    sv_voltage_values[['SvVoltage.v']] = (sv_voltage_values[['SvVoltage.v']].apply(lambda x: x.apply(Decimal)))
    # Group by topological node id and by some logic take SvVoltage that will be dropped
    voltages_to_discard = (sv_voltage_values.groupby(['SvVoltage.SvTopologicalNode']).
                           apply(lambda x: take_best_match_for_sv_voltage(input_data=x,
                                                                          column_name='SvVoltage.v',
                                                                          to_keep=False), include_groups=False))
    if not voltages_to_discard.empty:
        logger.info(f"Removing {len(voltages_to_discard.index)} duplicate voltage levels from boundary nodes")
        sv_voltages_to_remove = pandas.merge(cgm_sv_data, voltages_to_discard['ID'].to_frame(), on='ID')
        cgm_sv_data = triplets.rdf_parser.remove_triplet_from_triplet(cgm_sv_data, sv_voltages_to_remove)
    return cgm_sv_data


def check_and_fix_dependencies(cgm_sv_data, cgm_ssh_data, original_data):
    """
    Seems that pypowsybl ver 1.6.0 managed to get rid of dependencies in exported file. This gathers them from
    SSH profiles and from the original models
    :param cgm_sv_data: merged SV profile that is missing the dependencies
    :param cgm_ssh_data: merged SSH profiles, will be used to get SSH dependencies
    :param original_data: original models, will be used to get TP dependencies
    :return updated merged SV profile
    """
    some_data = get_opdm_data_from_models(model_data=original_data)
    tp_file_ids = some_data[(some_data['KEY'] == 'Model.profile') & (some_data['VALUE'].str.contains('Topology'))]

    ssh_file_ids = cgm_ssh_data[(cgm_ssh_data['KEY'] == 'Model.profile') &
                                (cgm_ssh_data['VALUE'].str.contains('SteadyStateHypothesis'))]
    dependencies = pandas.concat([tp_file_ids, ssh_file_ids], ignore_index=True, sort=False)
    existing_dependencies = cgm_sv_data[cgm_sv_data['KEY'] == 'Model.DependentOn']
    if existing_dependencies.empty or len(existing_dependencies.index) < len(dependencies.index):
        logger.info(f"Missing dependencies. Adding {len(dependencies.index)} dependencies to SV profile")
        full_model_id = cgm_sv_data[(cgm_sv_data['KEY'] == 'Type') & (cgm_sv_data['VALUE'] == 'FullModel')]
        new_dependencies = dependencies[['ID']].copy().rename(columns={'ID': 'VALUE'}).reset_index(drop=True)
        new_dependencies.loc[:, 'KEY'] = 'Model.DependentOn'
        new_dependencies.loc[:, 'ID'] = full_model_id['ID'].iloc[0]
        new_dependencies.loc[:, 'INSTANCE_ID'] = full_model_id['INSTANCE_ID'].iloc[0]
        cgm_sv_data = triplets.rdf_parser.update_triplet_from_triplet(cgm_sv_data, new_dependencies)
    return cgm_sv_data


def remove_small_islands(solved_data, island_size_limit):
    small_island = pandas.DataFrame(solved_data.query("KEY == 'TopologicalIsland.TopologicalNodes'").ID.value_counts()).reset_index().query("count <= @island_size_limit")
    solved_data = triplets.rdf_parser.remove_triplet_from_triplet(solved_data, small_island, columns=["ID"])
    logger.info(f"Removed {len(small_island)} island(s) with size <= {island_size_limit}")
    return solved_data


def disconnect_equipment_if_flow_sum_not_zero(cgm_sv_data,
                                              cgm_ssh_data,
                                              original_data,
                                              equipment_name: str = "ConformLoad",
                                              sv_injection_limit: float = SV_INJECTION_LIMIT):
    """
    If there is a mismatch of flows at topological nodes it tries to switch of and set flows at terminals
    indicated by equipment_name to original values.
    The idea is that when loadflow calculation fails at some island, the results are still being updated and as
    currently there is not a better way to find the islands-> nodes -> terminals on which it fails then the HACK
    is to try to set them back to original values.
    NOTE THAT IT NOT ONLY SETS THE VALUES TO OLD ONES BUT ALSO DISCONNECTS IT FROM TERMINAL
    :param cgm_ssh_data: merged SSH profile (needed to switch the terminals of)
    :param cgm_sv_data: merged SV profile (needed to set the flows for terminals)
    :param original_data: IGMs (triplets, dictionary)
    :param equipment_name: name of the equipment. CURRENTLY, IT IS USED FOR CONFORM LOAD
    :param sv_injection_limit: threshold for deciding whether the node is violated by sum of flows
    :return updated merged SV and SSH profiles
    """
    original_data = get_opdm_data_from_models(model_data=original_data)
    # Get power flow after lf
    power_flow = cgm_sv_data.type_tableview('SvPowerFlow')[['SvPowerFlow.Terminal', 'SvPowerFlow.p', 'SvPowerFlow.q']]
    # Get terminals
    terminals = original_data.type_tableview('Terminal').rename_axis('Terminal').reset_index()
    terminals = terminals[['Terminal', 'Terminal.ConductingEquipment', 'Terminal.TopologicalNode']]
    # Calculate summed flows per topological node
    flows_summed = ((power_flow.merge(terminals, left_on='SvPowerFlow.Terminal', right_on='Terminal', how='left')
                     .groupby('Terminal.TopologicalNode')[['SvPowerFlow.p', 'SvPowerFlow.q']]
                     .sum()).rename_axis('Terminal.TopologicalNode').reset_index())
    # Get topological nodes that have mismatch
    nok_nodes = flows_summed[(abs(flows_summed['SvPowerFlow.p']) > sv_injection_limit) |
                             (abs(flows_summed['SvPowerFlow.q']) > sv_injection_limit)][['Terminal.TopologicalNode']]
    # Merge terminals with summed flows at nodes
    terminals_nodes = terminals.merge(flows_summed, on='Terminal.TopologicalNode', how='left')
    # Get equipment names
    equipment_names = (original_data.query('KEY == "Type"')[['ID', 'VALUE']]
                       .drop_duplicates().rename(columns={'ID': 'Terminal.ConductingEquipment',
                                                          'VALUE': 'Equipment_name'}))
    # Merge terminals with equipment names
    terminals_equipment = terminals_nodes.merge(equipment_names, on='Terminal.ConductingEquipment', how='left')
    # Get equipment lines corresponding to nodes that had mismatch
    if not nok_nodes.empty:
        logger.error(f"For {len(nok_nodes.index)} topological nodes, the sum of flows is over {sv_injection_limit}")
        nok_lines = (terminals_equipment.merge(nok_nodes, on='Terminal.TopologicalNode')
                     .sort_values(by=['Terminal.TopologicalNode']))
        nok_loads = nok_lines[nok_lines['Equipment_name'] == equipment_name]
        if not nok_loads.empty:
            logger.warning(f"Switching off {len(nok_loads.index)} terminals as they contain {equipment_name}")
            # Copy values from original models
            old_power_flows = original_data.type_tableview('SvPowerFlow')[['SvPowerFlow.Terminal',
                                                                           'SvPowerFlow.p', 'SvPowerFlow.q']]
            old_power_flows = (old_power_flows
                               .merge(nok_loads[['Terminal']].rename(columns={'Terminal': 'SvPowerFlow.Terminal'}),
                                      on='SvPowerFlow.Terminal'))
            new_power_flows = cgm_sv_data.type_tableview('SvPowerFlow')[['SvPowerFlow.Terminal', 'Type']]
            new_power_flows = (new_power_flows.reset_index().merge(old_power_flows, on='SvPowerFlow.Terminal')
                               .set_index('ID'))
            # Update values in SV profile
            cgm_sv_data = triplets.rdf_parser.update_triplet_from_tableview(cgm_sv_data, new_power_flows)
            # Just in case disconnect those things also
            terminals_in_ssh = cgm_ssh_data[cgm_ssh_data["KEY"].str.contains("Terminal.connected")].merge(
                nok_loads[["Terminal"]].rename(columns={'Terminal': 'ID'}), on='ID')
            terminals_in_ssh.loc[:, 'VALUE'] = 'false'
            cgm_ssh_data = triplets.rdf_parser.update_triplet_from_triplet(cgm_ssh_data, terminals_in_ssh)
    return cgm_sv_data, cgm_ssh_data


if __name__ == "__main__":

    from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download

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

    merged_model = load_model(valid_models + [latest_boundary])

    # TODO - run other LF if default fails
    solved_model = run_lf(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT)

    # TODO - get version dynamically form ELK
    sv_data, ssh_data = create_sv_and_updated_ssh(solved_model, valid_models, time_horizon, version, merging_area, merging_entity, mas)

    # Fix SV
    sv_data = fix_sv_shunts(sv_data, valid_models)
    sv_data = fix_sv_tapsteps(sv_data, ssh_data)

    # Package to in memory zip files
    serialized_data = export_to_cgmes_zip([ssh_data, sv_data])

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

    # Emport to EDX

    # Export to MINIO + ELK
