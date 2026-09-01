import json
import uuid
import logging
import config
import pandas as pd
import pypowsybl
import triplets
from decimal import Decimal
from emf.common.config_parser import parse_app_properties
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets, get_opdm_data_from_models
from emf.model_merger import merge_functions

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.post_processing)


def remove_small_islands(solved_data, island_size_limit):
    small_island = pd.DataFrame(
        solved_data.query("KEY == 'TopologicalIsland.TopologicalNodes'").ID.value_counts()).reset_index().query(
        "count <= @island_size_limit")
    solved_data = triplets.rdf_parser.remove_triplet_from_triplet(solved_data, small_island, columns=["ID"])
    logger.info(f"Removed {len(small_island)} island(s) with size <= {island_size_limit}")
    return solved_data


def remove_equivalent_shunt_section(sv_data: pd.DataFrame, models_as_triplets: pd.DataFrame):
    """Remove Shunt Sections for EQV Shunts from SV profile"""

    equiv_shunt = models_as_triplets.query("KEY == 'Type' and VALUE == 'EquivalentShunt'")
    if len(equiv_shunt) > 0:
        shunts_to_remove = sv_data.merge(
            sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").merge(equiv_shunt.ID, left_on='VALUE',
                                                                                        right_on="ID", how='inner',
                                                                                        suffixes=('', '_EQVShunt')).ID)
        if len(shunts_to_remove) > 0:
            logger.info(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
            sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

    return sv_data


def add_missing_sv_tap_steps(sv_data: pd.DataFrame, ssh_data: pd.DataFrame):
    """Update missing tap changer tap steps in SV"""

    ssh_tap_steps = ssh_data.query("KEY == 'TapChanger.step'")
    sv_tap_steps = sv_data.query("KEY == 'SvTapStep.TapChanger'")

    missing_sv_tap_steps = ssh_tap_steps.merge(sv_tap_steps[['VALUE']],
                                               left_on='ID',
                                               right_on="VALUE",
                                               how='left',
                                               indicator=True,
                                               suffixes=('', '_SV')).query("_merge == 'left_only'")

    tap_steps_to_be_added = []
    SV_INSTANCE_ID = sv_data.INSTANCE_ID.iloc[0]
    if not missing_sv_tap_steps.empty:
        logger.info(f"Adding {len(missing_sv_tap_steps.index)} missing SvTapStep(s), taking tap value from SSH")
    for tap_changer in missing_sv_tap_steps.itertuples():
        ID = str(uuid.uuid4())
        logger.debug(
            f'Missing SvTapStep for {tap_changer.ID}, adding SvTapStep {ID} and taking tap value {tap_changer.VALUE} from SSH')
        tap_steps_to_be_added.extend([
            (ID, 'Type', 'SvTapStep', SV_INSTANCE_ID),
            (ID, 'SvTapStep.TapChanger', tap_changer.ID, SV_INSTANCE_ID),
            (ID, 'SvTapStep.position', tap_changer.VALUE, SV_INSTANCE_ID),
        ])

    sv_data = pd.concat([sv_data, pd.DataFrame(tap_steps_to_be_added, columns=['ID', 'KEY', 'VALUE', 'INSTANCE_ID'])],
                        ignore_index=True)

    return sv_data


def open_switches_in_network(network_pre_instance: pypowsybl.network.Network, switches_dataframe: pd.DataFrame):
    """
    Opens switches in loaded network given by dataframe (uses ID for merging)
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :param switches_dataframe: dataframe
    """
    logger.info(f"Opening {len(switches_dataframe.index)} switches")
    switches = network_pre_instance.get_switches(all_attributes=True).reset_index()
    switches = switches.merge(switches_dataframe[['ID']].rename(columns={'ID': 'id'}), on='id')
    non_retained_closed = switches.merge(switches_dataframe.rename(columns={'ID': 'id'}), on='id')[['id', 'open']]
    non_retained_closed['open'] = True
    network_pre_instance.update_switches(non_retained_closed.set_index('id'))
    return network_pre_instance


def check_and_fix_dependencies(cgm_sv_data, cgm_ssh_data, original_data):
    """
    Seems that pypowsybl ver 1.6.0 managed to get rid of dependencies in exported file. This gathers them from
    SSH profiles and from the original models
    :param cgm_sv_data: merged SV profile that is missing the dependencies
    :param cgm_ssh_data: merged SSH profiles, will be used to get SSH dependencies
    :param original_data: original models, will be used to get TP dependencies
    :return updated merged SV profile
    """

    # some_data = load_opdm_objects_to_triplets(opdm_objects=original_data)
    some_data = get_opdm_data_from_models(model_data=original_data)
    tp_file_ids = some_data[(some_data['KEY'] == 'Model.profile') & (some_data['VALUE'].str.contains('Topology'))]

    ssh_file_ids = cgm_ssh_data[(cgm_ssh_data['KEY'] == 'Model.profile') &
                                (cgm_ssh_data['VALUE'].str.contains('SteadyStateHypothesis'))]
    dependencies = pd.concat([tp_file_ids, ssh_file_ids], ignore_index=True, sort=False)
    existing_dependencies = cgm_sv_data[cgm_sv_data['KEY'] == 'Model.DependentOn']
    dependency_difference = existing_dependencies.merge(dependencies[['ID']].rename(columns={'ID': 'VALUE'}),
                                                        on='VALUE', how='outer', indicator=True)
    if not dependency_difference.query('_merge == "right_only"').empty:
        cgm_sv_data = triplets.rdf_parser.remove_triplet_from_triplet(cgm_sv_data, existing_dependencies)
        full_model_id = cgm_sv_data[(cgm_sv_data['KEY'] == 'Type') & (cgm_sv_data['VALUE'] == 'FullModel')]
        dependencies_to_update = dependency_difference.query('_merge != "left_only"')
        logger.info(
            f"Mismatch of dependencies. Inserting {len(dependencies_to_update.index)} dependencies to SV profile")
        new_dependencies = dependencies_to_update[['VALUE']].copy().reset_index(drop=True)
        new_dependencies.loc[:, 'KEY'] = 'Model.DependentOn'
        new_dependencies.loc[:, 'ID'] = full_model_id['ID'].iloc[0]
        new_dependencies.loc[:, 'INSTANCE_ID'] = full_model_id['INSTANCE_ID'].iloc[0]
        cgm_sv_data = triplets.rdf_parser.update_triplet_from_triplet(cgm_sv_data, new_dependencies)
    return cgm_sv_data


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


def get_boundary_nodes_between_igms(model_data: list | pd.DataFrame):
    """
    Filters out nodes that are between the igms (mentioned at least 2 igms)
    :param model_data: input models
    : return series of node ids
    """
    model_data = get_opdm_data_from_models(model_data=model_data)
    all_boundary_nodes = model_data[(model_data['KEY'] == 'TopologicalNode.boundaryPoint') &
                                    (model_data['VALUE'] == 'true')]
    # Get boundary nodes that exist in igms
    merged = pd.merge(all_boundary_nodes,
                      model_data[(model_data['KEY'] == 'SvVoltage.TopologicalNode')],
                      left_on='ID', right_on='VALUE', suffixes=('_y', ''))
    # Get duplicates (all of them) then duplicated values. keep=False marks all duplicates True, 'first' marks first
    # occurrence to false, 'last' marks last occurrence to false. If any of them is used then in case duplicates are 2
    # then 1 is retrieved, if duplicates >3 then duplicates-1 retrieved. So, get all the duplicates and as a second
    # step, drop the duplicates
    merged = (merged[merged.duplicated(['VALUE'], keep=False)]).drop_duplicates(subset=['VALUE'])
    in_several_igms = (merged["VALUE"]).to_frame().rename(columns={'VALUE': 'ID'})
    return in_several_igms


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
    sv_voltage_ids = pd.merge(cgm_sv_data[cgm_sv_data['KEY'] == 'SvVoltage.TopologicalNode'],
                              in_several_igms.rename(columns={'ID': 'VALUE'}), on='VALUE')
    # Get SvVoltage voltage values for corresponding SvVoltage Ids
    sv_voltage_values = pd.merge(cgm_sv_data[cgm_sv_data['KEY'] == 'SvVoltage.v'][['ID', 'VALUE']].
                                 rename(columns={'VALUE': 'SvVoltage.v'}),
                                 sv_voltage_ids[['ID', 'VALUE']].
                                 rename(columns={'VALUE': 'SvVoltage.SvTopologicalNode'}), on='ID')
    # Just in case convert the values to numeric
    sv_voltage_values[['SvVoltage.v']] = (sv_voltage_values[['SvVoltage.v']].apply(lambda x: x.apply(Decimal)))
    # Group by topological node id and by some logic take SvVoltage that will be dropped
    voltages_to_keep = (sv_voltage_values.groupby(['SvVoltage.SvTopologicalNode']).
                        apply(lambda x: take_best_match_for_sv_voltage(input_data=x,
                                                                       column_name='SvVoltage.v',
                                                                       to_keep=True), include_groups=False))
    voltages_to_discard = sv_voltage_values.merge(voltages_to_keep['ID'], on='ID', how='left', indicator=True)
    voltages_to_discard = voltages_to_discard[voltages_to_discard['_merge'] == 'left_only']
    if not voltages_to_discard.empty:
        logger.info(f"Removing {len(voltages_to_discard.index)} duplicate voltage levels from boundary nodes")
        sv_voltages_to_remove = pd.merge(cgm_sv_data, voltages_to_discard['ID'].to_frame(), on='ID')
        cgm_sv_data = triplets.rdf_parser.remove_triplet_from_triplet(cgm_sv_data, sv_voltages_to_remove)

    return cgm_sv_data


def set_paired_boundary_injections_to_zero(original_models, cgm_ssh_data):
    """Where there are paired boundary points, equivalent injections need to be modified
    Set P and Q to 0 - so that no additional consumption or production is on tie line
    Set voltage control off - so that no additional consumption or production is on tie line
    Set terminal to connected - to be sure we have paired connected injections at boundary point
    In some models terminals are missing references to ConnectivityNodes
    """

    topological_boundary_points = original_models.query("KEY == 'TopologicalNode.boundaryPoint' and VALUE == 'true'")[
        ["ID"]]
    try:
        terminals = original_models.type_tableview("Terminal").reset_index()[['ID',
                                                                              'Terminal.ConductingEquipment',
                                                                              'Terminal.ConnectivityNode',
                                                                              'Terminal.TopologicalNode']]
    except KeyError:
        terminals = original_models.type_tableview("Terminal").reset_index()[['ID',
                                                                              'Terminal.ConductingEquipment',
                                                                              'Terminal.TopologicalNode']]
    injections = cgm_ssh_data.type_tableview('EquivalentInjection').reset_index()[['ID',
                                                                                   # 'EquivalentInjection.p',
                                                                                   # 'EquivalentInjection.q',
                                                                                   # 'EquivalentInjection.regulationStatus'
                                                                                   ]]
    topological_boundary_points = topological_boundary_points.merge(terminals,
                                                                    left_on="ID",
                                                                    right_on="Terminal.TopologicalNode",
                                                                    suffixes=('_TopologicalNode', '_Terminal'))
    topological_injections = injections.merge(topological_boundary_points,
                                              left_on="ID",
                                              right_on='Terminal.ConductingEquipment',
                                              suffixes=('_ConnectivityNode', ''))
    paired_injections = (topological_injections.groupby("Terminal.TopologicalNode")
                         .filter(lambda x: len(x) == 2))

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
    return cgm_ssh_data.update_triplet_from_triplet(
        pd.concat([updated_terminal_status, updated_regulation_status, updated_p_value, updated_q_value],
                 ignore_index=True), add=False)


def check_energized_boundary_nodes(cgm_sv_data, cgm_ssh_data, original_models, fix_errors: bool = False):
    """
    On one case (1D RTEFrance alone on 01.08.2024 12.30Z) pypowsybl calculates the loadflow and updates
    the voltages on boundaries, however the powerflows are still copied over from the original files.
    This, therefore, joins a lot of tables and, if voltage at some boundary node is zero and the
    equivalent injection is not, sets the injection to zero.
    """
    original_models = get_opdm_data_from_models(model_data=original_models)
    boundary_nodes = original_models.query('KEY == "TopologicalNode.boundaryPoint" & VALUE == "true"')[['ID']]
    if boundary_nodes.empty:
        return cgm_ssh_data
    all_terminals = original_models.type_tableview('Terminal')
    if all_terminals is None:
        return cgm_ssh_data
    terminals = (all_terminals.rename_axis('Terminal').reset_index()
                 .merge(boundary_nodes.rename(columns={'ID': 'Terminal.TopologicalNode'}),
                        on='Terminal.TopologicalNode'))[['Terminal', 'ACDCTerminal.connected',
                                                         'Terminal.ConductingEquipment', 'Terminal.TopologicalNode']]
    new_voltages = (cgm_sv_data.type_tableview('SvVoltage').rename_axis('SvVoltage').reset_index()
                    .merge(boundary_nodes.rename(columns={'ID': 'SvVoltage.TopologicalNode'}),
                           on='SvVoltage.TopologicalNode')).sort_values(by=['SvVoltage'])
    old_voltages = (original_models.type_tableview('SvVoltage').rename_axis('SvVoltage').reset_index()
                    .merge(boundary_nodes.rename(columns={'ID': 'SvVoltage.TopologicalNode'}),
                           on='SvVoltage.TopologicalNode')).sort_values(by=['SvVoltage.TopologicalNode'])
    voltage_diff = ((old_voltages[['SvVoltage.TopologicalNode', 'SvVoltage.v', 'SvVoltage.angle']]
                     .merge(new_voltages[['SvVoltage.TopologicalNode', 'SvVoltage.v', 'SvVoltage.angle']],
                            on='SvVoltage.TopologicalNode', suffixes=('_old', '_new')))
                    .sort_values(by=['SvVoltage.TopologicalNode']))
    old_powerflows = ((original_models.type_tableview('SvPowerFlow').rename_axis('SvPowerFlow').reset_index()
                       .merge(terminals.rename(columns={'Terminal': 'SvPowerFlow.Terminal'}),
                              on='SvPowerFlow.Terminal'))
                      .sort_values(by=['Terminal.TopologicalNode']))
    new_powerflows = ((cgm_sv_data.type_tableview('SvPowerFlow').rename_axis('SvPowerFlow').reset_index()
                       .merge(terminals.rename(columns={'Terminal': 'SvPowerFlow.Terminal'}),
                              on='SvPowerFlow.Terminal'))
                      .sort_values(by=['Terminal.TopologicalNode']))
    powerflow_diff = ((old_powerflows[['SvPowerFlow.Terminal', 'SvPowerFlow.p', 'SvPowerFlow.q']]
                       .merge(new_powerflows[['SvPowerFlow.Terminal', 'SvPowerFlow.p', 'SvPowerFlow.q', 'SvPowerFlow',
                                              'Terminal.ConductingEquipment', 'Terminal.TopologicalNode']],
                              on='SvPowerFlow.Terminal', suffixes=('_old', '_new')))
                      .sort_values(by=['Terminal.TopologicalNode']))
    old_injections = ((original_models.type_tableview('EquivalentInjection')
                       .rename_axis('EquivalentInjection').reset_index())
                      .merge(terminals.rename(columns={'Terminal.ConductingEquipment': 'EquivalentInjection'}),
                             on='EquivalentInjection')).sort_values(by=['Terminal.TopologicalNode'])
    new_injections = ((cgm_ssh_data.type_tableview('EquivalentInjection')
                       .rename_axis('EquivalentInjection').reset_index())
                      .merge(terminals.rename(columns={'Terminal.ConductingEquipment': 'EquivalentInjection'}),
                             on='EquivalentInjection')).sort_values(by=['Terminal.TopologicalNode'])
    injection_diff = ((old_injections[['EquivalentInjection', 'EquivalentInjection.p', 'EquivalentInjection.q']]
                       .merge(new_injections[['EquivalentInjection', 'EquivalentInjection.p', 'EquivalentInjection.q']],
                              on='EquivalentInjection', suffixes=('_old', '_new')))
                      .sort_values(by=['EquivalentInjection']))
    all_together = (powerflow_diff.rename(columns={'SvPowerFlow.Terminal': 'Terminal',
                                                   'Terminal.ConductingEquipment': 'EquivalentInjection',
                                                   'Terminal.TopologicalNode': 'TopologicalNode'})
                    .merge(injection_diff, on='EquivalentInjection', how='left'))
    all_together = all_together.merge(voltage_diff.rename(columns={'SvVoltage.TopologicalNode': 'TopologicalNode'}),
                                      on='TopologicalNode', how='left').sort_values(by=['TopologicalNode'])
    if all_together.empty:
        return cgm_ssh_data
    # Voltage may be set to zero at some boundary nodes while powerflow and equivalent injection are not.
    zero_voltages = all_together[(all_together["SvVoltage.v_new"] == 0) & (all_together["SvVoltage.angle_new"] == 0)]
    if zero_voltages.empty:
        return cgm_ssh_data
    zero_voltages = zero_voltages.copy()
    zero_voltages['Summed_flow'] = (zero_voltages[['EquivalentInjection.p_new', 'EquivalentInjection.q_new']]
                                    .astype(float).abs().sum(axis=1, skipna=True))
    not_zero_flows = zero_voltages[zero_voltages['Summed_flow'] != 0]
    if not_zero_flows.empty:
        return cgm_ssh_data
    logger.warning(f"{len(not_zero_flows.index)} cases where boundary voltage is zero but injection is not")
    if fix_errors:
        logger.info(f"Setting injection at boundary to zero")
        updated_injections = (not_zero_flows.copy(deep=True)[['EquivalentInjection']]
                              .rename(columns={'EquivalentInjection': 'ID'}))
        updated_p_value = updated_injections[["ID"]].copy()
        updated_p_value["KEY"] = "EquivalentInjection.p"
        updated_p_value["VALUE"] = 0
        updated_q_value = updated_injections[["ID"]].copy()
        updated_q_value["KEY"] = "EquivalentInjection.q"
        updated_q_value["VALUE"] = 0
        cgm_ssh_data = cgm_ssh_data.update_triplet_from_triplet(
            pd.concat([updated_p_value, updated_q_value], ignore_index=True), add=False)
    return cgm_ssh_data


def check_for_disconnected_terminals(cgm_sv_data, original_models, fix_errors: bool = False):
    """
    Checks if disconnected terminals have powerflow different from 0
    :param cgm_sv_data: merged sv profile
    :param original_models: original profiles
    :param fix_errors: sets flows to zero
    :return (updated) sv profile
    """
    all_terminals = original_models.type_tableview('Terminal')
    power_flows_post = cgm_sv_data.type_tableview('SvPowerFlow')
    if all_terminals is None or power_flows_post is None:
        return cgm_sv_data
    all_terminals = all_terminals.rename_axis('SvPowerFlow.Terminal').reset_index()
    disconnected_terminals = all_terminals[all_terminals['ACDCTerminal.connected'] == 'false']
    if disconnected_terminals.empty:
        return cgm_sv_data
    power_flows_post = power_flows_post.reset_index()
    disconnected_powerflows = power_flows_post.merge(disconnected_terminals[['SvPowerFlow.Terminal']],
                                                     on='SvPowerFlow.Terminal')
    flows_on_powerflows = disconnected_powerflows[(abs(disconnected_powerflows['SvPowerFlow.p'].astype('float')) > 0) |
                                                  (abs(disconnected_powerflows['SvPowerFlow.q'].astype('float')) > 0)]
    if not flows_on_powerflows.empty:
        logger.warning(f"Found {len(flows_on_powerflows.index)} disconnected terminals which have flows set")
        if fix_errors:
            logger.info(f"Setting flows on disconnected terminals to zero")
            flows_on_powerflows.loc[:, 'SvPowerFlow.p'] = 0
            flows_on_powerflows.loc[:, 'SvPowerFlow.q'] = 0
            cgm_sv_data = triplets.rdf_parser.update_triplet_from_tableview(cgm_sv_data,
                                                                            flows_on_powerflows,
                                                                            add=False,
                                                                            update=True)
    return cgm_sv_data


def check_non_regulating_rotating_machine_q(cgm_ssh_data, original_models, fix_errors: bool = False):
    """
    QoCDC section 5.10 (Table 5): cim:RotatingMachine.q may only differ from the IGM SSH value if the
    machine's own regulating control is enabled (RegulatingCondEq.controlEnabled and the referenced
    RegulatingControl.enabled both true). Restores the IGM SSH value for machines without an eligible
    control, rather than leaving whatever the loadflow solved.
    :param cgm_ssh_data: merged ssh profile
    :param original_models: original profiles, used for eligibility flags and the original SSH value
    :param fix_errors: restores the ineligible values
    :return (updated) ssh profile
    """
    original_q = original_models.query("KEY == 'RotatingMachine.q'")[['ID', 'VALUE']]
    if original_q.empty:
        return cgm_ssh_data

    enabled_controls = original_models.query("KEY == 'RegulatingControl.enabled' and VALUE == 'true'")[['ID']]
    machine_control_enabled = original_models.query(
        "KEY == 'RegulatingCondEq.controlEnabled' and VALUE == 'true'")[['ID']]
    machine_regulating_control = original_models.query("KEY == 'RegulatingCondEq.RegulatingControl'")[
        ['ID', 'VALUE']]
    eligible_machines = machine_regulating_control.merge(machine_control_enabled, on='ID').merge(
        enabled_controls.rename(columns={'ID': 'VALUE'}), on='VALUE')

    ineligible_q = original_q[~original_q['ID'].isin(eligible_machines['ID'])].copy()
    if not ineligible_q.empty:
        logger.info(f"Found {len(ineligible_q.index)} RotatingMachine(s) without an eligible regulating "
                    f"control - restoring their SSH q to the original IGM values")
        if fix_errors:
            ineligible_q.loc[:, 'KEY'] = 'RotatingMachine.q'
            cgm_ssh_data = cgm_ssh_data.update_triplet_from_triplet(ineligible_q[['ID', 'KEY', 'VALUE']],
                                                                    add=False)
    return cgm_ssh_data


def check_rotating_machine_q_outside_p_limits(cgm_ssh_data, original_models, fix_errors: bool = False):
    """
    QoCDC section 5.10 (Table 5): cim:RotatingMachine.q may only differ from the IGM SSH value if
    Pmin <= Pgen <= Pmax, where Pgen = -RotatingMachine.p from the IGM SSH. Pmin/Pmax come from the
    machine's ReactiveCapabilityCurve when it has one (curve takes precedence per Table 5), otherwise
    from its GeneratingUnit.minOperatingP/maxOperatingP. Restores the IGM SSH value for machines whose
    Pgen falls outside that range, regardless of their regulating-control state.
    Machines with neither a curve nor GeneratingUnit limits are left alone.
    :param cgm_ssh_data: merged ssh profile
    :param original_models: original profiles, used for P/limit data and the original SSH value
    :param fix_errors: restores the ineligible values
    :return (updated) ssh profile
    """
    original_q = original_models.query("KEY == 'RotatingMachine.q'")[['ID', 'VALUE']]
    original_p = original_models.query("KEY == 'RotatingMachine.p'")[['ID', 'VALUE']].rename(columns={'VALUE': 'p'})
    if original_q.empty or original_p.empty:
        return cgm_ssh_data
    pgen = original_p.copy()
    pgen['pgen'] = -pd.to_numeric(pgen['p'], errors='coerce')

    # Curve-derived limits (precedence): the curve's own P range across its CurveData points
    machine_curve = original_models.query("KEY == 'SynchronousMachine.InitialReactiveCapabilityCurve'")[
        ['ID', 'VALUE']].rename(columns={'VALUE': 'Curve'})
    curve_points = original_models.query("KEY == 'CurveData.xvalue'")[['ID', 'VALUE']].rename(
        columns={'VALUE': 'xvalue'})
    curve_owner = original_models.query("KEY == 'CurveData.Curve'")[['ID', 'VALUE']].rename(
        columns={'VALUE': 'Curve'})
    curve_limits = curve_points.merge(curve_owner, on='ID')
    curve_limits['xvalue'] = pd.to_numeric(curve_limits['xvalue'], errors='coerce')
    curve_limits = curve_limits.groupby('Curve')['xvalue'].agg(
        curve_p_min='min', curve_p_max='max').reset_index()
    machine_curve_limits = machine_curve.merge(curve_limits, on='Curve')

    # Fallback when no curve is present: GeneratingUnit.minOperatingP/maxOperatingP
    machine_unit = original_models.query("KEY == 'RotatingMachine.GeneratingUnit'")[['ID', 'VALUE']].rename(
        columns={'VALUE': 'GeneratingUnit'})
    unit_min = original_models.query("KEY == 'GeneratingUnit.minOperatingP'")[['ID', 'VALUE']].rename(
        columns={'ID': 'GeneratingUnit', 'VALUE': 'unit_p_min'})
    unit_max = original_models.query("KEY == 'GeneratingUnit.maxOperatingP'")[['ID', 'VALUE']].rename(
        columns={'ID': 'GeneratingUnit', 'VALUE': 'unit_p_max'})
    unit_min['unit_p_min'] = pd.to_numeric(unit_min['unit_p_min'], errors='coerce')
    unit_max['unit_p_max'] = pd.to_numeric(unit_max['unit_p_max'], errors='coerce')
    machine_unit_limits = machine_unit.merge(unit_min, on='GeneratingUnit').merge(unit_max, on='GeneratingUnit')

    limits = pgen.merge(machine_curve_limits[['ID', 'curve_p_min', 'curve_p_max']], on='ID', how='left')
    limits = limits.merge(machine_unit_limits[['ID', 'unit_p_min', 'unit_p_max']], on='ID', how='left')
    limits['p_min'] = limits['curve_p_min'].combine_first(limits['unit_p_min'])
    limits['p_max'] = limits['curve_p_max'].combine_first(limits['unit_p_max'])
    limits = limits.dropna(subset=['pgen', 'p_min', 'p_max'])

    outside_limits = limits[(limits['pgen'] < limits['p_min']) | (limits['pgen'] > limits['p_max'])]
    ineligible_q = original_q[original_q['ID'].isin(outside_limits['ID'])].copy()
    if not ineligible_q.empty:
        logger.info(f"Found {len(ineligible_q.index)} RotatingMachine(s) with Pgen outside [Pmin, Pmax] - "
                    f"restoring their SSH q to the original IGM values")
        if fix_errors:
            ineligible_q.loc[:, 'KEY'] = 'RotatingMachine.q'
            cgm_ssh_data = cgm_ssh_data.update_triplet_from_triplet(ineligible_q[['ID', 'KEY', 'VALUE']],
                                                                    add=False)
    return cgm_ssh_data


def check_non_ltc_tap_changer_step(cgm_ssh_data, cgm_sv_data, original_models, fix_errors: bool = False):
    """
    QoCDC section 5.10 (Table 5): cim:TapChanger.step may only differ from the IGM SSH value if the tap
    changer is an LTC with its control enabled (ltcFlag, TapChanger.controlEnabled, and the referenced
    RegulatingControl.enabled all true). For ineligible tap changers, restores the IGM step to BOTH
    TapChanger.step (SSH) and SvTapStep.position (SV) together.
    :param cgm_ssh_data: merged ssh profile
    :param cgm_sv_data: merged sv profile
    :param original_models: original profiles, used for eligibility flags and the original SSH value
    :param fix_errors: restores the ineligible values
    :return (updated ssh profile, updated sv profile)
    """
    original_step = original_models.query("KEY == 'TapChanger.step'")[['ID', 'VALUE']]
    if original_step.empty:
        return cgm_ssh_data, cgm_sv_data

    enabled_controls = original_models.query("KEY == 'RegulatingControl.enabled' and VALUE == 'true'")[['ID']]
    tap_ltc = original_models.query("KEY == 'TapChanger.ltcFlag' and VALUE == 'true'")[['ID']]
    tap_control_enabled = original_models.query("KEY == 'TapChanger.controlEnabled' and VALUE == 'true'")[['ID']]
    tap_regulating_control = original_models.query("KEY == 'TapChanger.TapChangerControl'")[['ID', 'VALUE']]
    eligible_taps = tap_regulating_control.merge(tap_ltc, on='ID').merge(tap_control_enabled, on='ID').merge(
        enabled_controls.rename(columns={'ID': 'VALUE'}), on='VALUE')

    ineligible_step = original_step[~original_step['ID'].isin(eligible_taps['ID'])].copy()
    if not ineligible_step.empty:
        logger.info(f"Found {len(ineligible_step.index)} TapChanger(s) without an eligible LTC control - "
                    f"restoring their SSH step and SV position to the IGM values")
        if fix_errors:
            ssh_update = ineligible_step.copy()
            ssh_update.loc[:, 'KEY'] = 'TapChanger.step'
            cgm_ssh_data = cgm_ssh_data.update_triplet_from_triplet(ssh_update[['ID', 'KEY', 'VALUE']], add=False)

            sv_tap_steps = cgm_sv_data.query("KEY == 'SvTapStep.TapChanger'")[['ID', 'VALUE']].rename(
                columns={'ID': 'SvTapStep_ID', 'VALUE': 'ID'})
            sv_update = ineligible_step.merge(sv_tap_steps, on='ID')[['SvTapStep_ID', 'VALUE']].rename(
                columns={'SvTapStep_ID': 'ID'})
            if not sv_update.empty:
                sv_update.loc[:, 'KEY'] = 'SvTapStep.position'
                cgm_sv_data = cgm_sv_data.update_triplet_from_triplet(sv_update[['ID', 'KEY', 'VALUE']],
                                                                      add=False)
    return cgm_ssh_data, cgm_sv_data


def check_net_interchanges(cgm_sv_data, cgm_ssh_data, original_models):
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
        logger.error(f"Was not able to get tie flows from original models with exception: {error}")
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

    net_interchange_errors = tie_flows_grouped.loc[
        tie_flows_grouped['ControlArea.netInterchange'].ne(tie_flows_grouped['SvPowerFlow.p_post'])]

    if not net_interchange_errors.empty:
        # Apply modification
        logger.info(f"Updating {len(net_interchange_errors.index)} interchanges to new values")
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
    return check_injection_type_vs_powerflow(cgm_sv_data=cgm_sv_data,
                                             cgm_ssh_data=cgm_ssh_data,
                                             original_models=original_models,
                                             injection_name='EquivalentInjection',
                                             fields_to_check={'SvPowerFlow.p': 'EquivalentInjection.p'},
                                             threshold=threshold,
                                             terminals=terminals,
                                             fix_errors=fix_errors)


def check_injection_type_vs_powerflow(cgm_sv_data,
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
                              additional_processing: bool,
                              ):
    # Load original input models to triplets
    input_models_triplets = load_opdm_objects_to_triplets(opdm_objects=input_models)

    # Apply corrections to SV profile
    sv_data = merge_functions.update_merged_model_sv(sv_data=exported_model, opdm_object_meta=opdm_object_meta)

    # Create update SSH
    sv_data, ssh_data, opdm_object_meta = merge_functions.create_updated_ssh(models_as_triplets=input_models_triplets,
                                                             input_models=input_models,
                                                             sv_data=sv_data,
                                                             opdm_object_meta=opdm_object_meta)
    # --- SV cleanup: remove invalid/redundant entries ---
    sv_data = remove_equivalent_shunt_section(sv_data, input_models_triplets)
    sv_data = remove_small_islands(sv_data, int(SMALL_ISLAND_SIZE))
    sv_data = remove_duplicate_sv_voltages(cgm_sv_data=sv_data, original_data=input_models_triplets)

    # --- SV cleanup: fill in missing entries ---
    sv_data = add_missing_sv_tap_steps(sv_data, ssh_data)

    # --- SV metadata fix ---
    sv_data = check_and_fix_dependencies(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data,
                                         original_data=input_models_triplets)

    # --- SSH consistency fix ---
    # TODO following SSH profile fix should be removed once pypowsybl SSH export will be used
    ssh_data = set_paired_boundary_injections_to_zero(original_models=input_models_triplets,
                                                       cgm_ssh_data=ssh_data)

    if additional_processing:
        sv_data = check_for_disconnected_terminals(cgm_sv_data=sv_data,
                                                    original_models=input_models_triplets,
                                                    fix_errors=True)
        ssh_data = check_energized_boundary_nodes(cgm_sv_data=sv_data,
                                                   cgm_ssh_data=ssh_data,
                                                   original_models=input_models_triplets,
                                                   fix_errors=True)
        ssh_data = check_non_regulating_rotating_machine_q(cgm_ssh_data=ssh_data,
                                                            original_models=input_models_triplets,
                                                            fix_errors=True)
        ssh_data = check_rotating_machine_q_outside_p_limits(cgm_ssh_data=ssh_data,
                                                              original_models=input_models_triplets,
                                                              fix_errors=True)
        ssh_data, sv_data = check_non_ltc_tap_changer_step(cgm_ssh_data=ssh_data,
                                                            cgm_sv_data=sv_data,
                                                            original_models=input_models_triplets,
                                                            fix_errors=True)

    # Run injections check and apply modification if defined in configuration
    injection_threshold = float(INJECTION_THRESHOLD)
    fix_injection_errors = json.loads(str(FIX_INJECTION_ERRORS).lower())

    injection_types_to_check = {
        'EnergySource': 'EnergySource.activePower',
        'ExternalNetworkInjection': 'ExternalNetworkInjection.p',
    }
    for injection_name, injection_field in injection_types_to_check.items():
        ssh_data = check_injection_type_vs_powerflow(cgm_ssh_data=ssh_data,
                                                      cgm_sv_data=sv_data,
                                                      original_models=input_models_triplets,
                                                      injection_name=injection_name,
                                                      fields_to_check={'SvPowerFlow.p': injection_field},
                                                      threshold=injection_threshold,
                                                      fix_errors=fix_injection_errors)
    ssh_data = check_non_boundary_equivalent_injections(cgm_sv_data=sv_data,
                                                        cgm_ssh_data=ssh_data,
                                                        original_models=input_models_triplets,
                                                        threshold=injection_threshold,
                                                        fix_errors=fix_injection_errors)

    try:
        ssh_data = check_net_interchanges(cgm_sv_data=sv_data,
                                          cgm_ssh_data=ssh_data,
                                          original_models=input_models_triplets)
    except KeyError:
        logger.warning(f"No fields for net interchange correction")

    return sv_data, ssh_data, opdm_object_meta
