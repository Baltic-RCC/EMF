import pypowsybl
import triplets
import pandas as pd
import logging
import uuid
from decimal import Decimal
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets

logger = logging.getLogger(__name__)


def remove_small_islands(solved_data, island_size_limit):
    # TODO - EVALUATE LEGACY
    small_island = pd.DataFrame(solved_data.query("KEY == 'TopologicalIsland.TopologicalNodes'").ID.value_counts()).reset_index().query("count <= @island_size_limit")
    solved_data = triplets.rdf_parser.remove_triplet_from_triplet(solved_data, small_island, columns=["ID"])
    logger.info(f"Removed {len(small_island)} island(s) with size <= {island_size_limit}")
    return solved_data


def remove_equivalent_shunt_section(sv_data: pd.DataFrame, models_as_triplets: pd.DataFrame):
    # TODO - EVALUATE LEGACY
    """Remove Shunt Sections for EQV Shunts from SV profile"""

    equiv_shunt = models_as_triplets.query("KEY == 'Type' and VALUE == 'EquivalentShunt'")
    if len(equiv_shunt) > 0:
        shunts_to_remove = sv_data.merge(
            sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").merge(equiv_shunt.ID, left_on='VALUE',
                                                                                        right_on="ID", how='inner',
                                                                                        suffixes=('', '_EQVShunt')).ID)
        if len(shunts_to_remove) > 0:
            logger.warning(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
            sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

    return sv_data


def add_missing_sv_tap_steps(sv_data: pd.DataFrame, ssh_data: pd.DataFrame):
    # TODO - EVALUATE LEGACY
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
    for tap_changer in missing_sv_tap_steps.itertuples():
        ID = str(uuid.uuid4())
        logger.warning(f'Missing SvTapStep for {tap_changer.ID}, adding SvTapStep {ID} and taking tap value {tap_changer.VALUE} from SSH')
        tap_steps_to_be_added.extend([
            (ID, 'Type', 'SvTapStep', SV_INSTANCE_ID),
            (ID, 'SvTapStep.TapChanger', tap_changer.ID, SV_INSTANCE_ID),
            (ID, 'SvTapStep.position', tap_changer.VALUE, SV_INSTANCE_ID),
        ])

    sv_data = pd.concat([sv_data, pd.DataFrame(tap_steps_to_be_added, columns=['ID', 'KEY', 'VALUE', 'INSTANCE_ID'])], ignore_index=True)

    return sv_data


def open_switches_in_network(network_pre_instance: pypowsybl.network.Network, switches_dataframe: pd.DataFrame):
    # TODO - EVALUATE LEGACY
    """
    Opens switches in loaded network given by dataframe (uses ID for merging)
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :param switches_dataframe: dataframe
    """
    logger.info(f"Opening {len(switches_dataframe.index)} switches")
    switches = network_pre_instance.get_switches(all_attributes=True).reset_index()
    switches = switches.merge(switches_dataframe[['ID']].rename(columns={'ID': 'id'}), on='id')
    non_retained_closed = switches.merge(switches_dataframe.rename(columns={'ID': 'id'}),
                                         on='id')[['id', 'open']]
    non_retained_closed['open'] = True
    network_pre_instance.update_switches(non_retained_closed.set_index('id'))
    return network_pre_instance


def check_and_fix_dependencies(cgm_sv_data, cgm_ssh_data, original_data):
    # TODO - EVALUATE LEGACY
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
        logger.warning(f"Mismatch of dependencies. Inserting {len(dependencies_to_update.index)} dependencies to SV profile")
        new_dependencies = dependencies_to_update[['VALUE']].copy().reset_index(drop=True)
        new_dependencies.loc[:, 'KEY'] = 'Model.DependentOn'
        new_dependencies.loc[:, 'ID'] = full_model_id['ID'].iloc[0]
        new_dependencies.loc[:, 'INSTANCE_ID'] = full_model_id['INSTANCE_ID'].iloc[0]
        cgm_sv_data = triplets.rdf_parser.update_triplet_from_triplet(cgm_sv_data, new_dependencies)
    return cgm_sv_data


def handle_igm_ssh_vs_cgm_ssh_error(network_pre_instance: pypowsybl.network.Network):
    # TODO - EVALUATE LEGACY
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
        generators_mask = (all_generators['CGMES.synchronousMachineOperatingMode'].str.contains('generator'))
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

            # Solution 1: set max_p from curve max, it should contain p on target-p
            upper_limit_violated = curve_generators[(curve_generators['max_p'] > curve_generators['curve_p_max'])]
            if not upper_limit_violated.empty:
                logger.warning(f"Updating max p from curve for {len(upper_limit_violated.index)} generators")
                upper_limit_violated['max_p'] = upper_limit_violated['curve_p_max']
                network_pre_instance.update_generators(upper_limit_violated[['id', 'max_p']].set_index('id'))

            lower_limit_violated = curve_generators[(curve_generators['min_p'] < curve_generators['curve_p_min'])]
            if not lower_limit_violated.empty:
                logger.warning(f"Updating min p from curve for {len(lower_limit_violated.index)} generators")
                lower_limit_violated['min_p'] = lower_limit_violated['curve_p_min']
                network_pre_instance.update_generators(lower_limit_violated[['id', 'min_p']].set_index('id'))

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
    except Exception as ex:
        logger.warning(f"Unable to pre-process for igm-cgm-ssh error: {ex}")

    return network_pre_instance


def take_best_match_for_sv_voltage(input_data, column_name: str = 'v', to_keep: bool = True):
    # TODO - EVALUATE LEGACY
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


def get_opdm_data_from_models(model_data: list | pd.DataFrame):
    """
    Check if input is already parsed to triplets. Do it otherwise
    :param model_data: input models
    :return triplets
    """
    if not isinstance(model_data, pd.DataFrame):
        model_data = load_opdm_objects_to_triplets(model_data)
    return model_data


def get_boundary_nodes_between_igms(model_data: list | pd.DataFrame):
    # TODO - EVALUATE LEGACY
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
    # TODO - EVALUATE LEGACY
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

    topological_boundary_points = original_models.query("KEY == 'TopologicalNode.boundaryPoint' and VALUE == 'true'")[["ID"]]
    try:
        terminals = original_models.type_tableview("Terminal").reset_index()[['ID',
                                                                              'Terminal.ConductingEquipment',
                                                                              'Terminal.ConnectivityNode',
                                                                              'Terminal.TopologicalNode']]
    except KeyError:
        terminals = original_models.type_tableview("Terminal").reset_index()[['ID',
                                                                              'Terminal.ConductingEquipment',
                                                                              'Terminal.TopologicalNode']]
    injections = original_models.type_tableview('EquivalentInjection').reset_index()[['ID',
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
    paired_topological_injections = (topological_injections.groupby("Terminal.TopologicalNode")
                                     .filter(lambda x: len(x) == 2))
    paired_injections = paired_topological_injections

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
    return cgm_ssh_data.update_triplet_from_triplet(pd.concat([updated_regulation_status, updated_p_value, updated_q_value], ignore_index=True), add=False)
