import logging
import pandas
import triplets
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets

logger = logging.getLogger(__name__)


def get_nodes_against_kirchhoff_first_law(original_models,
                                          cgm_sv_data: pandas.DataFrame = None,
                                          sv_injection_limit: float = 0.1,
                                          consider_sv_injection: bool = False,
                                          nodes_only: bool = False):
    """
    Gets dataframe of nodes in which the sum of flows exceeds the limit
    :param cgm_sv_data: merged SV profile (needed to set the flows for terminals)
    :param original_models: IGMs (triplets, dictionary)
    :param consider_sv_injection: whether to consider the sv injections
    :param nodes_only: if true then return unique nodes only, if false then nodes with corresponding terminals
    :param sv_injection_limit: threshold for deciding whether the node is violated by sum of flows
    """
    original_models = load_opdm_objects_to_triplets(opdm_objects=original_models)
    sv_injections = pandas.DataFrame()
    if cgm_sv_data is None:
        cgm_sv_data = original_models
    power_flow = cgm_sv_data.type_tableview('SvPowerFlow')[['SvPowerFlow.Terminal', 'SvPowerFlow.p', 'SvPowerFlow.q']]
    if consider_sv_injection:
        try:
            sv_injections = (cgm_sv_data.type_tableview('SvInjection')
                             .rename_axis('SvInjection')
                             .rename(columns={'SvInjection.TopologicalNode': 'Terminal.TopologicalNode',
                                              'SvInjection.pInjection': 'SvPowerFlow.p',
                                              'SvInjection.qInjection': 'SvPowerFlow.q'})
                             .reset_index())[['Terminal.TopologicalNode', 'SvPowerFlow.p', 'SvPowerFlow.q']]
        except AttributeError:
            # logger.warning(f"No SvInjections provided")
            pass
    # Get terminals
    terminals = original_models.type_tableview('Terminal').rename_axis('Terminal').reset_index()
    terminals = terminals[['Terminal', 'Terminal.ConductingEquipment', 'Terminal.TopologicalNode']]
    # Calculate summed flows per topological node
    flows_summed = ((power_flow.merge(terminals, left_on='SvPowerFlow.Terminal', right_on='Terminal', how='left')
                     .groupby('Terminal.TopologicalNode')[['SvPowerFlow.p', 'SvPowerFlow.q']]
                     .agg(lambda x: pandas.to_numeric(x, errors='coerce').sum()))
                    .rename_axis('Terminal.TopologicalNode').reset_index())
    if not sv_injections.empty:
        flows_summed = (pandas.concat([flows_summed, sv_injections]).groupby('Terminal.TopologicalNode').sum()
                        .reset_index())
    # Get topological nodes that have mismatch
    nok_nodes = flows_summed[(abs(flows_summed['SvPowerFlow.p']) > sv_injection_limit) |
                             (abs(flows_summed['SvPowerFlow.q']) > sv_injection_limit)][['Terminal.TopologicalNode']]
    if nodes_only:
        return nok_nodes
    try:
        terminals_nodes = terminals.merge(flows_summed, on='Terminal.TopologicalNode', how='left')
        terminals_nodes = terminals_nodes.merge(nok_nodes, on='Terminal.TopologicalNode')
        return terminals_nodes
    except IndexError:
        return pandas.DataFrame()


def check_switch_terminals(input_data: pandas.DataFrame, column_name: str):
    """
    Checks if column of a dataframe contains only one value
    :param input_data: input data frame
    :param column_name: name of the column to check
    return True if different values are in column, false otherwise
    """
    data_slice = (input_data.reset_index())[column_name]
    return not pandas.Series(data_slice[0] == data_slice).all()


def check_not_retained_switches_between_nodes(original_data, open_not_retained_switches: bool = False):
    """
    For the loadflow open all the non-retained switches that connect different topological nodes
    Currently it is seen to help around 9 to 10 Kirchhoff 1st law errors from 2 TSOs
    :param original_data: original models in triplets format
    :param open_not_retained_switches: if true then found switches are set to open, else it only checks and reports
    :return: updated original data
    """
    violated_switches = 0
    if not isinstance(original_data, pandas.DataFrame):
        original_models = load_opdm_objects_to_triplets(opdm_objects=original_data)
    else:
        original_models = original_data
    not_retained_switches = original_models[(original_models['KEY'] == 'Switch.retained')
                                            & (original_models['VALUE'] == "false")][['ID']]
    closed_switches = original_models[(original_models['KEY'] == 'Switch.open')
                                      & (original_models['VALUE'] == 'false')]
    not_retained_closed = not_retained_switches.merge(closed_switches[['ID']], on='ID')
    terminals = original_models.type_tableview('Terminal').rename_axis('Terminal').reset_index()
    terminals = terminals[['Terminal', 'Terminal.ConductingEquipment', 'Terminal.TopologicalNode']]
    not_retained_terminals = (terminals.rename(columns={'Terminal.ConductingEquipment': 'ID'})
                              .merge(not_retained_closed, on='ID'))
    if not_retained_terminals.empty:
        return original_data, violated_switches

    between_tn = ((not_retained_terminals.groupby('ID')[['Terminal.TopologicalNode']]
                  .apply(lambda x: check_switch_terminals(x, 'Terminal.TopologicalNode')))
                  .reset_index(name='same_TN'))
    between_tn = between_tn[between_tn['same_TN']]
    if not between_tn.empty:
        violated_switches = len(between_tn.index)
        logger.warning(f"Found {len(between_tn.index)} not retained switches between topological nodes")
        if open_not_retained_switches:
            logger.warning(f"Opening not retained switches")
            open_switches = closed_switches.merge(between_tn[['ID']], on='ID')
            open_switches.loc[:, 'VALUE'] = 'true'
            original_data = triplets.rdf_parser.update_triplet_from_triplet(original_data, open_switches)

    return original_data, violated_switches


def very_special_fix_for_dk(triplets_data):
    """
    For fixing issues when GeographicalRegion ids do not match
    """
    # Get all Geographical regions
    geo_regions = (triplets_data.type_tableview('GeographicalRegion').reset_index()
                   .rename(columns={'ID': 'SubGeographicalRegion.Region'}))
    # Slice it with control area EIC codes: get region that has to be
    control_areas = triplets_data.type_tableview('ControlArea').reset_index()
    ca_geo_regions = geo_regions.merge(control_areas[['IdentifiedObject.energyIdentCodeEic']],
                                       on='IdentifiedObject.energyIdentCodeEic')
    sub_regions = triplets_data.type_tableview('SubGeographicalRegion').reset_index()

    # Cut out the SubGeographical region from boundary just in case
    sub_regions = sub_regions[sub_regions['IdentifiedObject.name'] != 'ENTSO-E']
    # Cut regions to DK (because some other TSOs like to redeclare the geographical regions)
    geo_regions = geo_regions[geo_regions['IdentifiedObject.name'].str.contains('DK')]

    sub_regions = sub_regions.merge(geo_regions[['SubGeographicalRegion.Region']],
                                      on='SubGeographicalRegion.Region')
    sub_regions_with_eic = sub_regions.merge(ca_geo_regions[['SubGeographicalRegion.Region']],
                                               on='SubGeographicalRegion.Region')
    if not sub_regions_with_eic.empty:
        return triplets_data
    if not sub_regions.empty and not ca_geo_regions.empty:
        logger.warning(f"Detected {len(sub_regions)} Sub regions and {len(ca_geo_regions)} regions with EIC in igm")
        sub_regions = sub_regions.drop(columns='SubGeographicalRegion.Region')
        new_region_names = ca_geo_regions['SubGeographicalRegion.Region'].unique().tolist()
        if len(new_region_names) > 1:
            logger.warning(f"More than 1 region found, returning")
            return triplets_data
        sub_regions['SubGeographicalRegion.Region'] = new_region_names[0]
        triplets_data = triplets.rdf_parser.update_triplet_from_tableview(triplets_data, sub_regions, update=True)
    return triplets_data
