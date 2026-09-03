import logging
import pandas
import polars as pl
import triplets
import triplets.tools as triplet_tools
import xml.etree.ElementTree as ET
import datetime
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets
from emf.common.helpers.statistics import get_tieflow_data, sum_on_KEY

logger = logging.getLogger(__name__)


def _as_polars(data):
    """
    Normalizes triplet data to a polars DataFrame.

    `load_opdm_objects_to_triplets` / `get_tieflow_data` live in emf.common.helpers, outside
    the triplets package, so there's no control whether it hands back pandas or polars. This
    makes the rest of the pipeline work either way while staying on polars internally,
    (triplets.tools functions dispatch to their native polars_engine implementation when
    given a polars DataFrame).
    """
    if isinstance(data, pl.DataFrame):
        return data
    return pl.from_pandas(data)

def get_nodes_against_kirchhoff_first_law(original_models,
                                          cgm_sv_data=None,
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
    original_models = _as_polars(load_opdm_objects_to_triplets(opdm_objects=original_models))
    if cgm_sv_data is None:
        cgm_sv_data = original_models
    else:
        cgm_sv_data = _as_polars(cgm_sv_data)

    power_flow_view = triplet_tools.type_tableview(cgm_sv_data, 'SvPowerFlow')
    if power_flow_view is None:
        power_flow = pl.DataFrame(
            schema={'SvPowerFlow.Terminal': pl.Utf8, 'SvPowerFlow.p': pl.Float64, 'SvPowerFlow.q': pl.Float64}
        )
    else:
        power_flow = power_flow_view.select(['SvPowerFlow.Terminal', 'SvPowerFlow.p', 'SvPowerFlow.q'])
    power_flow = power_flow.with_columns([
        pl.col('SvPowerFlow.p').cast(pl.Float64, strict=False),
        pl.col('SvPowerFlow.q').cast(pl.Float64, strict=False),
    ])

    sv_injections = None
    if consider_sv_injection:
        try:
            sv_view = triplet_tools.type_tableview(cgm_sv_data, 'SvInjection')
            if sv_view is not None:
                sv_injections = (
                    sv_view.rename({
                        'SvInjection.TopologicalNode': 'Terminal.TopologicalNode',
                        'SvInjection.pInjection': 'SvPowerFlow.p',
                        'SvInjection.qInjection': 'SvPowerFlow.q',
                    })
                    .select(['Terminal.TopologicalNode', 'SvPowerFlow.p', 'SvPowerFlow.q'])
                    .with_columns([
                        pl.col('SvPowerFlow.p').cast(pl.Float64, strict=False),
                        pl.col('SvPowerFlow.q').cast(pl.Float64, strict=False),
                    ])
                )
        except (AttributeError, pl.exceptions.ColumnNotFoundError, pl.exceptions.SchemaError):
            # logger.warning(f"No SvInjections provided")
            pass

    # Get terminals. type_tableview's polars engine returns the object ID as a plain "ID"
    # column (polars has no index), so its just renamed - no reset_index dance needed.
    terminals_view = triplet_tools.type_tableview(original_models, 'Terminal')
    if terminals_view is None:
        return pandas.DataFrame()
    terminals = (
        terminals_view.rename({'ID': 'Terminal'})
        .select(['Terminal', 'Terminal.ConductingEquipment', 'Terminal.TopologicalNode'])
    )

    # Calculate summed flows per topological node (vectorized left-join + group_by/agg,
    # equivalent to pandas.to_numeric(..., errors='coerce').sum() - unparsable values were
    # cast to null above, and sum() skips nulls just like pandas skips NaN)
    flows_summed = (
        power_flow.join(terminals, left_on='SvPowerFlow.Terminal', right_on='Terminal', how='left')
        .group_by('Terminal.TopologicalNode')
        .agg([
            pl.col('SvPowerFlow.p').sum(),
            pl.col('SvPowerFlow.q').sum(),
        ])
    )

    if sv_injections is not None and sv_injections.height > 0:
        flows_summed = (
            pl.concat([flows_summed, sv_injections], how='diagonal')
            .group_by('Terminal.TopologicalNode')
            .agg([
                pl.col('SvPowerFlow.p').sum(),
                pl.col('SvPowerFlow.q').sum(),
            ])
        )

    # Get topological nodes that have mismatch
    nok_nodes = flows_summed.filter(
        (pl.col('SvPowerFlow.p').abs() > sv_injection_limit) |
        (pl.col('SvPowerFlow.q').abs() > sv_injection_limit)
    ).select('Terminal.TopologicalNode')

    if nodes_only:
        return nok_nodes.to_pandas()

    try:
        terminals_nodes = terminals.join(flows_summed, on='Terminal.TopologicalNode', how='left')
        terminals_nodes = terminals_nodes.join(nok_nodes, on='Terminal.TopologicalNode', how='inner')
        return terminals_nodes.to_pandas()
    except (IndexError, pl.exceptions.ColumnNotFoundError, pl.exceptions.SchemaError):
        # Mirrors the original's defensive IndexError catch, plus polars' own exception
        # types for the equivalent failure modes.
        return pandas.DataFrame()

def check_not_retained_switches_between_nodes(original_data, open_not_retained_switches: bool = False):
    """
    For the loadflow open all the non-retained switches that connect different topological nodes
    Currently it is seen to help around 9 to 10 Kirchhoff 1st law errors from 2 TSOs
    :param original_data: original models in triplets format
    :param open_not_retained_switches: if true then found switches are set to open, else it only checks and reports
    :return: updated original data
    """
    violated_switches = 0
    was_dataframe = isinstance(original_data, (pandas.DataFrame, pl.DataFrame))
    if not was_dataframe:
        original_models = _as_polars(load_opdm_objects_to_triplets(opdm_objects=original_data))
    else:
        original_models = _as_polars(original_data)

    not_retained_switches = (
        original_models
        .filter((pl.col('KEY') == 'Switch.retained') & (pl.col('VALUE') == 'false'))
        .select('ID')
    )
    closed_switches = (
        original_models
        .filter((pl.col('KEY') == 'Switch.open') & (pl.col('VALUE') == 'false'))
    )
    not_retained_closed = not_retained_switches.join(closed_switches.select('ID'), on='ID', how='inner')

    terminals_view = triplet_tools.type_tableview(original_models, 'Terminal')
    if terminals_view is None:
        return original_data, violated_switches
    terminals = (
        terminals_view.rename({'ID': 'Terminal'})
        .select(['Terminal', 'Terminal.ConductingEquipment', 'Terminal.TopologicalNode'])
        .rename({'Terminal.ConductingEquipment': 'ID'})
    )

    not_retained_terminals = terminals.join(not_retained_closed, on='ID', how='inner')

    if not_retained_terminals.height == 0:
        return original_data, violated_switches

    # Replaces the pandas groupby().apply(check_switch_terminals) python-level loop with a
    # single vectorized group_by/agg: a switch is violated when its terminals span more than
    # one distinct TopologicalNode.
    between_tn = (
        not_retained_terminals
        .group_by('ID')
        .agg(pl.col('Terminal.TopologicalNode').n_unique().alias('n_unique_tn'))
        .filter(pl.col('n_unique_tn') > 1)
    )

    if between_tn.height > 0:
        violated_switches = between_tn.height
        logger.warning(f"Found {violated_switches} not retained switches between topological nodes")
        if open_not_retained_switches:
            logger.warning(f"Opening not retained switches")
            open_switches = closed_switches.join(between_tn.select('ID'), on='ID', how='inner')
            open_switches = open_switches.with_columns(pl.lit('true').alias('VALUE'))

            # triplets.tools.update_triplets_from_triplets dispatches on the original_data
            # object's own type - if it's pandas, the update_data must be pandas too. This
            # preserves the original function's behavior of updating `original_data` (not the
            # locally-normalized `original_models`) verbatim.
            if isinstance(original_data, pl.DataFrame):
                update_arg = open_switches
            else:
                update_arg = open_switches.to_pandas()
            original_data = triplet_tools.update_triplets_from_triplets(original_data, update_arg)

    return original_data, violated_switches

def get_ac_net_position(models_as_triplets):
    """
    Taken from model_quality/statistics.py. Finds sum of EquivalentInjection on the borders

    :param models_as_triplets: input dataframe of model as triplets
    """
    # Use only Interchange Control Area Tieflows
    tieflow_type = "ControlAreaTypeKind.Interchange"
    tieflow_data = _as_polars(get_tieflow_data(models_as_triplets))

    tieflow_data = tieflow_data.filter(pl.col('ControlArea.type') == tieflow_type)
    # AC was needed?
    if 'BoundaryPoint.isDirectCurrent' in tieflow_data.columns:
        tieflow_data = tieflow_data.filter(pl.col('BoundaryPoint.isDirectCurrent') == False)  # noqa: E712

    data_columns = ["EquivalentInjection.p", "EquivalentInjection.q", "SvPowerFlow.p", "SvPowerFlow.q"]
    if tieflow_data.height == 0:
        tieflow_values = {c: 0.0 for c in data_columns}
    else:
        sums = tieflow_data.select([
            pl.col(c).cast(pl.Float64).sum().alias(c) for c in data_columns
        ])
        tieflow_values = sums.to_dicts()[0]

    return tieflow_values.get("EquivalentInjection.p", None)

def get_sum_of_loads(models_as_triplets, parameter_name: str = 'ConformLoad'):
    """
    Taken from model_quality/statistics.py. Slices the data and takes sum of values

    :param models_as_triplets: input dataframe of model as triplets
    :param parameter_name: VALUE that can be used to slice the input data

    """
    models_pl = _as_polars(models_as_triplets)

    if parameter_name is not None:
        ids_of_type = models_pl.filter(
            (pl.col('KEY') == 'Type') & (pl.col('VALUE') == parameter_name)
        ).select('ID')
        input_data_pl = models_pl.join(ids_of_type, on='ID', how='inner')
    else:
        input_data_pl = models_pl

    # Filter out negative conform loads
    conform_keys = ['EnergyConsumer.p', 'EnergyConsumer.q']
    load_data_pl = input_data_pl.filter(pl.col('KEY').is_in(conform_keys))
    filtered_pl = load_data_pl.filter(pl.col('VALUE').cast(pl.Float64) >= 0)

    # The heavy filtering above (over the full triplet table) is done in polars; the final
    # summation is delegated back to the existing sum_on_KEY helper (from emf.common.helpers),
    # to keep its exact semantics intact. sum_on_KEY presumably expects
    # pandas, so convert only this already-small, already-filtered slice.
    filtered_input_data = filtered_pl.to_pandas()

    output = {
        "EnergyConsumer.p": sum_on_KEY(filtered_input_data, 'EnergyConsumer.p'),
        "EnergyConsumer.q": sum_on_KEY(filtered_input_data, 'EnergyConsumer.q'),
        # "RotatingMachine.p": sum_on_KEY(input_data, 'RotatingMachine.p'),
        # "RotatingMachine.q": sum_on_KEY(input_data, 'RotatingMachine.q')
    }
    return output.get("EnergyConsumer.p", None)

def get_lvl8_report_igm(report: dict):
    # Pure XML construction from a plain dict - no dataframe involved, so left as-is.

    # Create <QAReport> root
    qa_attribs = {
        'created': datetime.datetime.strptime(report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%SZ'),
        'schemeVersion': "2.0",
        'serviceProvider': "BALTICRCC",
        'xmlns': "http://entsoe.eu/checks"
    }
    qa_root = ET.Element("QAReport", attrib=qa_attribs)

    # Add RuleViolations if present
    violations_list = [
        {
            'ruleId': "IGMConvergence",
            'validationLevel': "8",
            'severity': "ERROR",
            'Message': "Power flow could not be calculated for IGM with required settings. Check diagnostic messages."
        },
    ]

    # Later possible to add violation conditions and checks
    violations = list()
    if report["loadflow"]["status_text"] == 'Converged':
        logger.info(f"IGM validation success status included in lvl8 report")
        quality_indicator_igm = "Valid"
    else:
        violations = violations_list
        quality_indicator_igm = "Invalid - inconsistent data"

    # Create <QAReport> <IGM>
    igm = ET.SubElement(qa_root, "IGM", {
        'created': datetime.datetime.strptime(report["@timestamp"], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%SZ'),
        'scenarioTime': datetime.datetime.fromisoformat(report['@scenario_timestamp']).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'tso': report['tso'],
        'version': str(report['@version']),
        'processType': report['@time_horizon'],
        'qualityIndicator': quality_indicator_igm,
    })
    resource_igm = ET.SubElement(igm, "resource")
    resource_igm.text = report['fullModel_ID']

    if violations:
        for v in violations:
            rv = ET.SubElement(igm, "RuleViolation", {
                'ruleId': v['ruleId'],
                'validationLevel': v['validationLevel'],
                'severity': v['severity']
            })
            msg = ET.SubElement(rv, "Message")
            msg.text = v['Message']
    else:
        logger.info(f"No violations present for IGM-level-8 report")

    # Generate final XML
    qa_report_lvl8 = ET.tostring(qa_root, encoding='utf-8', xml_declaration=True)

    return qa_report_lvl8

def modify_region_name_for_denmark(input_data):
    """
    For fixing issues when GeographicalRegion ids do not match
    """
    was_polars = isinstance(input_data, pl.DataFrame)
    input_data_pl = _as_polars(input_data)

    # Get all Geographical regions. type_tableview's polars engine returns the object ID as a
    # plain "ID" column, so this rename replaces the old rename_axis+reset_index combo.
    geo_regions = (
        triplet_tools.type_tableview(input_data_pl, 'GeographicalRegion')
        .rename({'ID': 'SubGeographicalRegion.Region'})
    )
    control_areas = triplet_tools.type_tableview(input_data_pl, 'ControlArea')
    sub_regions = triplet_tools.type_tableview(input_data_pl, 'SubGeographicalRegion')

    # Slice it with control area EIC codes: get region that has to be
    ca_geo_regions = geo_regions.join(
        control_areas.select('IdentifiedObject.energyIdentCodeEic'),
        on='IdentifiedObject.energyIdentCodeEic', how='inner'
    )

    # Cut out the SubGeographical region from boundary just in case
    sub_regions = sub_regions.filter(pl.col('IdentifiedObject.name') != 'ENTSO-E')

    # Cut regions to DK (because some other TSOs like to redeclare the geographical regions)
    geo_regions_dk = geo_regions.filter(pl.col('IdentifiedObject.name').str.contains('DK'))
    sub_regions = sub_regions.join(
        geo_regions_dk.select('SubGeographicalRegion.Region'),
        on='SubGeographicalRegion.Region', how='inner'
    )
    sub_regions_with_eic = sub_regions.join(
        ca_geo_regions.select('SubGeographicalRegion.Region'),
        on='SubGeographicalRegion.Region', how='inner'
    )
    if sub_regions_with_eic.height > 0:
        return input_data

    if sub_regions.height > 0 and ca_geo_regions.height > 0:
        logger.warning(f"Detected {sub_regions.height} sub regions and {ca_geo_regions.height} regions with EIC in IGM")
        sub_regions = sub_regions.drop('SubGeographicalRegion.Region')
        new_region_names = ca_geo_regions.select('SubGeographicalRegion.Region').unique().to_series().to_list()
        if len(new_region_names) > 1:
            logger.warning(f"More than 1 region found, returning")
            return input_data

        sub_regions = sub_regions.with_columns(pl.lit(new_region_names[0]).alias('SubGeographicalRegion.Region'))

        # Same "call update on the original, unnormalized object" pattern as
        # check_not_retained_switches_between_nodes - match the tableview's type to whichever
        # engine `input_data` itself uses.
        tableview_arg = sub_regions if was_polars else sub_regions.to_pandas()
        input_data = triplet_tools.update_triplets_from_tableview(input_data, tableview_arg, update=True)

    return input_data
