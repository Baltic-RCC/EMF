import pandas as pd
import numpy as np
from model_statistics import get_tieflow_data, type_tableview_merge
from quality_functions import get_uap_outages_from_scenario_time

def check_generator_quality(report, network):
    # Check Kruonis and Riga TEC generators
    generators = network.type_tableview('SynchronousMachine').rename_axis('Terminal').reset_index()
    kruonis_generators = generators[generators['IdentifiedObject.name'].str.contains('KHAE_G')]
    rtec_generators = generators[generators['IdentifiedObject.name'].str.contains('RTEC')]

    if not kruonis_generators.empty:
        gen_count1 = kruonis_generators[kruonis_generators['RotatingMachine.p'] > 0].shape[0]
        flag1 = gen_count1 < 3
        report.update({"kruonis_generators": gen_count1, "kruonis_check": flag1})
    else:
        report.update({"kruonis_generators": None, "kruonis_check": None})
    if not rtec_generators.empty:
        gen_count2 = rtec_generators[rtec_generators['RotatingMachine.p'] > 0.000001].shape[0]
        flag2 = gen_count2 < 3
        report.update({"rtec_generators": gen_count2, "rtec_check": flag2})
    else:
        report.update({"rtec_generators": None, "rtec_check": None})

    return report


def check_lt_pl_crossborder(report, network, border_limit, tieflow_data=None):
    # Check LT-PL crossborder flow
    try:
        if tieflow_data is None or tieflow_data.empty:
            tieflow_data = get_tieflow_data(network)
        tie_flows = tieflow_data[tieflow_data['cross_border'] == 'LT-PL']
        tie_flows = tie_flows[tie_flows['IdentifiedObject.name_TieFlow'] == 'LIETUVA']
        tie_flow_1 = tie_flows[tie_flows['IdentifiedObject.shortName_EquivalentInjection'] == 'XEL_AL11']
        tie_flow_2 = tie_flows[tie_flows['IdentifiedObject.shortName_EquivalentInjection'] == 'XEL_AL12']
        tie_flow = float((tie_flow_1['SvPowerFlow.p'].iloc[0] + tie_flow_2['SvPowerFlow.p'].iloc[0]) / 2)
        report.update({"lt_pl_flow": tie_flow, "lt_pl_xborder_check": abs(tie_flow) < float(border_limit)})
    except:
        report.update({"lt_pl_flow": None, "lt_pl_xborder_check": None})

    return report

def check_crossborder_inconsistencies(report, network):
    # Check cross-border line inconsistencies
    try:
        connectivity_nodes = type_tableview_merge(network, "ControlArea<-TieFlow->Terminal->ConnectivityNode")
        boundary_nodes = connectivity_nodes[connectivity_nodes['ConnectivityNode.boundaryPoint'] == "true"]

        tso_list = ["Augstsprieguma tikls", 'Litgrid', "Elering", "PSE S.A."]
        ba_boundary_nodes = boundary_nodes[boundary_nodes['ConnectivityNode.fromEndNameTso'].isin(tso_list) &
                                           boundary_nodes['ConnectivityNode.toEndNameTso'].isin(tso_list)]

        line_terminals = ba_boundary_nodes.merge(network.type_tableview("ACLineSegment"),
                                                 left_on="Terminal.ConductingEquipment",
                                                 right_on="ID",
                                                 suffixes=("", "_Line"))

        line_terminals = line_terminals.rename(columns={'ACDCTerminal.connected': 'connected',
                                                        "IdentifiedObject.name_TieFlow": 'country',
                                                        "IdentifiedObject.name_Line": 'name'})

        line_terminals['connected'] = line_terminals['connected'].map({"true": True, 'false': False})

        inconsistency_group = line_terminals.groupby('IdentifiedObject.name')['connected'].nunique().loc[
            lambda x: x > 1].index
        inconsistencies = [
            {
                "xb_key": key,
                "lines": group[['country', 'name', 'connected']].to_dict('records')
            }
            for key, group in line_terminals[line_terminals['IdentifiedObject.name'].isin(inconsistency_group)].groupby(
                'IdentifiedObject.name')
        ]

        report.update(
            {"xborder_inconsistencies": inconsistencies, "xborder_consistency_check": len(inconsistencies) < 1})
    except:
        report.update({"xborder_inconsistencies": None, "xborder_consistency_check": None})

    return report


def check_outage_inconsistencies(report, network, handler, model_metadata):
    try:
        outages = get_uap_outages_from_scenario_time(handler, time_horizon=model_metadata['pmd:timeHorizon'],
                                                     model_timestamp=model_metadata['pmd:scenarioDate'])
        outages['mrid'] = outages['mrid'].str.lstrip('_')

        connectivity_nodes = type_tableview_merge(network, "Terminal->ConnectivityNode")
        line_terminals = connectivity_nodes.merge(network.type_tableview("ACLineSegment").reset_index(),
                                                  left_on="Terminal.ConductingEquipment",
                                                  right_on="ID",
                                                  suffixes=("", "_Line"))
        line_terminals['ACDCTerminal.connected'] = line_terminals['ACDCTerminal.connected'].str.lower() == 'true'
        outage_terminals = line_terminals[line_terminals['ID'].isin(outages['mrid'])]
        outage_inconsistencies = outage_terminals.groupby('ID').filter(lambda x: x['ACDCTerminal.connected'].all())

        outage_inconsistencies = (
            outage_inconsistencies.groupby(['IdentifiedObject.name', 'ID'])['ACDCTerminal.connected'].agg(
                ['first', 'last'])
            .rename(columns={'first': 'line_end_1_connected', 'last': 'line_end_2_connected'})).reset_index()
        outage_inconsistencies = outage_inconsistencies.rename(
            columns={'ID': 'grid_id', 'IdentifiedObject.name': 'name'}).to_dict('records')
        # TODO add a way to check if line is off when its not supposed to be
        report.update(
            {"outage_inconsistencies": outage_inconsistencies, "outage_check": not bool(outage_inconsistencies)})
    except:
        report.update({"outage_inconsistencies": None, "outage_check": None})

    return report


def check_line_impedance(report, network):
    try:
        lines = type_tableview_merge(network, "ACLineSegment").rename(columns={'ACLineSegment.r': 'r',
                                                                                  'ACLineSegment.x': 'x',
                                                                                  'ConductingEquipment.BaseVoltage': 'BaseVoltage'
                                                                                  })
        # transformers = type_tableview_merge(network, "PowerTransformerEnd").rename(columns={'PowerTransformerEnd.r': 'r',
        #                                                                                       'PowerTransformerEnd.x': 'x',
        #                                                                                        'TransformerEnd.BaseVoltage': 'BaseVoltage'})
        # transformers['Conductor.length'] = 1
        # elements = pd.concat([lines, transformers], axis=0, ignore_index=True)

        elements = lines.merge(type_tableview_merge(network, "BaseVoltage").rename(columns={'ID': 'Id'})[
                                   ['Id', 'BaseVoltage.nominalVoltage']],
                               left_on='BaseVoltage', right_on='Id')
        elements = elements[elements['BaseVoltage.nominalVoltage'] >= 110]
        elements['r_total'] = elements['r'] * elements['Conductor.length']
        elements['x_total'] = elements['x'] * elements['Conductor.length']
        elements['x/r_ratio'] = elements['x_total'] / elements['r_total']

        impedance_data = elements.rename(columns={'ID': 'grid_id', 'Type': 'type', 'IdentifiedObject.name': 'name'})

        impedance_errors = impedance_data[(impedance_data['x/r_ratio'] == np.inf) | (impedance_data['x/r_ratio'] > 50) |
                                          (impedance_data['x/r_ratio'] < 0.1)]
        impedance_warnings = impedance_data[(impedance_data['x/r_ratio'] < 1) | (impedance_data['x/r_ratio'] > 20)]
        impedance_warnings = pd.concat([impedance_errors, impedance_warnings]).drop_duplicates(keep=False)
        if not impedance_errors.empty:
            impedance_bool = False
            impedance_error_dict = impedance_errors[['grid_id', 'name', 'type', 'r', 'x', 'x/r_ratio']].to_dict(
                orient='records')
        else:
            impedance_bool = True
            impedance_error_dict = {}
        if not impedance_warnings.empty:
            impedance_warning_dict = impedance_warnings[['grid_id', 'name', 'type', 'r', 'x', 'x/r_ratio']].to_dict(
                orient='records')
        else:
            impedance_warning_dict = {}
        report.update({"impedance_errors": impedance_error_dict, "impedance_warnings:": impedance_warning_dict,
                       "impedance_check": impedance_bool})
    except:
        report.update({"impedance_errors": None, "impedance_warnings": None, "impedance_check": None})

    return report


def check_line_limits(report, network, handler, limit_temperature='25 C'):

    try:
        terminals = type_tableview_merge(network, "OperationalLimitSet->Terminal")
        line_segments = type_tableview_merge(network, "ACLineSegment")
        terminals = terminals.merge(line_segments, left_on='Terminal.ConductingEquipment', right_on='ID')
        current_limits = type_tableview_merge(network, "CurrentLimit")
        terminals = terminals.merge(current_limits, right_on='OperationalLimit.OperationalLimitSet',
                                    left_on='ID_OperationalLimitSet', suffixes=('_line', '_limit'))

        line_ratings = handler.elastic_service.get_docs_by_query(index='config-line-ratings',
                                                                 query={"match_all": {}}, size=10000, return_df=True)
        line_ratings['grid_id'] = line_ratings['grid_id'].str.lstrip('_')
        line_ratings = line_ratings.merge(terminals, left_on='grid_id', right_on='ID_line')

        limit_values = (line_ratings.groupby('grid_id')['CurrentLimit.value'].agg(['first', 'last'])
                        .rename(columns={'first': 'CurrentLimit.value1', 'last': 'CurrentLimit.value2'}))
        line_ratings = line_ratings.drop_duplicates(subset='grid_id', keep='first').drop(['CurrentLimit.value'], axis=1)

        result_df = line_ratings.merge(limit_values, left_on='grid_id', right_index=True)
        result_df['flag'] = (result_df['CurrentLimit.value1'] - result_df[limit_temperature]).abs() / result_df[limit_temperature] > 0.01
        result_df = result_df.rename(columns={limit_temperature: 'set_limit'})

        line_rating_mismatch =  result_df[result_df['flag']][['IdentifiedObject.name_line', 'grid_id', 'set_limit',
                                             'CurrentLimit.value1']].to_dict('records')
        report.update(
            {"line_rating_mismatch": line_rating_mismatch, "line_rating_check": not bool(line_rating_mismatch)})
    except:
        report.update({"line_rating_mismatch": None, "line_rating_check": None})

    return report
