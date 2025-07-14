import pandas as pd
import triplets


def configure_paired_boundarypoint_injections(data):
    # TODO [LEGACY]
    """Where there are paired boundary points, equivalent injections need to be modified
    Set P and Q to 0 - so that no additional consumption or production is on tie line
    Set voltage control off - so that no additional consumption or production is on tie line
    Set terminal to connected - to be sure we have paired connected injections at boundary point
    """
    boundary_points = data.query("KEY == 'ConnectivityNode.boundaryPoint' and VALUE == 'true'")[["ID"]]
    boundary_points = boundary_points.merge(data.type_tableview("Terminal").reset_index(),
                                            left_on="ID",
                                            right_on="Terminal.ConnectivityNode",
                                            suffixes=('_ConnectivityNode', '_Terminal'))
    injections = data.type_tableview('EquivalentInjection').reset_index().merge(boundary_points,
                                                                                left_on="ID",
                                                                                right_on='Terminal.ConductingEquipment',
                                                                                suffixes=('_ConnectivityNode', ''))

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

    return data.update_triplet_from_triplet(pd.concat([updated_terminal_status, updated_regulation_status, updated_p_value, updated_q_value], ignore_index=True), add=False)


def configure_paired_boundarypoint_injections_by_nodes(data):
    # TODO [LEGACY]
    """Where there are paired boundary points, equivalent injections need to be modified
    Set P and Q to 0 - so that no additional consumption or production is on tie line
    Set voltage control off - so that no additional consumption or production is on tie line
    Set terminal to connected - to be sure we have paired connected injections at boundary point

    TODO NOTE THAT THIS IS COPY FROM 'configure_paired_boundarypoint_injections'
    In some models terminals are missing references to ConnectivityNodes
    """
    connectivity_boundary_points = data.query("KEY == 'ConnectivityNode.boundaryPoint' and VALUE == 'true'")[["ID"]]
    topological_boundary_points = data.query("KEY == 'TopologicalNode.boundaryPoint' and VALUE == 'true'")[["ID"]]
    try:
        terminals = data.type_tableview("Terminal").reset_index()[['ID',
                                                                   'Terminal.ConductingEquipment',
                                                                   'Terminal.ConnectivityNode',
                                                                   'Terminal.TopologicalNode']]
    except KeyError:
        terminals = data.type_tableview("Terminal").reset_index()[['ID',
                                                                   'Terminal.ConductingEquipment',
                                                                   'Terminal.TopologicalNode']]
    injections = data.type_tableview('EquivalentInjection').reset_index()[['ID',
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
    if 'Terminal.ConnectivityNode' in terminals:
        connectivity_boundary_points = connectivity_boundary_points.merge(terminals,
                                                                          left_on="ID",
                                                                          right_on="Terminal.ConnectivityNode",
                                                                          suffixes=('_ConnectivityNode', '_Terminal'))
        connectivity_injections = injections.merge(connectivity_boundary_points,
                                                   left_on="ID",
                                                   right_on='Terminal.ConductingEquipment',
                                                   suffixes=('_TopologicalNode', ''))

        paired_connectivity_injections = (connectivity_injections.groupby("Terminal.ConnectivityNode")
                                          .filter(lambda x: len(x) == 2))
        merged_injections = paired_connectivity_injections.merge(paired_topological_injections,
                                                                 on='ID',
                                                                 how='outer',
                                                                 indicator=True,
                                                                 suffixes=('_CN', '_TN'))
        only_connectivity_injections = merged_injections[merged_injections['_merge'] == 'left_only']
        only_topological_injections = merged_injections[merged_injections['_merge'] == 'right_only']
        if len(only_connectivity_injections.index) != 0 or len(only_topological_injections.index) == 0:
            paired_injections = paired_connectivity_injections
        else:
            logger.warning(f"Mismatch of finding paired injections from topological nodes and connectivity nodes")
    else:
        logger.warning(f"Terminals do not contain Connectivity nodes")
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

    return data.update_triplet_from_triplet(pd.concat([updated_terminal_status, updated_regulation_status, updated_p_value, updated_q_value], ignore_index=True), add=False)