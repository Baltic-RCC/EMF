"""
Conversion notes (main changes):

1. assumes that triplets is used everywhere

2. `triplets.rdf_parser.*` (pandas-only, deprecated) replaced with
   `.triplets.*` accessor methods (`remove_triplets_from_triplets`,
   `update_triplets_from_triplets`, `update_triplets_from_tableview`),
   which work on both pandas and polars frames.

3. `DataFrame.query(...)` -> `DataFrame.filter(pl.col(...) ...)`.
   `DataFrame.merge(...)` -> `DataFrame.join(...)`

4. `merge(..., how='left'/'outer', indicator=True)` + `.query('_merge == ...')`
   patterns have been replaced with `how="anti"` / `how="semi"` joins

5. `groupby(...).apply(python_function)` replaced by vectorized polars:
   `sort + group_by(..., maintain_order=True).agg(pl.col(...).first())`, or
   window function (`pl.len().over(...)`), See example in
   `remove_duplicate_sv_voltages` .

6. `pandas.Series.isin(...)` replaced by `semi`/`anti` joins

7. `pd.to_numeric(x, errors='coerce')` -> `pl.col(x).cast(pl.Float64, strict=False)`
   (non-numeric strings become `null` instead of `NaN`, which behaves the same way
   for the comparisons and sums used here).

8. `combine_first` -> `pl.coalesce([...])`.

9. `type_tableview`/`id_tableview` on the pandas engine return the object ID as
   the *index*, requiring `.rename_axis(...).reset_index()` everywhere.

10. `open_switches_in_network` is intentionally left untouched: it operates on
    `pypowsybl`'s own pandas-based network tables (`get_switches`/`update_switches`),
    which is a separate library boundary, not our triplet store. Converting it
    to polars would just add a round-trip conversion for no benefit.

11. Places where a numeric literal (e.g. `0`) is written back into a `VALUE`
    column: the triplet store's `VALUE` column is `Utf8` end-to-end, so these are
    written as `pl.lit(0)` the same way the original pandas code assigned a bare
    `0` (object dtype absorbed it silently) — the values are converted to text on
    export the same way as before. !If you see dtype-mismatch errors on `pl.concat`,
    !!!!!!!!check this first!!!!!.
"""

import json
import uuid
import logging
import config
import pandas as pd
import polars as pl
import pypowsybl
import triplets
from emf.common.config_parser import parse_app_properties
from emf.common.helpers.opdm_objects import load_opdm_objects_to_triplets, get_opdm_data_from_models
from emf.model_merger import merge_functions

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.post_processing)


def remove_small_islands(solved_data: pl.DataFrame, island_size_limit: int) -> pl.DataFrame:
    """Remove topological islands with size (count of TopologicalNodes) <= limit."""
    small_island = (
        solved_data
        .filter(pl.col("KEY") == "TopologicalIsland.TopologicalNodes")
        .group_by("ID")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") <= island_size_limit)
    )
    solved_data = solved_data.triplets.remove_triplets_from_triplets(small_island, columns=["ID"])
    logger.info(f"Removed {small_island.height} island(s) with size <= {island_size_limit}")
    return solved_data


def remove_equivalent_shunt_section(sv_data: pl.DataFrame, models_as_triplets: pl.DataFrame) -> pl.DataFrame:
    """Remove Shunt Sections for EQV Shunts from SV profile."""
    equiv_shunt = models_as_triplets.filter((pl.col("KEY") == "Type") & (pl.col("VALUE") == "EquivalentShunt"))
    if equiv_shunt.height > 0:
        shunt_sections = sv_data.filter(pl.col("KEY") == "SvShuntCompensatorSections.ShuntCompensator")
        matched_ids = shunt_sections.join(equiv_shunt.select("ID"), left_on="VALUE", right_on="ID", how="semi")
        shunts_to_remove = sv_data.join(matched_ids.select("ID"), on="ID", how="semi")
        if shunts_to_remove.height > 0:
            logger.info("Removing invalid SvShuntCompensatorSections for EquivalentShunt")
            sv_data = sv_data.triplets.remove_triplets_from_triplets(shunts_to_remove)
    return sv_data


def add_missing_sv_tap_steps(sv_data: pl.DataFrame, ssh_data: pl.DataFrame) -> pl.DataFrame:
    """Update missing tap changer tap steps in SV, taking the value from SSH.
    """
    ssh_tap_steps = ssh_data.filter(pl.col("KEY") == "TapChanger.step")
    sv_tap_steps = sv_data.filter(pl.col("KEY") == "SvTapStep.TapChanger")

    missing_sv_tap_steps = ssh_tap_steps.join(
        sv_tap_steps.select("VALUE"), left_on="ID", right_on="VALUE", how="anti"
    )

    if missing_sv_tap_steps.height == 0:
        return sv_data

    sv_instance_id = sv_data[0, "INSTANCE_ID"]
    logger.info(f"Adding {missing_sv_tap_steps.height} missing SvTapStep(s), taking tap value from SSH")

    new_ids = [str(uuid.uuid4()) for _ in range(missing_sv_tap_steps.height)]
    missing_sv_tap_steps = missing_sv_tap_steps.with_columns(pl.Series("NEW_ID", new_ids))

    col_order = ["ID", "KEY", "VALUE", "INSTANCE_ID"]
    type_rows = missing_sv_tap_steps.select(pl.col("NEW_ID").alias("ID")).with_columns(
        pl.lit("Type").alias("KEY"), pl.lit("SvTapStep").alias("VALUE"), pl.lit(sv_instance_id).alias("INSTANCE_ID")
    ).select(col_order)
    tapchanger_rows = missing_sv_tap_steps.select(
        pl.col("NEW_ID").alias("ID"), pl.col("ID").alias("VALUE")
    ).with_columns(
        pl.lit("SvTapStep.TapChanger").alias("KEY"), pl.lit(sv_instance_id).alias("INSTANCE_ID")
    ).select(col_order)
    position_rows = missing_sv_tap_steps.select(
        pl.col("NEW_ID").alias("ID"), pl.col("VALUE").alias("VALUE")
    ).with_columns(
        pl.lit("SvTapStep.position").alias("KEY"), pl.lit(sv_instance_id).alias("INSTANCE_ID")
    ).select(col_order)

    new_rows = pl.concat([type_rows, tapchanger_rows, position_rows], how="diagonal_relaxed")
    sv_data = pl.concat([sv_data, new_rows], how="diagonal_relaxed")
    return sv_data


def open_switches_in_network(network_pre_instance: pypowsybl.network.Network, switches_dataframe: pd.DataFrame):
    """
    Opens switches in loaded network given by dataframe (uses ID for merging).
    :param network_pre_instance: pypowsybl Network instance where igms are loaded in
    :param switches_dataframe: dataframe (pandas, or polars — converted to pandas below)
    """
    if isinstance(switches_dataframe, pl.DataFrame):
        switches_dataframe = switches_dataframe.to_pandas()

    logger.info(f"Opening {len(switches_dataframe.index)} switches")
    switches = network_pre_instance.get_switches(all_attributes=True).reset_index()
    switches = switches.merge(switches_dataframe[['ID']].rename(columns={'ID': 'id'}), on='id')
    non_retained_closed = switches.merge(switches_dataframe.rename(columns={'ID': 'id'}), on='id')[['id', 'open']]
    non_retained_closed['open'] = True
    network_pre_instance.update_switches(non_retained_closed.set_index('id'))
    return network_pre_instance


def check_and_fix_dependencies(cgm_sv_data: pl.DataFrame, cgm_ssh_data: pl.DataFrame,
                                original_data: pl.DataFrame) -> pl.DataFrame:
    """
    Seems that pypowsybl ver 1.6.0 managed to get rid of dependencies in exported file. This gathers them from
    SSH profiles and from the original models
    :param cgm_sv_data: merged SV profile that is missing the dependencies
    :param cgm_ssh_data: merged SSH profiles, will be used to get SSH dependencies
    :param original_data: original models, will be used to get TP dependencies
    :return updated merged SV profile
    """
    some_data = get_opdm_data_from_models(model_data=original_data)

    tp_file_ids = some_data.filter(
        (pl.col("KEY") == "Model.profile") & pl.col("VALUE").str.contains("Topology"))

    ssh_file_ids = cgm_ssh_data.filter(
        (pl.col("KEY") == "Model.profile") & pl.col("VALUE").str.contains("SteadyStateHypothesis"))
    dependencies = pl.concat([tp_file_ids, ssh_file_ids], how="vertical_relaxed")
    existing_dependencies = cgm_sv_data.filter(pl.col("KEY") == "Model.DependentOn")
    dependency_ids = dependencies.select(pl.col("ID").alias("VALUE"))

    # rows present in the new dependency set that are missing from the existing SV dependencies
    # (equivalent to the old outer-merge's "right_only"/mismatch check)
    missing_from_existing = dependency_ids.join(existing_dependencies.select("VALUE"), on="VALUE", how="anti")

    if missing_from_existing.height > 0:
        cgm_sv_data = cgm_sv_data.triplets.remove_triplets_from_triplets(existing_dependencies)
        full_model_id = cgm_sv_data.filter((pl.col("KEY") == "Type") & (pl.col("VALUE") == "FullModel"))

        logger.info(f"Mismatch of dependencies. Inserting {dependency_ids.height} dependencies to SV profile")
        new_dependencies = dependency_ids.with_columns(
            pl.lit("Model.DependentOn").alias("KEY"),
            pl.lit(full_model_id[0, "ID"]).alias("ID"),
            pl.lit(full_model_id[0, "INSTANCE_ID"]).alias("INSTANCE_ID"),
        )
        cgm_sv_data = cgm_sv_data.triplets.update_triplets_from_triplets(new_dependencies)
    return cgm_sv_data


def get_boundary_nodes_between_igms(model_data) -> pl.DataFrame:
    """
    Filters out nodes that are between the igms (mentioned at least 2 igms)
    :param model_data: input models
    : return series of node ids
    """
    model_data = get_opdm_data_from_models(model_data=model_data)
    all_boundary_nodes = model_data.filter(
        (pl.col("KEY") == "TopologicalNode.boundaryPoint") & (pl.col("VALUE") == "true")
    ).select("ID")
    sv_voltage_targets = model_data.filter(pl.col("KEY") == "SvVoltage.TopologicalNode").select(
        pl.col("VALUE"))

    # a boundary node id that is the join target of >= 2 SvVoltage.TopologicalNode rows
    # is shared between (at least) two igms
    matches = all_boundary_nodes.join(sv_voltage_targets, left_on="ID", right_on="VALUE", how="inner")
    in_several_igms = (
        matches.group_by("ID").agg(pl.len().alias("_count"))
        .filter(pl.col("_count") >= 2)
        .select("ID"))
    return in_several_igms


def remove_duplicate_sv_voltages(cgm_sv_data: pl.DataFrame, original_data: pl.DataFrame) -> pl.DataFrame:
    """
    Pypowsybl 1.6.0 provides multiple sets of SvVoltage values for the topological nodes that are boundary nodes (from
    each IGM side that uses the corresponding boundary node). So this is a hack that removes one of them (preferably the
    one that is zero).

    Vectorized replacement for the original `groupby(...).apply(take_best_match_for_sv_voltage)`:
    "take the first row per group, but if it's zero and a non-zero one exists, take the first non-zero
    one instead".

    :param cgm_sv_data: merged SV profile from where duplicate SvVoltage values are removed
    :param original_data: will be used to get boundary node ids
    :return updated merged SV profile
    """
    # Check that models are in triplets
    some_data = get_opdm_data_from_models(model_data=original_data)
    # Get ids of boundary nodes that are shared by several igms
    in_several_igms = get_boundary_nodes_between_igms(model_data=some_data)
    # Get SvVoltage Ids corresponding to shared boundary nodes
    sv_voltage_ids = cgm_sv_data.filter(pl.col("KEY") == "SvVoltage.TopologicalNode").join(
        in_several_igms.rename({"ID": "VALUE"}), on="VALUE", how="inner")
    # Get SvVoltage voltage values for corresponding SvVoltage Ids
    sv_voltage_values = (
        cgm_sv_data.filter(pl.col("KEY") == "SvVoltage.v")
        .select(["ID", "VALUE"]).rename({"VALUE": "SvVoltage.v"})
        .join(
            sv_voltage_ids.select(["ID", "VALUE"]).rename({"VALUE": "SvVoltage.SvTopologicalNode"}),
            on="ID",))
    # Just in case convert the values to numeric
    sv_voltage_values = (
        sv_voltage_values
        .with_columns(pl.col("SvVoltage.v").cast(pl.Float64, strict=False).alias("_v_numeric"))
        .with_row_index("_row_idx"))
    # Group by topological node id and by some logic take SvVoltage that will be dropped
    voltages_to_keep = (
        sv_voltage_values
        .sort([(pl.col("_v_numeric") == 0), "_row_idx"])
        .group_by("SvVoltage.SvTopologicalNode", maintain_order=True)
        .agg(pl.col("ID").first()))

    voltages_to_discard = sv_voltage_values.join(voltages_to_keep.select("ID"), on="ID", how="anti")
    if voltages_to_discard.height > 0:
        logger.info(f"Removing {voltages_to_discard.height} duplicate voltage levels from boundary nodes")
        sv_voltages_to_remove = cgm_sv_data.join(voltages_to_discard.select("ID"), on="ID", how="semi")
        cgm_sv_data = cgm_sv_data.triplets.remove_triplets_from_triplets(sv_voltages_to_remove)
    return cgm_sv_data


def set_paired_boundary_injections_to_zero(original_models: pl.DataFrame, cgm_ssh_data: pl.DataFrame) -> pl.DataFrame:
    """Where there are paired boundary points, equivalent injections need to be modified
    Set P and Q to 0 - so that no additional consumption or production is on tie line
    Set voltage control off - so that no additional consumption or production is on tie line
    Set terminal to connected - to be sure we have paired connected injections at boundary point
    In some models terminals are missing references to ConnectivityNodes
    """
    topological_boundary_points = original_models.filter(
        (pl.col("KEY") == "TopologicalNode.boundaryPoint") & (pl.col("VALUE") == "true")
    ).select(pl.col("ID").alias("Terminal.TopologicalNode"))

    terminals_full = original_models.triplets.type_tableview("Terminal")
    base_cols = [pl.col("ID").alias("ID_Terminal"), "Terminal.ConductingEquipment", "Terminal.TopologicalNode"]
    if "Terminal.ConnectivityNode" in terminals_full.columns:
        base_cols.insert(2, "Terminal.ConnectivityNode")
    terminals = terminals_full.select(base_cols)

    injections = cgm_ssh_data.triplets.type_tableview("EquivalentInjection").select("ID")

    boundary_terminals = topological_boundary_points.join(terminals, on="Terminal.TopologicalNode", how="inner")

    topological_injections = injections.join(
        boundary_terminals, left_on="ID", right_on="Terminal.ConductingEquipment", how="inner")

    paired_injections = topological_injections.filter(pl.len().over("Terminal.TopologicalNode") == 2)

    updated_terminal_status = paired_injections.select(pl.col("ID_Terminal").alias("ID")).with_columns(
        pl.lit("ACDCTerminal.connected").alias("KEY"), pl.lit("true").alias("VALUE"))
    updated_regulation_status = paired_injections.select("ID").with_columns(
        pl.lit("EquivalentInjection.regulationStatus").alias("KEY"), pl.lit("false").alias("VALUE"))
    updated_p_value = paired_injections.select("ID").with_columns(
        pl.lit("EquivalentInjection.p").alias("KEY"), pl.lit(0).alias("VALUE"))
    updated_q_value = paired_injections.select("ID").with_columns(
        pl.lit("EquivalentInjection.q").alias("KEY"), pl.lit(0).alias("VALUE"))

    updates = pl.concat(
        [updated_terminal_status, updated_regulation_status, updated_p_value, updated_q_value],
        how="diagonal_relaxed",)
    return cgm_ssh_data.triplets.update_triplets_from_triplets(updates, add=False)


def check_energized_boundary_nodes(cgm_sv_data: pl.DataFrame, cgm_ssh_data: pl.DataFrame,
                                    original_models: pl.DataFrame, fix_errors: bool = False) -> pl.DataFrame:
    """
    On one case (1D RTEFrance alone on 01.08.2024 12.30Z) pypowsybl calculates the loadflow and updates
    the voltages on boundaries, however the powerflows are still copied over from the original files.
    This, therefore, joins a lot of tables and, if voltage at some boundary node is zero and the
    equivalent injection is not, sets the injection to zero.
    """
    original_models = get_opdm_data_from_models(model_data=original_models)
    boundary_nodes = original_models.filter(
        (pl.col("KEY") == "TopologicalNode.boundaryPoint") & (pl.col("VALUE") == "true")
    ).select("ID")
    if boundary_nodes.height == 0:
        return cgm_ssh_data
    all_terminals = original_models.triplets.type_tableview("Terminal")
    if all_terminals is None:
        return cgm_ssh_data

    term_cols = ["Terminal", "Terminal.ConductingEquipment", "Terminal.TopologicalNode"]
    terminals = all_terminals.rename({"ID": "Terminal"})
    if "ACDCTerminal.connected" in terminals.columns:
        term_cols.insert(1, "ACDCTerminal.connected")
    terminals = terminals.join(
        boundary_nodes.rename({"ID": "Terminal.TopologicalNode"}), on="Terminal.TopologicalNode", how="inner"
    ).select(term_cols)

    new_voltages = cgm_sv_data.triplets.type_tableview("SvVoltage").rename({"ID": "SvVoltage"}).join(
        boundary_nodes.rename({"ID": "SvVoltage.TopologicalNode"}), on="SvVoltage.TopologicalNode", how="inner"
    ).sort("SvVoltage")
    old_voltages = original_models.triplets.type_tableview("SvVoltage").rename({"ID": "SvVoltage"}).join(
        boundary_nodes.rename({"ID": "SvVoltage.TopologicalNode"}), on="SvVoltage.TopologicalNode", how="inner"
    ).sort("SvVoltage.TopologicalNode")

    old_v = old_voltages.select(["SvVoltage.TopologicalNode", "SvVoltage.v", "SvVoltage.angle"]).rename(
        {"SvVoltage.v": "SvVoltage.v_old", "SvVoltage.angle": "SvVoltage.angle_old"}
    )
    new_v = new_voltages.select(["SvVoltage.TopologicalNode", "SvVoltage.v", "SvVoltage.angle"]).rename(
        {"SvVoltage.v": "SvVoltage.v_new", "SvVoltage.angle": "SvVoltage.angle_new"}
    )
    voltage_diff = old_v.join(new_v, on="SvVoltage.TopologicalNode", how="inner").sort("SvVoltage.TopologicalNode")

    old_powerflows = original_models.triplets.type_tableview("SvPowerFlow").join(
        terminals.rename({"Terminal": "SvPowerFlow.Terminal"}), on="SvPowerFlow.Terminal", how="inner"
    ).sort("Terminal.TopologicalNode")
    new_powerflows = cgm_sv_data.triplets.type_tableview("SvPowerFlow").join(
        terminals.rename({"Terminal": "SvPowerFlow.Terminal"}), on="SvPowerFlow.Terminal", how="inner"
    ).sort("Terminal.TopologicalNode")

    old_pf = old_powerflows.select(["SvPowerFlow.Terminal", "SvPowerFlow.p", "SvPowerFlow.q"]).rename(
        {"SvPowerFlow.p": "SvPowerFlow.p_old", "SvPowerFlow.q": "SvPowerFlow.q_old"}
    )
    new_pf = new_powerflows.select(
        ["SvPowerFlow.Terminal", "SvPowerFlow.p", "SvPowerFlow.q", "ID",
         "Terminal.ConductingEquipment", "Terminal.TopologicalNode"]
    ).rename({"SvPowerFlow.p": "SvPowerFlow.p_new", "SvPowerFlow.q": "SvPowerFlow.q_new", "ID": "SvPowerFlow"})
    powerflow_diff = old_pf.join(new_pf, on="SvPowerFlow.Terminal", how="inner").sort("Terminal.TopologicalNode")

    old_inj = original_models.triplets.type_tableview("EquivalentInjection").rename({"ID": "EquivalentInjection"}).join(
        terminals.rename({"Terminal.ConductingEquipment": "EquivalentInjection"}), on="EquivalentInjection",
        how="inner",
    ).sort("Terminal.TopologicalNode")
    new_inj = cgm_ssh_data.triplets.type_tableview("EquivalentInjection").rename({"ID": "EquivalentInjection"}).join(
        terminals.rename({"Terminal.ConductingEquipment": "EquivalentInjection"}), on="EquivalentInjection",
        how="inner",
    ).sort("Terminal.TopologicalNode")

    old_inj_r = old_inj.select(["EquivalentInjection", "EquivalentInjection.p", "EquivalentInjection.q"]).rename(
        {"EquivalentInjection.p": "EquivalentInjection.p_old", "EquivalentInjection.q": "EquivalentInjection.q_old"}
    )
    new_inj_r = new_inj.select(["EquivalentInjection", "EquivalentInjection.p", "EquivalentInjection.q"]).rename(
        {"EquivalentInjection.p": "EquivalentInjection.p_new", "EquivalentInjection.q": "EquivalentInjection.q_new"}
    )
    injection_diff = old_inj_r.join(new_inj_r, on="EquivalentInjection", how="inner").sort("EquivalentInjection")

    all_together = powerflow_diff.rename({
        "SvPowerFlow.Terminal": "Terminal",
        "Terminal.ConductingEquipment": "EquivalentInjection",
        "Terminal.TopologicalNode": "TopologicalNode",
    }).join(injection_diff, on="EquivalentInjection", how="left")

    all_together = all_together.join(
        voltage_diff.rename({"SvVoltage.TopologicalNode": "TopologicalNode"}), on="TopologicalNode", how="left"
    ).sort("TopologicalNode")

    if all_together.height == 0:
        return cgm_ssh_data

    zero_voltages = all_together.filter(
        (pl.col("SvVoltage.v_new") == 0) & (pl.col("SvVoltage.angle_new") == 0)
    )
    if zero_voltages.height == 0:
        return cgm_ssh_data

    zero_voltages = zero_voltages.with_columns(
        pl.sum_horizontal(
            pl.col("EquivalentInjection.p_new").cast(pl.Float64, strict=False).abs().fill_null(0),
            pl.col("EquivalentInjection.q_new").cast(pl.Float64, strict=False).abs().fill_null(0),
        ).alias("Summed_flow")
    )

    not_zero_flows = zero_voltages.filter(pl.col("Summed_flow") != 0)
    if not_zero_flows.height == 0:
        return cgm_ssh_data

    logger.warning(f"{not_zero_flows.height} cases where boundary voltage is zero but injection is not")

    if fix_errors:
        logger.info("Setting injection at boundary to zero")
        updated_injections = not_zero_flows.select(pl.col("EquivalentInjection").alias("ID"))
        updated_p_value = updated_injections.with_columns(
            pl.lit("EquivalentInjection.p").alias("KEY"), pl.lit(0).alias("VALUE")
        )
        updated_q_value = updated_injections.with_columns(
            pl.lit("EquivalentInjection.q").alias("KEY"), pl.lit(0).alias("VALUE")
        )
        cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_triplets(
            pl.concat([updated_p_value, updated_q_value], how="diagonal_relaxed"), add=False
        )
    return cgm_ssh_data


def check_for_disconnected_terminals(cgm_sv_data: pl.DataFrame, original_models: pl.DataFrame,
                                      fix_errors: bool = False) -> pl.DataFrame:
    """
    Checks if disconnected terminals have powerflow different from 0
    :param cgm_sv_data: merged sv profile
    :param original_models: original profiles
    :param fix_errors: sets flows to zero
    :return (updated) sv profile
    """
    all_terminals = original_models.triplets.type_tableview("Terminal")
    power_flows_post = cgm_sv_data.triplets.type_tableview("SvPowerFlow")
    if all_terminals is None or power_flows_post is None:
        return cgm_sv_data

    all_terminals = all_terminals.rename({"ID": "SvPowerFlow.Terminal"})
    disconnected_terminals = all_terminals.filter(pl.col("ACDCTerminal.connected") == "false")
    if disconnected_terminals.height == 0:
        return cgm_sv_data

    disconnected_powerflows = power_flows_post.join(
        disconnected_terminals.select("SvPowerFlow.Terminal"), on="SvPowerFlow.Terminal", how="semi"
    )
    flows_on_powerflows = disconnected_powerflows.filter(
        (pl.col("SvPowerFlow.p").abs() > 0) | (pl.col("SvPowerFlow.q").abs() > 0)
    )
    if flows_on_powerflows.height > 0:
        logger.info(f"Found {flows_on_powerflows.height} disconnected terminals which have flows set")
        if fix_errors:
            logger.info("Setting flows on disconnected terminals to zero")
            flows_on_powerflows = flows_on_powerflows.with_columns(
                pl.lit(0).alias("SvPowerFlow.p"), pl.lit(0).alias("SvPowerFlow.q")
            )
            cgm_sv_data = cgm_sv_data.triplets.update_triplets_from_tableview(
                flows_on_powerflows, add=False, update=True
            )
    return cgm_sv_data


def check_non_regulating_rotating_machine_q(cgm_ssh_data: pl.DataFrame, original_models: pl.DataFrame,
                                             fix_errors: bool = False) -> pl.DataFrame:
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
    original_q = original_models.filter(pl.col("KEY") == "RotatingMachine.q").select(["ID", "VALUE"])
    if original_q.height == 0:
        return cgm_ssh_data

    enabled_controls = original_models.filter(
        (pl.col("KEY") == "RegulatingControl.enabled") & (pl.col("VALUE") == "true")
    ).select("ID")
    machine_control_enabled = original_models.filter(
        (pl.col("KEY") == "RegulatingCondEq.controlEnabled") & (pl.col("VALUE") == "true")
    ).select("ID")
    machine_regulating_control = original_models.filter(
        pl.col("KEY") == "RegulatingCondEq.RegulatingControl"
    ).select(["ID", "VALUE"])

    eligible_machines = (
        machine_regulating_control
        .join(machine_control_enabled, on="ID", how="semi")
        .join(enabled_controls.rename({"ID": "VALUE"}), on="VALUE", how="semi")
    )

    ineligible_q = original_q.join(eligible_machines.select("ID"), on="ID", how="anti")

    if ineligible_q.height > 0:
        logger.info(f"Found {ineligible_q.height} RotatingMachine(s) without an eligible regulating "
                    f"control - restoring their SSH q to the original IGM values")
        if fix_errors:
            ineligible_q = ineligible_q.with_columns(pl.lit("RotatingMachine.q").alias("KEY"))
            cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_triplets(
                ineligible_q.select(["ID", "KEY", "VALUE"]), add=False
            )
    return cgm_ssh_data


def check_rotating_machine_q_outside_p_limits(cgm_ssh_data: pl.DataFrame, original_models: pl.DataFrame,
                                               fix_errors: bool = False) -> pl.DataFrame:
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
    original_q = original_models.filter(pl.col("KEY") == "RotatingMachine.q").select(["ID", "VALUE"])
    original_p = original_models.filter(pl.col("KEY") == "RotatingMachine.p").select(["ID", "VALUE"]).rename(
        {"VALUE": "p"})
    if original_q.height == 0 or original_p.height == 0:
        return cgm_ssh_data

    pgen = original_p.with_columns((-pl.col("p").cast(pl.Float64, strict=False)).alias("pgen"))

    machine_curve = original_models.filter(
        pl.col("KEY") == "SynchronousMachine.InitialReactiveCapabilityCurve"
    ).select(["ID", "VALUE"]).rename({"VALUE": "Curve"})

    curve_points = original_models.filter(pl.col("KEY") == "CurveData.xvalue").select(["ID", "VALUE"]).rename(
        {"VALUE": "xvalue"})
    curve_owner = original_models.filter(pl.col("KEY") == "CurveData.Curve").select(["ID", "VALUE"]).rename(
        {"VALUE": "Curve"})
    curve_limits = (
        curve_points.join(curve_owner, on="ID", how="inner")
        .with_columns(pl.col("xvalue").cast(pl.Float64, strict=False))
        .group_by("Curve")
        .agg(pl.col("xvalue").min().alias("curve_p_min"), pl.col("xvalue").max().alias("curve_p_max")))
    machine_curve_limits = machine_curve.join(curve_limits, on="Curve", how="inner")

    machine_unit = original_models.filter(pl.col("KEY") == "RotatingMachine.GeneratingUnit").select(
        ["ID", "VALUE"]
    ).rename({"VALUE": "GeneratingUnit"})
    unit_min = original_models.filter(pl.col("KEY") == "GeneratingUnit.minOperatingP").select(["ID", "VALUE"]).rename(
        {"ID": "GeneratingUnit", "VALUE": "unit_p_min"}
    ).with_columns(pl.col("unit_p_min").cast(pl.Float64, strict=False))
    unit_max = original_models.filter(pl.col("KEY") == "GeneratingUnit.maxOperatingP").select(["ID", "VALUE"]).rename(
        {"ID": "GeneratingUnit", "VALUE": "unit_p_max"}
    ).with_columns(pl.col("unit_p_max").cast(pl.Float64, strict=False))
    machine_unit_limits = machine_unit.join(unit_min, on="GeneratingUnit", how="inner").join(
        unit_max, on="GeneratingUnit", how="inner")

    limits = pgen.join(machine_curve_limits.select(["ID", "curve_p_min", "curve_p_max"]), on="ID", how="left")
    limits = limits.join(machine_unit_limits.select(["ID", "unit_p_min", "unit_p_max"]), on="ID", how="left")
    limits = limits.with_columns(
        pl.coalesce(["curve_p_min", "unit_p_min"]).alias("p_min"),
        pl.coalesce(["curve_p_max", "unit_p_max"]).alias("p_max"),
    ).drop_nulls(subset=["pgen", "p_min", "p_max"])

    outside_limits = limits.filter((pl.col("pgen") < pl.col("p_min")) | (pl.col("pgen") > pl.col("p_max")))

    ineligible_q = original_q.join(outside_limits.select("ID"), on="ID", how="semi")

    if ineligible_q.height > 0:
        logger.info(f"Found {ineligible_q.height} RotatingMachine(s) with Pgen outside [Pmin, Pmax] - "
                    f"restoring their SSH q to the original IGM values")
        if fix_errors:
            ineligible_q = ineligible_q.with_columns(pl.lit("RotatingMachine.q").alias("KEY"))
            cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_triplets(
                ineligible_q.select(["ID", "KEY", "VALUE"]), add=False)
    return cgm_ssh_data


def check_non_ltc_tap_changer_step(cgm_ssh_data: pl.DataFrame, cgm_sv_data: pl.DataFrame,
                                    original_models: pl.DataFrame, fix_errors: bool = False):
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
    original_step = original_models.filter(pl.col("KEY") == "TapChanger.step").select(["ID", "VALUE"])
    if original_step.height == 0:
        return cgm_ssh_data, cgm_sv_data

    enabled_controls = original_models.filter(
        (pl.col("KEY") == "RegulatingControl.enabled") & (pl.col("VALUE") == "true")
    ).select("ID")
    tap_ltc = original_models.filter(
        (pl.col("KEY") == "TapChanger.ltcFlag") & (pl.col("VALUE") == "true")
    ).select("ID")
    tap_control_enabled = original_models.filter(
        (pl.col("KEY") == "TapChanger.controlEnabled") & (pl.col("VALUE") == "true")
    ).select("ID")
    tap_regulating_control = original_models.filter(
        pl.col("KEY") == "TapChanger.TapChangerControl"
    ).select(["ID", "VALUE"])

    eligible_taps = (
        tap_regulating_control
        .join(tap_ltc, on="ID", how="semi")
        .join(tap_control_enabled, on="ID", how="semi")
        .join(enabled_controls.rename({"ID": "VALUE"}), on="VALUE", how="semi"))

    ineligible_step = original_step.join(eligible_taps.select("ID"), on="ID", how="anti")

    if ineligible_step.height > 0:
        logger.info(f"Found {ineligible_step.height} TapChanger(s) without an eligible LTC control - "
                    f"restoring their SSH step and SV position to the IGM values")
        if fix_errors:
            ssh_update = ineligible_step.with_columns(pl.lit("TapChanger.step").alias("KEY"))
            cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_triplets(
                ssh_update.select(["ID", "KEY", "VALUE"]), add=False)

            sv_tap_steps = cgm_sv_data.filter(pl.col("KEY") == "SvTapStep.TapChanger").select(
                ["ID", "VALUE"]
            ).rename({"ID": "SvTapStep_ID", "VALUE": "ID"})
            sv_update = ineligible_step.join(sv_tap_steps, on="ID", how="inner").select(
                [pl.col("SvTapStep_ID").alias("ID"), "VALUE"]
            )
            if sv_update.height > 0:
                sv_update = sv_update.with_columns(pl.lit("SvTapStep.position").alias("KEY"))
                cgm_sv_data = cgm_sv_data.triplets.update_triplets_from_triplets(
                    sv_update.select(["ID", "KEY", "VALUE"]), add=False)
    return cgm_ssh_data, cgm_sv_data


def check_net_interchanges(cgm_sv_data: pl.DataFrame, cgm_ssh_data: pl.DataFrame,
                            original_models: pl.DataFrame) -> pl.DataFrame:
    """
    An attempt to calculate the net interchange 2 values and check them against those provided in ssh profiles
    :param cgm_sv_data: merged sv profile
    :param cgm_ssh_data: merged ssh profile
    :param original_models: igms in triplets
    :return (updated) ssh profiles
    """
    wanted = ["ControlArea.netInterchange", "ControlArea.pTolerance",
              "IdentifiedObject.energyIdentCodeEic", "IdentifiedObject.name"]

    control_areas = original_models.triplets.type_tableview("ControlArea").rename({"ID": "ControlArea"})
    control_areas = control_areas.select(["ControlArea"] + [c for c in wanted if c in control_areas.columns])

    ssh_areas = cgm_ssh_data.triplets.type_tableview("ControlArea").rename({"ID": "ControlArea"})
    ssh_areas = ssh_areas.select(["ControlArea"] + [c for c in wanted if c in ssh_areas.columns])
    control_areas = control_areas.join(ssh_areas, on="ControlArea", how="inner")
    control_areas = control_areas.select(["ControlArea"] + [c for c in wanted if c in control_areas.columns])

    tie_flows = original_models.triplets.type_tableview("TieFlow").rename({
        "TieFlow.ControlArea": "ControlArea", "TieFlow.Terminal": "Terminal",
    }).select(["ControlArea", "Terminal", "TieFlow.positiveFlowIn"])
    tie_flows = tie_flows.join(control_areas.select("ControlArea"), on="ControlArea", how="semi")

    terminals_full = original_models.triplets.type_tableview("Terminal").rename({"ID": "Terminal"})
    terminal_cols = ["Terminal"] + (
        ["ACDCTerminal.connected"] if "ACDCTerminal.connected" in terminals_full.columns else []
    )
    tie_flows = tie_flows.join(terminals_full.select(terminal_cols), on="Terminal", how="inner")

    power_flows_pre = None
    try:
        power_flows_pre = original_models.triplets.type_tableview("SvPowerFlow").rename(
            {"SvPowerFlow.Terminal": "Terminal"}
        ).select(["Terminal", "SvPowerFlow.p"])
        tie_flows = tie_flows.join(power_flows_pre, on="Terminal", how="left")
        tie_flows = tie_flows.rename({"SvPowerFlow.p": "SvPowerFlow.p_pre"})
    except Exception as error:
        logger.error(f"Was not able to get tie flows from original models with exception: {error}")

    power_flows_post = cgm_sv_data.triplets.type_tableview("SvPowerFlow").rename(
        {"SvPowerFlow.Terminal": "Terminal"}
    ).select(["Terminal", "SvPowerFlow.p"]).rename({"SvPowerFlow.p": "SvPowerFlow.p_post"})
    tie_flows = tie_flows.join(power_flows_post, on="Terminal", how="left")

    if power_flows_pre is not None:
        tie_flows_grouped = tie_flows.group_by("ControlArea").agg(
            pl.col("SvPowerFlow.p_pre").sum(),
            pl.col("SvPowerFlow.p_post").sum(),)
    else:
        tie_flows_grouped = tie_flows.group_by("ControlArea").agg(pl.col("SvPowerFlow.p_post").sum())

    tie_flows_grouped = control_areas.join(tie_flows_grouped, on="ControlArea", how="inner")

    net_interchange_errors = tie_flows_grouped.filter(
        pl.col("ControlArea.netInterchange") != pl.col("SvPowerFlow.p_post"))

    if net_interchange_errors.height > 0:
        logger.info(f"Updating {net_interchange_errors.height} interchanges to new values")
        new_areas = cgm_ssh_data.triplets.type_tableview("ControlArea").select(
            ["ID", "ControlArea.pTolerance", "Type"]
        ).rename({"ID": "ControlArea"})
        new_areas = new_areas.join(
            net_interchange_errors.select(["ControlArea", "SvPowerFlow.p_post"]).rename(
                {"SvPowerFlow.p_post": "ControlArea.netInterchange"}
            ),
            on="ControlArea", how="inner",
        ).rename({"ControlArea": "ID"})
        cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_tableview(new_areas)
    return cgm_ssh_data


def check_non_boundary_equivalent_injections(cgm_sv_data: pl.DataFrame, cgm_ssh_data: pl.DataFrame,
                                              original_models: pl.DataFrame, threshold: float = 0,
                                              fix_errors: bool = False) -> pl.DataFrame:
    """
    Checks equivalent injections that are not on boundary topological nodes
    :param cgm_sv_data: merged SV profile
    :param cgm_ssh_data: merged SSH profile
    :param original_models: igms in triplets
    :param threshold: threshold for checking
    :param fix_errors: if true then copies values from sv profile to ssh profile
    :return cgm_ssh_data
    """
    boundary_nodes = original_models.filter(
        (pl.col("KEY") == "TopologicalNode.boundaryPoint") & (pl.col("VALUE") == "true")
    ).select("ID")

    terminals = original_models.triplets.type_tableview("Terminal").rename({"ID": "SvPowerFlow.Terminal"})
    non_boundary_terminals = terminals.join(
        boundary_nodes.rename({"ID": "Terminal.TopologicalNode"}), on="Terminal.TopologicalNode", how="anti"
    ).select(["SvPowerFlow.Terminal", "Terminal.ConductingEquipment"])

    return check_all_kind_of_injections(
        cgm_sv_data=cgm_sv_data,
        cgm_ssh_data=cgm_ssh_data,
        original_models=original_models,
        injection_name="EquivalentInjection",
        fields_to_check={"SvPowerFlow.p": "EquivalentInjection.p"},
        threshold=threshold,
        terminals=non_boundary_terminals,
        fix_errors=fix_errors,)


def check_all_kind_of_injections(cgm_sv_data: pl.DataFrame,
                                 cgm_ssh_data: pl.DataFrame,
                                 original_models: pl.DataFrame,
                                 injection_name: str = 'ExternalNetworkInjection',
                                 fields_to_check: dict = None,
                                 fix_errors: bool = False,
                                 threshold: float = 0,
                                 terminals: pl.DataFrame = None,
                                 report_sum: bool = True) -> pl.DataFrame:
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

    original_injections = original_models.triplets.type_tableview(injection_name)
    injections = cgm_ssh_data.triplets.type_tableview(injection_name)
    if original_injections is None or injections is None:
        logger.info(f"SSH profile doesn't contain data about {injection_name}")
        return cgm_ssh_data

    wanted_cols = fixed_fields + list(fields_to_check.values())
    missing = [c for c in wanted_cols if c not in injections.columns or c not in original_injections.columns]
    if missing:
        logger.info(f"{injection_name} tableview got error: missing columns {missing}")
        return cgm_ssh_data

    injections_reduced = injections.select(wanted_cols)
    original_injections_reduced = original_injections.select(wanted_cols).rename(
        {v: f"{v}_org" for v in fields_to_check.values()})
    injections_reduced = injections_reduced.join(original_injections_reduced, on="ID", how="inner")

    if terminals is None:
        terminals = original_models.triplets.type_tableview("Terminal").rename(
            {"ID": "SvPowerFlow.Terminal"}
        ).select(["SvPowerFlow.Terminal", "Terminal.ConductingEquipment"])

    flows = cgm_sv_data.triplets.type_tableview("SvPowerFlow").select(
        ["SvPowerFlow.Terminal"] + list(fields_to_check.keys()))
    terminals = terminals.join(flows, on="SvPowerFlow.Terminal", how="inner")
    # polars only keeps the *left* join key when left_on/right_on differ (unlike pandas' merge,
    # which keeps both) — restore "ID" (the injection's own id) explicitly after the join.
    terminals = terminals.join(
        injections_reduced, left_on="Terminal.ConductingEquipment", right_on="ID", how="inner"
    ).with_columns(pl.col("Terminal.ConductingEquipment").alias("ID"))

    filtered_list = []
    for flow_field, injection_field in fields_to_check.items():
        diff = (pl.col(injection_field) - pl.col(flow_field)).abs()
        filtered_list.append(terminals.filter(diff > threshold))
        if report_sum:
            logger.info(
                f"IGM {injection_field} = {terminals[injection_field + '_org'].sum()} vs "
                f"CGM {injection_field} = {terminals[injection_field].sum()} vs "
                f"CGM {flow_field} = {terminals[flow_field].sum()}")

    if not filtered_list:
        return cgm_ssh_data

    filtered = pl.concat(filtered_list, how="diagonal_relaxed").unique()

    if filtered.height > 0:
        logger.warning(f"Found {filtered.height} mismatches between {injection_name} and flow values on terminals")
        if fix_errors:
            logger.info(f"Updating {injection_name} values from terminal flow values")
            injections_update = injections.join(
                filtered.select(fixed_fields + list(fields_to_check.keys())), on="ID", how="inner")
            injections_update = injections_update.drop(list(fields_to_check.values()))
            injections_update = injections_update.rename(fields_to_check)
            cgm_ssh_data = cgm_ssh_data.triplets.update_triplets_from_tableview(
                injections_update, update=True, add=False)
    return cgm_ssh_data


def run_post_merge_processing(input_models: list, exported_model: bytes, opdm_object_meta: dict,
                               additional_processing: bool):
    # Load original input models to triplets
    input_models_triplets = load_opdm_objects_to_triplets(opdm_objects=input_models)

    # Apply corrections to SV profile
    sv_data = merge_functions.update_merged_model_sv(sv_data=exported_model, opdm_object_meta=opdm_object_meta)

    # Create update SSH
    sv_data, ssh_data, opdm_object_meta = merge_functions.create_updated_ssh(
        models_as_triplets=input_models_triplets, input_models=input_models, sv_data=sv_data,
        opdm_object_meta=opdm_object_meta)

    # --- upstream is still pandas, so conversion from/to pandas still necessary:
    input_models_triplets = pl.from_pandas(input_models_triplets)
    sv_data = pl.from_pandas(sv_data)
    ssh_data = pl.from_pandas(ssh_data)

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
    ssh_data = set_paired_boundary_injections_to_zero(original_models=input_models_triplets, cgm_ssh_data=ssh_data)

    if additional_processing:
        sv_data = check_for_disconnected_terminals(cgm_sv_data=sv_data, original_models=input_models_triplets,
                                                     fix_errors=True)
        ssh_data = check_energized_boundary_nodes(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data,
                                                   original_models=input_models_triplets, fix_errors=True)
        ssh_data = check_non_regulating_rotating_machine_q(cgm_ssh_data=ssh_data,
                                                            original_models=input_models_triplets, fix_errors=True)
        ssh_data = check_rotating_machine_q_outside_p_limits(cgm_ssh_data=ssh_data,
                                                              original_models=input_models_triplets, fix_errors=True)
        ssh_data, sv_data = check_non_ltc_tap_changer_step(cgm_ssh_data=ssh_data, cgm_sv_data=sv_data,
                                                            original_models=input_models_triplets, fix_errors=True)

        # Run injections check and apply modification if defined in configuration
        injection_threshold = float(INJECTION_THRESHOLD)
        fix_injection_errors = json.loads(str(FIX_INJECTION_ERRORS).lower())

        ssh_data = check_all_kind_of_injections(cgm_ssh_data=ssh_data, cgm_sv_data=sv_data,
                                                 original_models=input_models_triplets,
                                                 injection_name="EnergySource", threshold=injection_threshold,
                                                 fields_to_check={"SvPowerFlow.p": "EnergySource.activePower"},
                                                 fix_errors=fix_injection_errors)
        ssh_data = check_all_kind_of_injections(cgm_ssh_data=ssh_data, cgm_sv_data=sv_data,
                                                 original_models=input_models_triplets,
                                                 injection_name="ExternalNetworkInjection",
                                                 fields_to_check={"SvPowerFlow.p": "ExternalNetworkInjection.p"},
                                                 threshold=injection_threshold, fix_errors=fix_injection_errors)
        ssh_data = check_non_boundary_equivalent_injections(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data,
                                                             original_models=input_models_triplets,
                                                             threshold=injection_threshold,
                                                             fix_errors=fix_injection_errors)
        try:
            ssh_data = check_net_interchanges(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data,
                                               original_models=input_models_triplets)
        except KeyError:
            logger.warning("No fields for net interchange correction")

    # --- return back to pandas (this can be removed once (if ever) opdm_objects.py and merge_functions.py is in polars)
    sv_data = sv_data.to_pandas()
    ssh_data = ssh_data.to_pandas()

    return sv_data, ssh_data, opdm_object_meta
