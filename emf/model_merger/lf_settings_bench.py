"""
Interactive helper for trying out multiple pypowsybl loadflow settings against an
already-loaded network, without re-downloading/re-parsing models on every attempt.

Intended usage is from an IDE's Python console (PyCharm "Python Console", VS Code
Jupyter cell, ipython, etc.) so the expensive part - building the merged model and
fetching the AC/DC schedules - runs once and stays in memory, while `compare_settings(...)`
gets re-run as many times as needed with different settings dicts:

    # 1. Build the merged model and schedules ONCE, reusing the same retrieval,
    #    replacement, and outage/igm-ssh-error pre-processing as the real merge task
    #    (HandlerMergeModels.handle() in model_merger.py), stopping right before the
    #    load flow itself. Read-only against ObjectStorage/OPDM - nothing gets
    #    uploaded, published, or written to Elastic.
    from emf.model_merger.lf_settings_bench import build_merge_inputs, SAMPLE_TASK_PROPERTIES

    task_properties = dict(SAMPLE_TASK_PROPERTIES)  # copy, then edit for your scenario:
    task_properties["timestamp_utc"] = "2025-02-14T03:30:00Z"
    merged_model, ac_schedules, dc_schedules = build_merge_inputs(task_properties)
    network = merged_model.network

    # 2. Compare settings - re-run this as many times as needed, tweaking `settings`,
    #    WITHOUT repeating step 1. Each config runs on its own pypowsybl variant so
    #    attempts never see each other's mutations.
    from emf.model_merger.lf_settings_bench import compare_settings, print_comparison, with_overrides
    from emf.common.loadflow_tool import loadflow_settings as ls

    settings = {
        'EU_DEFAULT': ls.EU_DEFAULT,
        'EU_RELAXED': ls.EU_RELAXED,
        'krylov': with_overrides(ls.EU_DEFAULT, acSolverType='NEWTON_KRYLOV'),
        'no_q_limits': with_overrides(ls.EU_DEFAULT, use_reactive_limits=False),
    }
    results = compare_settings(network, settings, ac_schedules, dc_schedules)
    print_comparison(results)  # one short block per label - readable in a narrow console
    # results is still a normal DataFrame if you want to sort/filter/export it instead:
    # results.to_csv('lf_bench.csv')

Pass ac_schedules/dc_schedules to also run scaler.scale_balance() per config (mirrors
the merge->scale chain in model_merger.py); omit them to only compare the load flow.

For a lighter-weight load that skips replacement/outage/igm-ssh-error pre-processing
(plain OPDM download only), build network/ac_schedules/dc_schedules directly instead:

    from emf.common.helpers.loadflow import load_network_model
    from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
    from emf.common.integrations.object_storage.schedules import query_acnp_schedules, query_hvdc_schedules

    valid_models = get_latest_models_and_download(time_horizon, scenario_date, valid=True)
    network = load_network_model(valid_models + [get_latest_boundary()])
    ac_schedules = query_acnp_schedules(time_horizon, scenario_date)
    dc_schedules = query_hvdc_schedules(time_horizon, scenario_date)
"""
import copy
import json
import logging
import time

import pandas as pd
import pypowsybl as pp

import config
from emf.common.helpers.loadflow import load_network_model
from emf.common.helpers.utils import attr_to_dict, convert_dict_str_to_bool
from emf.common.integrations.object_storage.models import get_latest_boundary, get_latest_models_and_download
from emf.common.integrations.object_storage.schedules import (
    calculate_ac_net_position,
    query_acnp_schedules,
    query_hvdc_schedules,
)
from emf.model_merger import merge_functions, scaler
from emf.model_merger.merge_functions import filter_models_by_acnp
from emf.model_merger.model_merger import (
    ACNP_THRESHOLD,
    CONFORM_LOAD_FACTOR,
    REMOVE_GENERATORS_FROM_SLACK_DISTRIBUTION,
    MergedModel,
)
from emf.model_merger.replacement import get_tsos_available_in_storage, run_replacement
from emf.model_merger.temporary import handle_igm_ssh_vs_cgm_ssh_error

logger = logging.getLogger(__name__)

BENCH_VARIANT_PREFIX = "bench__"

# Same shape as model_merger.py's sample_task['task_properties']. upload_to_minio is
# flipped to "False" here (the real sample has it "True") since this constant is meant
# for the read-only bench workflow below - build_merge_inputs() never looks at it (or at
# upload_to_opdm/send_merge_report/lvl8_reporting/scaling, all of which belong to the
# second half of HandlerMergeModels.handle() that this module deliberately skips), but
# keeping it "False" here avoids it being copy-pasted somewhere that does act on it.
SAMPLE_TASK_PROPERTIES = {
    "timestamp_utc": "2026-08-13T15:30:00+00:00",
    "merge_type": "BA",
    "merging_entity": "BALTICRCC",
    "included": ["PSE", "LITGRID", "ELERING", "AST"],
    "excluded": [],
    "local_import": [],
    "replace_tso": [],
    "time_horizon": "1D",
    "version": "000",
    "mas": "http://www.baltic-rsc.eu/OperationalPlanning",
    "post_temp_fixes": "True",
    "replacement": "True",
    "scaling": "True",
    "outage_update": "True",
    "force_outage_fix": "False",
    "upload_to_opdm": "False",
    "upload_to_minio": "False",
    "send_merge_report": "True",
    "lvl8_reporting": "False",
}


def build_merge_inputs(task_properties: dict):
    """
    Re-runs the real merge task's retrieval and pre-processing - model/boundary
    retrieval, replacement, outage fixes, igm-ssh-vs-cgm-ssh correction - stopping
    right before the load flow, so compare_settings() runs against a realistically
    pre-processed network instead of a plain OPDM download. This is a deliberate,
    line-by-line port of the first half of HandlerMergeModels.handle() in
    model_merger.py (through ensure_paired_boundary_line_connectivity, i.e. everything
    up to but excluding the self.run_loadflow(...) call) - kept as a separate function
    rather than calling handle() itself, because handle()'s second half unconditionally
    writes to Elastic (OPDE_MODELS_ELK_INDEX, task-status, trace) regardless of the
    upload_to_opdm/upload_to_minio flags, which a repeated settings-comparison loop
    should not be doing on every attempt.

    :param task_properties: same shape as model_merger.py's sample_task['task_properties']
                            (see SAMPLE_TASK_PROPERTIES for a ready-to-copy-and-edit version)
    :return: (merged_model, ac_schedules, dc_schedules). merged_model.network is the
            unsolved, pre-processed network; merged_model.excluded/.replaced_entity/
            .outages_updated record what got substituted or fixed along the way.
    """
    task_properties = convert_dict_str_to_bool(task_properties)
    merged_model = MergedModel()

    included_models = task_properties.get("included", [])
    excluded_models = task_properties.get("excluded", [])
    local_import_models = task_properties.get("local_import", [])
    replace_tso = task_properties.get("replace_tso", [])
    time_horizon = task_properties["time_horizon"]
    scenario_datetime = task_properties["timestamp_utc"]
    schedule_start = task_properties.get("reference_schedule_start_utc")
    schedule_time_horizon = task_properties.get("reference_schedule_time_horizon")
    merging_area = task_properties["merge_type"]
    model_replacement = task_properties["replacement"]
    outage_update = task_properties["outage_update"]
    force_outage_fix = task_properties["force_outage_fix"]

    if not schedule_time_horizon or schedule_time_horizon == "AUTO":
        schedule_time_horizon = time_horizon
    if not schedule_start:
        schedule_start = scenario_datetime

    ac_schedules = query_acnp_schedules(time_horizon=schedule_time_horizon, scenario_timestamp=schedule_start,
                                        merged_model=merged_model)
    dc_schedules = query_hvdc_schedules(time_horizon=schedule_time_horizon, scenario_timestamp=schedule_start)
    acnp_dict = calculate_ac_net_position(ac_schedules)

    config_areas_mapping = config.paths.cgm_worker.config_areas_mapping
    tsos_config_json = json.load(config_areas_mapping)
    full_tso_list = [area["party.name"] for area in tsos_config_json if "party.name" in area]
    desired_tsos = merge_functions.filter_models(tsos=full_tso_list, included_models=included_models,
                                               excluded_models=excluded_models)

    models = get_latest_models_and_download(time_horizon=time_horizon, scenario_date=scenario_datetime,
                                            valid=True, tso=desired_tsos, data_source="OPDM")
    latest_boundary = get_latest_boundary()

    if local_import_models:
        additional_models = get_latest_models_and_download(time_horizon=time_horizon, scenario_date=scenario_datetime,
                                                            valid=True, tso=local_import_models, data_source="PDN")
        additional_tsos = {model["pmd:TSO"] for model in additional_models}
        missing_local_import = [tso for tso in local_import_models if tso not in additional_tsos]
        merged_model.excluded.extend([{"tso": tso, "reason": "missing-pdn"} for tso in missing_local_import])
    else:
        additional_models = []
        missing_local_import = []

    if included_models:
        models_tsos = {model["pmd:TSO"] for model in models}
        missing_models = [tso for tso in included_models if tso not in models_tsos]
        if missing_models:
            merged_model.excluded.extend([{"tso": tso, "reason": "missing-opdm"} for tso in missing_models])

        missing_models_rmm = [tso for tso in missing_models if merging_area == "BA"]
        if missing_models_rmm:
            pdn_auto_models = get_latest_models_and_download(time_horizon=time_horizon, scenario_date=scenario_datetime,
                                                              valid=True, tso=missing_models_rmm, data_source="PDN")
            pdn_tsos = {m["pmd:TSO"] for m in pdn_auto_models}
            missing_pdn_auto = [tso for tso in missing_models_rmm if tso not in pdn_tsos]
            models = models + pdn_auto_models
            for item in merged_model.excluded:
                if item["tso"] in missing_pdn_auto:
                    item["reason"] = "missing-opdm-and-pdn"
            missing_models = [tso for tso in missing_models if tso not in pdn_tsos]
    else:
        if model_replacement:
            available_tsos = get_tsos_available_in_storage(time_horizon=time_horizon)
            valid_model_tsos = [model["pmd:TSO"] for model in models]
            missing_models = [tso for tso in available_tsos if tso not in valid_model_tsos + excluded_models]
            if missing_models:
                merged_model.excluded.extend([{"tso": tso, "reason": "missing-opdm"} for tso in missing_models])
        else:
            missing_models = []

    if acnp_dict:
        filterd_models = filter_models_by_acnp(models, merged_model, acnp_dict, ACNP_THRESHOLD, CONFORM_LOAD_FACTOR)
        filterd_additional_models = filter_models_by_acnp(additional_models, merged_model, acnp_dict, ACNP_THRESHOLD,
                                                  CONFORM_LOAD_FACTOR)
        if included_models:
            missing_models = [m for m in included_models if m not in [m["pmd:TSO"] for m in filterd_models]]
            missing_local_import = [m for m in local_import_models
                                    if m not in [m["pmd:TSO"] for m in filterd_additional_models]]
            models = filterd_models
            additional_models = filterd_additional_models
        elif model_replacement:
            models_tsos = {model["pmd:TSO"] for model in models}
            excluded_incorrect = [tso for tso in valid_model_tsos if tso not in models_tsos and tso not in missing_models]
            missing_models = missing_models + excluded_incorrect

    models, additional_models = run_replacement(
        models=models, additional_models=additional_models, model_replacement=model_replacement,
        local_import_models=local_import_models, missing_local_import=missing_local_import,
        missing_models=missing_models, replace_tso=replace_tso, time_horizon=time_horizon,
        scenario_datetime=scenario_datetime, merged_model=merged_model, acnp_dict=acnp_dict,
        acnp_threshold=ACNP_THRESHOLD, conform_load_factor=CONFORM_LOAD_FACTOR,
    )

    input_models = models + additional_models + [latest_boundary]
    if len(input_models) < 2:
        raise RuntimeError("No valid models found for merging")

    merged_model.network = load_network_model(opdm_objects=input_models)
    merged_model.network_meta = attr_to_dict(instance=merged_model.network, sanitize_to_strings=True)
    merged_model.included = [model["pmd:TSO"] for model in input_models if model.get("pmd:TSO", None)]

    replaced_tso_list = [entity["tso"] for entity in merged_model.replaced_entity]
    tso_list = []
    if force_outage_fix:
        tso_list = merged_model.included
    elif outage_update and merging_area == "BA" and any(tso in ["LITGRID", "AST", "ELERING"] for tso in replaced_tso_list):
        tso_list = replaced_tso_list
    if tso_list:
        merged_model = merge_functions.update_model_outages(merged_model=merged_model, tso_list=tso_list,
                                                             scenario_datetime=scenario_datetime, time_horizon=time_horizon)

    if json.loads(REMOVE_GENERATORS_FROM_SLACK_DISTRIBUTION.lower()):
        merged_model.network = handle_igm_ssh_vs_cgm_ssh_error(network_pre_instance=merged_model.network)

    merged_model.network = merge_functions.ensure_paired_equivalent_injection_compatibility(network=merged_model.network)
    merged_model.network = merge_functions.ensure_paired_boundary_line_connectivity(network=merged_model.network)

    return merged_model, ac_schedules, dc_schedules


class BenchModel:
    """Minimal stand-in for MergedModel: scale_balance() only needs `.network` and
    otherwise assigns result attributes onto it freely (.scaled, .scaled_entity, ...).

    scale_balance() has early `return model` paths on divergence (main-island divergence
    on the first load flow, after HVDC/ACNP alignment, or mid-iteration) that skip setting
    .scaled_entity - and the very first one skips .scaled too. Defaulting them to None here
    means a diverged attempt reads back as a clean result instead of an AttributeError."""
    network = None
    scaled = None
    scaled_entity = None
    scaled_hvdc = None


def with_overrides(base: pp.loadflow.Parameters, **overrides) -> pp.loadflow.Parameters:
    """
    Deep-copies `base` and applies each override, for quickly deriving a one-off
    variant from an existing named settings object (e.g. loadflow_settings.EU_DEFAULT)
    instead of re-typing a whole Parameters(...) call.

    A key that matches a real top-level Parameters field (use_reactive_limits,
    distributed_slack, balance_type, ...) is set directly; anything else is treated as
    an OpenLoadFlow provider parameter and written into provider_parameters (stringified,
    as pypowsybl provider parameters are always strings).

    Example: with_overrides(ls.EU_DEFAULT, use_reactive_limits=False, acSolverType='NEWTON_KRYLOV')
    """
    variant = copy.deepcopy(base)
    for key, value in overrides.items():
        if key != "provider_parameters" and hasattr(variant, key):
            setattr(variant, key, value)
        else:
            variant.provider_parameters[key] = str(value)
    return variant


def compare_settings(network: pp.network.Network,
                     settings: dict[str, pp.loadflow.Parameters],
                     ac_schedules: list[dict] | None = None,
                     dc_schedules: list[dict] | None = None,
                     base_variant: str | None = None) -> pd.DataFrame:
    """
    Runs AC load flow - and, if ac_schedules/dc_schedules are both given, scaling on top
    of it, same as model_merger.py's merge -> scale chain - for each entry in `settings`,
    on the SAME already-loaded `network`. Each config runs on its own pypowsybl variant
    (cloned from `base_variant`) so attempts are isolated and `network`'s working variant
    is restored to `base_variant` when done - safe to call repeatedly from a console.

    :param network: an already-loaded pypowsybl network (load it once, reuse across calls)
    :param settings: label -> pypowsybl.loadflow.Parameters to try under that label
    :param ac_schedules: pass together with dc_schedules to also run scaler.scale_balance
    :param dc_schedules: pass together with ac_schedules to also run scaler.scale_balance
    :param base_variant: variant to clone from (defaults to network's current working variant)
    :return: one row per label with load flow (and, if requested, scaling) results
    """
    base_variant = base_variant or network.get_working_variant_id()
    run_scaling = bool(ac_schedules) and bool(dc_schedules)
    rows = []

    for name, params in settings.items():
        variant_id = f"{BENCH_VARIANT_PREFIX}{name}"
        network.clone_variant(base_variant, variant_id, may_overwrite=True)
        network.set_working_variant(variant_id)
        row = {"settings": name}

        try:
            t0 = time.perf_counter()
            lf_results = pp.loadflow.run_ac(network=network, parameters=params)
            main_island = lf_results[0]
            row["lf_status"] = main_island.status_text
            row["lf_seconds"] = round(time.perf_counter() - t0, 2)
            row["lf_iterations"] = main_island.iteration_count
            converged_islands = sum(1 for island in lf_results if island.status_text == "Converged")
            row["lf_islands_converged"] = f"{converged_islands}/{len(lf_results)}"
            row["lf_distributed_active_power"] = round(main_island.distributed_active_power, 2)
            slack_results = main_island.slack_bus_results
            row["lf_active_power_mismatch"] = round(slack_results[0].active_power_mismatch, 3) if slack_results else None
        except Exception as error:
            logger.error(f"[{name}] Load flow failed: {error}", exc_info=True)
            row["lf_status"] = f"ERROR: {error}"

        if run_scaling and row.get("lf_status") == "Converged":
            bench_model = BenchModel()
            bench_model.network = network
            try:
                t0 = time.perf_counter()
                bench_model = scaler.scale_balance(model=bench_model, ac_schedules=ac_schedules,
                                                    dc_schedules=dc_schedules, lf_settings=params)
                row["scale_seconds"] = round(time.perf_counter() - t0, 2)
                row["scaled"] = bench_model.scaled

                areas = bench_model.scaled_entity or []
                ok_areas = [entry for entry in areas if entry.get("success")]
                row["scale_areas_ok"] = f"{len(ok_areas)}/{len(areas)}" if areas else None

                initial_offsets = [abs(entry["initial_offset_acnp"]) for entry in areas
                                   if entry.get("initial_offset_acnp") is not None]
                final_offsets = [abs(entry["final_offset_acnp"]) for entry in areas
                                 if entry.get("final_offset_acnp") is not None]
                row["scale_initial_max_offset"] = round(max(initial_offsets), 2) if initial_offsets else None
                row["scale_final_max_offset"] = round(max(final_offsets), 2) if final_offsets else None
            except Exception as error:
                logger.error(f"[{name}] Scaling failed: {error}", exc_info=True)
                row["scaled"] = f"ERROR: {error}"
        elif run_scaling:
            row["scaled"] = "SKIPPED (load flow did not converge)"

        rows.append(row)

    network.set_working_variant(base_variant)
    return pd.DataFrame(rows).set_index("settings")


def print_comparison(results: pd.DataFrame):
    """
    Prints compare_settings()'s result one settings label per block, each line short
    and independent, instead of one wide table - stays readable in a narrow console
    where a ~9-column DataFrame would wrap or truncate.

    scaled reads as OK/FAILED/N/A based on which of scale_balance()'s early-return
    paths (if any) was hit: None means it diverged before the scaling loop even
    started (first load flow or HVDC/ACNP alignment); False means it entered the
    loop but didn't close the gap within scaler.properties' MAX_ITERATION, or
    diverged partway through - see the BenchModel docstring above.
    """
    has_scaling = "scaled" in results.columns

    for name, row in results.iterrows():
        print(f"--- {name} ---")

        lf_status = row.get("lf_status")
        print(f"  load flow: {lf_status}")
        if lf_status == "Converged":
            print(f"    time:      {row.get('lf_seconds')}s ({row.get('lf_iterations')} iterations)")
            print(f"    islands:   {row.get('lf_islands_converged')} converged")
            print(f"    mismatch:  {row.get('lf_active_power_mismatch')} MW")
            print(f"    distrib_p: {row.get('lf_distributed_active_power')} MW")

        if has_scaling:
            scaled_value = row.get("scaled")
            if scaled_value is True:
                scale_status = "OK"
            elif scaled_value is False:
                scale_status = "FAILED (iteration budget or mid-loop divergence)"
            elif scaled_value is None:
                scale_status = "N/A (diverged before the scaling loop started)"
            else:
                scale_status = str(scaled_value)  # "ERROR: ..." / "SKIPPED (...)"
            print(f"  scaling:   {scale_status}")
            if isinstance(scaled_value, bool):
                print(f"    time:      {row.get('scale_seconds')}s")
                print(f"    areas ok:  {row.get('scale_areas_ok')}")
                print(f"    offset:    {row.get('scale_initial_max_offset')} -> {row.get('scale_final_max_offset')} MW")
        print()


def cleanup_bench_variants(network: pp.network.Network):
    """Removes all variants created by compare_settings(), for tidying up a long interactive session."""
    for variant_id in network.get_variant_ids():
        if variant_id.startswith(BENCH_VARIANT_PREFIX):
            network.remove_variant(variant_id)


if __name__ == "__main__":
    # Runs against a real merged model - build_merge_inputs() does the actual retrieval/
    # replacement/outage-fix/igm-ssh-error pre-processing (see its docstring), so this
    # needs the full EMFOS stack and credentials (ELK, OPDM, MinIO) configured, same as
    # running model_merger.py's own sample_task would. Edit SAMPLE_TASK_PROPERTIES below
    # for your scenario (timestamp_utc, merge_type, included TSOs, ...) before running.
    import sys
    from emf.common.loadflow_tool import loadflow_settings as ls

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    task_properties = dict(SAMPLE_TASK_PROPERTIES)
    merged_model, ac_schedules, dc_schedules = build_merge_inputs(task_properties)
    demo_network = merged_model.network

    # EU_DEFAULT baseline plus the alignment-safe convergence candidates (none of these
    # touch a parameter the RCC alignment sheet marks ALIGNED - see the settings review
    # earlier in this conversation for why acSolverType/areaInterchangeControl are excluded).
    demo_settings = {
        "EU_DEFAULT": ls.EU_DEFAULT,
        "extrapolate_reactive_limits": with_overrides(ls.EU_DEFAULT, extrapolateReactiveLimits=True),
        "force_target_q": with_overrides(ls.EU_DEFAULT, forceTargetQInReactiveLimits=True),
        "k_equal_proportion": with_overrides(ls.EU_DEFAULT, reactivePowerDispatchMode="K_EQUAL_PROPORTION"),
        "fix_voltage_targets": with_overrides(ls.EU_DEFAULT, fixVoltageTargets=True),
        "keep_initial_tap": with_overrides(ls.EU_DEFAULT, transformerVoltageControlUseInitialTapPosition=True),
    }
    print_comparison(compare_settings(demo_network, demo_settings, ac_schedules, dc_schedules))
    cleanup_bench_variants(demo_network)
    print("Run complete")
