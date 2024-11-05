import triplets
from emf.loadflow_tool.helper import create_opdm_objects
from emf.loadflow_tool.model_merger.merge_functions import (load_opdm_data, create_sv_and_updated_ssh, fix_sv_shunts,
                                                            fix_sv_tapsteps, remove_duplicate_sv_voltages,
                                                            remove_small_islands,check_and_fix_dependencies,
                                                            disconnect_equipment_if_flow_sum_not_zero,
                                                            export_to_cgmes_zip, set_brell_lines_to_zero_in_models,
                                                            configure_paired_boundarypoint_injections_by_nodes,
                                                            set_brell_lines_to_zero_in_models_new)


def run_pre_merge_processing(input_models, merging_area):

    # TODO warning logs for temp fix functions

    # SET BRELL LINE VALUES
    if merging_area == 'BA':
        input_models = set_brell_lines_to_zero_in_models(input_models)

    assembled_data = load_opdm_data(input_models)

    # TODO try to optimize it better
    # if merging_area == 'BA':
    #     assembled_data = set_brell_lines_to_zero_in_models_new(assembled_data)

    assembled_data = triplets.cgmes_tools.update_FullModel_from_filename(assembled_data)
    assembled_data = configure_paired_boundarypoint_injections_by_nodes(assembled_data)
    escape_upper_xml = assembled_data[assembled_data['VALUE'].astype(str).str.contains('.XML')]
    if not escape_upper_xml.empty:
        escape_upper_xml['VALUE'] = escape_upper_xml['VALUE'].str.replace('.XML', '.xml')
        assembled_data = triplets.rdf_parser.update_triplet_from_triplet(assembled_data, escape_upper_xml, update=True, add=False)

    input_models = create_opdm_objects([export_to_cgmes_zip([assembled_data])])

    return input_models


def run_post_merge_processing(input_models, solved_model, task_properties, SMALL_ISLAND_SIZE, enable_temp_fixes,
                              time_horizon: str=None):

    time_horizon = time_horizon or task_properties["time_horizon"]
    scenario_datetime = task_properties["timestamp_utc"]
    merging_area = task_properties["merge_type"]
    merging_entity = task_properties["merging_entity"]
    mas = task_properties["mas"]
    version = task_properties["version"]

    models_as_triplets = load_opdm_data(input_models)
    sv_data, ssh_data = create_sv_and_updated_ssh(solved_model, input_models, models_as_triplets,
                                                  scenario_datetime, time_horizon,
                                                  version, merging_area,
                                                  merging_entity, mas)

    if enable_temp_fixes:
        sv_data = fix_sv_shunts(sv_data, models_as_triplets)
        sv_data = fix_sv_tapsteps(sv_data, ssh_data)
        sv_data = remove_small_islands(sv_data, int(SMALL_ISLAND_SIZE))
        sv_data = remove_duplicate_sv_voltages(cgm_sv_data=sv_data, original_data=models_as_triplets)
        sv_data = check_and_fix_dependencies(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data, original_data=models_as_triplets)
        sv_data, ssh_data = disconnect_equipment_if_flow_sum_not_zero(cgm_sv_data=sv_data, cgm_ssh_data=ssh_data, original_data=models_as_triplets)

    return sv_data, ssh_data
