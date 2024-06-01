import config
from emf.loadflow_tool.helper import load_model, load_opdm_data, filename_from_metadata, attr_to_dict, export_model
from emf.loadflow_tool import loadflow_settings
import pypowsybl

import logging
import json
import sys
from aniso8601 import parse_datetime
import datetime

import triplets
import pandas

from uuid import uuid4



logger = logging.getLogger(__name__)

def run_lf(merged_model, loadflow_settings=loadflow_settings.CGM_DEFAULT):

    loadflow_report = pypowsybl.report.Reporter()

    loadflow_result = pypowsybl.loadflow.run_ac(network=merged_model["network"],
                                                parameters=loadflow_settings,
                                                reporter=loadflow_report)

    loadflow_result_dict = [attr_to_dict(island) for island in loadflow_result]
    #merged_model["LOADFLOW_REPORT"] = json.loads(loadflow_report.to_json())
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
    })


def create_sv_and_updated_ssh(merged_model, original_models, scenario_date, time_horizon, version, merging_area, merging_entity, mas):

    ### SV ###
    # Set Metadata
    SV_ID = merged_model['network_meta']['id'].split("uuid:")[-1]

    opdm_object_meta = create_opdm_object_meta( SV_ID,
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
    updated_ssh_data = ssh_data.copy()
    for update in ssh_update_map:
        source_data = sv_data.type_tableview(update['from_class']).reset_index(drop=True)

        # Merge with terminal, if needed
        if terminal_reference := [column_name if ".Terminal" in column_name else None for column_name in source_data.columns][0]:
            source_data = source_data.merge(terminals, left_on=terminal_reference, right_on='ID')
            logger.debug(f"Added Terminals to {update['from_class']}")

        updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(
            source_data.rename(columns={update['from_ID']: 'ID', update['from_attribute']: update['to_attribute']})[
                ['ID', update['to_attribute']]].set_index('ID').tableview_to_triplet(), add=False)

    # Generate new UUID for updated SSH
    updated_ssh_id_map = {}
    for OLD_ID in updated_ssh_data.query("KEY == 'Type' and VALUE == 'FullModel'").ID.unique():
        NEW_ID = str(uuid4())
        updated_ssh_id_map[OLD_ID] = NEW_ID
        logger.info(f"Assigned new UUID for updated SSH: {OLD_ID} -> {NEW_ID}")

    # Update SSH ID-s
    updated_ssh_data = updated_ssh_data.replace(updated_ssh_id_map)

    # Update in SV SSH references
    sv_data = sv_data.replace(updated_ssh_id_map)

    # Add SSH supersedes reference to old SSH
    ssh_supersedes_data = pandas.DataFrame(
        [{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
    ssh_supersedes_data['INSTANCE_ID'] = updated_ssh_data.query("KEY == 'Type'").merge(ssh_supersedes_data.ID)[
        'INSTANCE_ID']
    updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(ssh_supersedes_data)

    # Update SSH metadata
    updated_ssh_data = triplets.cgmes_tools.update_FullModel_from_dict(updated_ssh_data, {
        "Model.version": opdm_object_meta['pmd:versionNumber'],
        "Model.created": opdm_object_meta['pmd:creationDate'],
        "Model.mergingEntity": opdm_object_meta['pmd:mergingEntity'],
        "Model.domain": opdm_object_meta['pmd:mergingArea']
    })

    # Update SSH filenames
    filename_mask = "{scenarioTime:%Y%m%dT%H%MZ}_{processType}_{mergingEntity}-{domain}-{forEntity}_{messageType}_{version:03d}"
    updated_ssh_data = triplets.cgmes_tools.update_filename_from_FullModel(updated_ssh_data, filename_mask=filename_mask)
    return sv_data, updated_ssh_data

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

def export_to_cgmes_zip(triplets:list):

    namespace_map = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
        "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
        "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    }

    #with open('../../config/cgm_worker/CGMES_v2_4_15_2014_08_07.json', 'r') as file_object:
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
    else:
        logger.info(f"Models to be excluded: {excluded_models}")


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









