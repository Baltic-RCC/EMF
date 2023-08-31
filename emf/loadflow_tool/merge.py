import pypowsybl
from helper import load_model, load_opdm_data, filename_from_metadata, attr_to_dict
from validator import validate_model
import logging
import uuid
import json
import loadflow_settings
import sys
from emf.common.integrations.opdm import OPDM
from aniso8601 import parse_datetime
import tempfile
import os
import triplets
import pandas
import datetime
from uuid import uuid4

# Update SSH

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

opdm_client = OPDM()

time_horizon = '1D'
scenario_date = "2023-08-16T11:30"
area = "EU"
version = "104"

latest_boundary = opdm_client.get_latest_boundary()
available_models = opdm_client.get_latest_models_and_download(time_horizon, scenario_date)#, tso="ELERING")

valid_models = []
invalid_models = []

# Validate models
for model in available_models:

    try:
        response = validate_model([model, latest_boundary])
        model["VALIDATION_STATUS"] = response
        if response["VALID"]:
            valid_models.append(model)
        else:
            invalid_models.append(model)

    except:
        invalid_models.append(model)
        logger.error("Validation failed")

# Remove all available models to save memory
del available_models

merged_model = load_model(valid_models + [latest_boundary])

# Run LF
model_data = []
loadflow_report = pypowsybl.report.Reporter()
loadflow_result = pypowsybl.loadflow.run_ac(network=merged_model["NETWORK"],
                                            parameters=loadflow_settings.CGM_DEFAULT,
                                            reporter=loadflow_report)

loadflow_result_dict = [attr_to_dict(island) for island in loadflow_result]
#model_data["LOADFLOW_RESUTLS"] = loadflow_result_dict

#model_data["LOADFLOW_REPORT"] = json.loads(loadflow_report.to_json())
#model_data["LOADFLOW_REPORT_STR"] = str(loadflow_report)

SV_ID = merged_model['NETWORK_META']['id'].split("uuid:")[-1]
CGM_meta = {'opdm:OPDMObject': {'pmd:fullModel_ID': SV_ID,
                                'pmd:creationDate': f"{datetime.datetime.utcnow():%Y-%m-%dT%H:%M:%S.%fZ}",
                                'pmd:timeHorizon': time_horizon,
                                'pmd:cgmesProfile': 'SV',
                                'pmd:contentType': 'CGMES',
                                'pmd:modelPartReference': '',
                                'pmd:mergingEntity': 'BALTICRSC',
                                'pmd:mergingArea': area,
                                'pmd:validFrom': f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}",
                                'pmd:modelingAuthoritySet': 'http://www.baltic-rsc.eu/OperationalPlanning',
                                'pmd:scenarioDate': f"{parse_datetime(scenario_date):%Y-%m-%dT%H:%M:00Z}",
                                'pmd:modelid': SV_ID,
                                'pmd:description':
f"""<MDE>
    <BP>{time_horizon}</BP>
    <TOOL>pypowsybl_{pypowsybl.__version__}</TOOL>
    <RSC>BALTICRSC</RSC>
</MDE>""",
                                'pmd:versionNumber': version,
                                'file_type': "xml"}
            }

#temp_dir = tempfile.mkdtemp()
temp_dir = ""
export_file_path = os.path.join(temp_dir, f"MERGED_SV_{uuid.uuid4()}.zip")
logger.info(f"Exprting merged model to {export_file_path}")

export_report = pypowsybl.report.Reporter()
merged_model["NETWORK"].dump(export_file_path,
                           format="CGMES",
                           parameters={
                                "iidm.export.cgmes.modeling-authority-set": CGM_meta['opdm:OPDMObject']['pmd:modelingAuthoritySet'],
                                "iidm.export.cgmes.base-name": filename_from_metadata(CGM_meta['opdm:OPDMObject']).split("_SV")[0],
                                "iidm.export.cgmes.profiles": "SV",
                                "iidm.export.cgmes.naming-strategy": "cgmes",  # identity, cgmes, cgmes-fix-all-invalid-ids
                                       })


# Load SV data
sv_data = pandas.read_RDF([export_file_path])

# Update SV filename
sv_data.set_VALUE_at_KEY(key='label', value=filename_from_metadata(CGM_meta['opdm:OPDMObject']))

# Update SV description
sv_data.set_VALUE_at_KEY(key='Model.description', value=CGM_meta['opdm:OPDMObject']['pmd:description'])

# Update SV created time
sv_data.set_VALUE_at_KEY(key='Model.created', value=CGM_meta['opdm:OPDMObject']['pmd:creationDate'])

# Update SSH Model.scenarioTime
sv_data.set_VALUE_at_KEY('Model.scenarioTime', CGM_meta['opdm:OPDMObject']['pmd:scenarioDate'])

# Update SV metadata
sv_data = triplets.cgmes_tools.update_FullModel_from_filename(sv_data)


# Load original SSH data to created updated SSH
ssh_data = load_opdm_data(valid_models, "SSH")
ssh_data = triplets.cgmes_tools.update_FullModel_from_filename(ssh_data)

# Update SSH Model.scenarioTime
ssh_data.set_VALUE_at_KEY('Model.scenarioTime', CGM_meta['opdm:OPDMObject']['pmd:scenarioDate'])

# Load full original data to fix issues
data = load_opdm_data(valid_models + [latest_boundary])
terminals = data.type_tableview("Terminal")


# Update SSH data from SV
update_map =[
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
for update in update_map:
    source_data = sv_data.type_tableview(update['from_class']).reset_index(drop=True)

    # Merge with terminal, if needed
    if terminal_reference := [column_name if ".Terminal" in column_name else None for column_name in source_data.columns][0]:
        source_data = source_data.merge(terminals, left_on=terminal_reference, right_on='ID')
        logger.debug(f"Added Terminals to {update['from_class']}")

    updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(source_data.rename(columns={update['from_ID']: 'ID', update['from_attribute']: update['to_attribute']})[['ID', update['to_attribute']]].set_index('ID').tableview_to_triplet(), add=False)


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
ssh_supersedes_data = pandas.DataFrame([{"ID": item[1], "KEY": "Model.Supersedes", "VALUE": item[0]} for item in updated_ssh_id_map.items()])
ssh_supersedes_data['INSTANCE_ID'] = updated_ssh_data.query("KEY == 'Type'").merge(ssh_supersedes_data.ID)['INSTANCE_ID']
updated_ssh_data = updated_ssh_data.update_triplet_from_triplet(ssh_supersedes_data)

# Update SSH metadata
updated_ssh_data = triplets.cgmes_tools.update_FullModel_from_dict(updated_ssh_data, {
    "Model.version": CGM_meta['opdm:OPDMObject']['pmd:versionNumber'],
    "Model.created": CGM_meta['opdm:OPDMObject']['pmd:creationDate'],
    "Model.mergingEntity": CGM_meta['opdm:OPDMObject']['pmd:mergingEntity'],
    "Model.domain": CGM_meta['opdm:OPDMObject']['pmd:mergingArea']
})

# Update SSH filenames
filename_mask = "{scenarioTime:%Y%m%dT%H%MZ}_{processType}_{mergingEntity}-{domain}-{forEntity}_{messageType}_{version:03d}"
updated_ssh_data = triplets.cgmes_tools.update_filename_from_FullModel(updated_ssh_data, filename_mask=filename_mask)


# Update SV metadata
sv_data = triplets.cgmes_tools.update_FullModel_from_dict(sv_data, {"Model.version": CGM_meta['opdm:OPDMObject']['pmd:versionNumber'],
                                                                            "Model.created": CGM_meta['opdm:OPDMObject']['pmd:creationDate']})

# Fix SV - Remove Shunt Sections for EQV Shunts
equiv_shunt = data.query("KEY == 'Type' and VALUE == 'EquivalentShunt'")
if len(equiv_shunt) > 0:
    shunts_to_remove = sv_data.merge(sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").merge(equiv_shunt.ID, left_on='VALUE', right_on="ID", how='inner', suffixes=('', '_EQVShunt')).ID)
    if len(shunts_to_remove) > 0:
        logger.warning(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
        sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

# Fix SV - add missing SV Tap Steps

ssh_tap_steps = updated_ssh_data.query("KEY == 'TapChanger.step'")
sv_tap_steps = sv_data.query("KEY == 'SvTapStep.TapChanger'")

missing_sv_tap_steps = ssh_tap_steps.merge(sv_tap_steps[['VALUE']], left_on='ID', right_on="VALUE", how='left', indicator=True, suffixes=('', '_SV')).query("_merge == 'left_only'")

del ssh_tap_steps
del sv_tap_steps

tap_steps_to_be_added = []
SV_INSTANCE_ID = sv_data.INSTANCE_ID.iloc[0]
for tap_changer in missing_sv_tap_steps.itertuples():
    ID = str(uuid4())
    logger.warning(f'Missing SvTapStep for {tap_changer.ID}, adding SvTapStep {ID} and taking tap value {tap_changer.VALUE} from SSH')
    tap_steps_to_be_added.extend([
        (ID, 'Type', 'SvTapStep', SV_INSTANCE_ID),
        (ID, 'SvTapStep.TapChanger', tap_changer.ID, SV_INSTANCE_ID),
        (ID, 'SvTapStep.position', tap_changer.VALUE, SV_INSTANCE_ID),
    ])

sv_data = pandas.concat([sv_data, pandas.DataFrame(tap_steps_to_be_added, columns=['ID', 'KEY', 'VALUE', 'INSTANCE_ID'])], ignore_index=True)


# Fix SV - Sum flow into Topological node != 0
#SV_INJECTION_LIMIT = 0.1
#power_flow = sv_data.type_tableview('SvPowerFlow')
#flow_sum_at_topological_node = power_flow.merge(terminals, left_on='SvPowerFlow.Terminal', right_on='ID', how='left').groupby('Terminal.TopologicalNode')[['SvPowerFlow.p', 'SvPowerFlow.q']].sum()
#mismatch_at_topological_node = flow_sum_at_topological_node[(abs(flow_sum_at_topological_node['SvPowerFlow.p']) > SV_INJECTION_LIMIT) | (abs(flow_sum_at_topological_node['SvPowerFlow.q']) > SV_INJECTION_LIMIT)]
#mismatch_at_equipment = data.query('KEY == "Type"')[['ID', 'VALUE']].drop_duplicates().merge(mismatch_at_topological_node.merge(terminals.reset_index(), on='Terminal.TopologicalNode'), left_on="ID", right_on="Terminal.ConductingEquipment")

# Start Exporting data
namespace_map = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
    "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
    "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    #"cgmbp": "http://entsoe.eu/CIM/Extensions/CGM-BP/2020#"
}

with open('entsoe_v2.4.15_2014-08-07.json', 'r') as file_object:
    rdf_map = json.load(file_object)



export = pandas.concat([updated_ssh_data, sv_data], ignore_index=True).export_to_cimxml(rdf_map=rdf_map,
                          namespace_map=namespace_map,
                          export_undefined=False,
                          export_type="xml_per_instance_zip_per_xml",
                          debug=False,
                          export_to_memory=True)

publication_responses = []
for instance_file in export:

    logger.info(f"Publishing {instance_file.name} to OPDM")
    publication_response = opdm_client.publication_request(instance_file, "CGMES")

    publication_responses.append(
        {"name": instance_file.name,
         "response": publication_response}
    )