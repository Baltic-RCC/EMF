import pypowsybl
from helper import attr_to_dict, load_model
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
import zipfile
from lxml import etree
from uuid import uuid4

from io import BytesIO
# Update SSH
def opdmprofile_to_bytes(opdm_profile):
    data = BytesIO(opdm_profile['opdm:Profile']['DATA'])
    data.name = opdm_profile['opdm:Profile']['pmd:fileName']
    return data

def load_opdm_data(opdm_objects, profile=None):
    if profile:
        return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opdm:OPDMObject']['opde:Component'] if instance['opdm:Profile']['pmd:cgmesProfile'] == profile])
    return pandas.read_RDF([opdmprofile_to_bytes(instance) for model in opdm_objects for instance in model['opdm:OPDMObject']['opde:Component']])

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

meta_separator = "_"


def metadata_from_filename(file_name):

    file_metadata = {} # Meta container

    file_name, file_metadata["file_type"] = file_name.split(".")
    meta_list = file_name.split(meta_separator)

    if len(meta_list) == 4:   #try: #if "_EQ_" in file_name or "_BD_" in file_name:

        file_metadata['pmd:validFrom'], model_authority, file_metadata['pmd:cgmesProfile'], file_metadata['pmd:versionNumber'] = meta_list
        file_metadata['pmd:timeHorizon'] = ""

    elif len(meta_list) == 5:

        file_metadata['pmd:validFrom'], file_metadata['pmd:timeHorizon'], model_authority, file_metadata['pmd:cgmesProfile'], file_metadata['pmd:versionNumber'] = meta_list

    else:
        print("Parsing error, number of allowed meta in filename is 4 or 5 separated by '_' -> {} ".format(file_name))

    model_authority_list = model_authority.split("-")

    if len(model_authority_list) == 1:
        file_metadata['pmd:modelPartReference'] = model_authority

    elif len(model_authority_list) == 2:
        file_metadata['pmd:mergingEntity'], file_metadata['pmd:mergingArea'] = model_authority_list

    elif len(model_authority_list) == 3:
        file_metadata['pmd:mergingEntity'], file_metadata['pmd:mergingArea'], file_metadata['pmd:modelPartReference'] = model_authority_list

    else:
        print(f"Parsing error {model_authority}")

    return file_metadata


def filename_from_metadata(metadata):

    model_part = metadata.get('pmd:modelPartReference', None)

    if model_part:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}-{model_part}"

    else:
        model_authority = f"{metadata['pmd:mergingEntity']}-{metadata['pmd:mergingArea']}"

    file_name = f"{metadata['pmd:validFrom']}_{metadata['pmd:timeHorizon']}_{model_authority}_{metadata['pmd:cgmesProfile']}_{metadata['pmd:versionNumber']}"
    file_name = ".".join([file_name, metadata["file_type"]])

    return file_name


def get_xml_from_zip(zip_file_path):

    zipfile_object    = zipfile.ZipFile(zip_file_path)
    xml_file_name     = zipfile_object.namelist()[0]
    file_unzipped     = zipfile_object.open(xml_file_name, mode="r")
    xml_tree_object   = etree.parse(file_unzipped)

    return xml_tree_object

def zip_xml_file(xml_etree_object, file_metadata, destination_bath):

    # Get meta and path
    file_metadata["file_type"] = "zip"
    zip_file_name = filename_from_metadata(file_metadata)

    file_metadata["file_type"] = "xml"
    xml_file_name = filename_from_metadata(file_metadata)

    zip_file_path = os.path.join(destination_bath, zip_file_name)

    # Create and save ZIP
    out_zipped_file = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    out_zipped_file.writestr(xml_file_name, etree.tostring(xml_etree_object))#, pretty_print=True))
    out_zipped_file.close()

    return zip_file_path


def get_metadata_from_xml(parsed_xml):
    """Parse model metadata form xml, retruns a dictionary"""
    #parsed_xml = etree.parse(filepath_or_fileobject)

    header = parsed_xml.find("{*}FullModel")
    meta_elements = header.getchildren()

    # Add model ID
    meta_dict = {"mRID":header.attrib.values()[0].split(":")[-1]}

    # Add all other metadata
    for element in meta_elements:
        if element.text:
            meta_dict[element.tag.split("}")[1]] = element.text
        else:
            meta_dict[element.tag.split("}")[1]] = element.attrib.values()[0]

    return meta_dict


def get_metadata_from_filename(file_name):

    # Separators
    file_type_separator           = "."
    meta_separator                = "_"
    entity_and_domain_separator   = "-"

    #print(file_name)
    file_metadata = {}
    file_name, file_type = file_name.split(file_type_separator)

    # Parse file metadata
    file_meta_list = file_name.split(meta_separator)

    # Naming before QoDC 2.1, where EQ might not have processType
    if len(file_meta_list) == 4:

        file_metadata["Model.scenarioTime"],\
        file_metadata["Model.modelingEntity"],\
        file_metadata["Model.messageType"],\
        file_metadata["Model.version"] = file_meta_list
        file_metadata["Model.processType"] = ""

        print("Warning - only 4 meta elements found, expecting 5, setting Model.processType to empty string")

    # Naming after QoDC 2.1, always 5 positions
    elif len(file_meta_list) == 5:

        file_metadata["Model.scenarioTime"],\
        file_metadata["Model.processType"],\
        file_metadata["Model.modelingEntity"],\
        file_metadata["Model.messageType"],\
        file_metadata["Model.version"] = file_meta_list

    else:
        print("Non CGMES file {}".format(file_name))

    if file_metadata.get("Model.modelingEntity", False):

        entity_and_area_list = file_metadata["Model.modelingEntity"].split(entity_and_domain_separator)

        if len(entity_and_area_list) == 1:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"] = "", "" # Set empty string for both
            file_metadata["Model.forEntity"] = entity_and_area_list[0]

        if len(entity_and_area_list) == 2:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"] = entity_and_area_list
            file_metadata["Model.forEntity"] = ""

        if len(entity_and_area_list) == 3:
            file_metadata["Model.mergingEntity"],\
            file_metadata["Model.domain"],\
            file_metadata["Model.forEntity"] = entity_and_area_list

    return file_metadata

opdm_client = OPDM()

time_horizon = '1D'
scenario_date = "2023-08-16T10:30"
area = "EU"
version = "101"

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

export_settings = {
    "iidm.export.cgmes.base-name": "",
    "iidm.export.cgmes.cim-version": "",  # 14, 16, 100
    "iidm.export.cgmes.export-boundary-power-flows": "true",
    "iidm.export.cgmes.export-power-flows-for-switches": "true",
    "iidm.export.cgmes.naming-strategy": "identity",  # identity, cgmes, cgmes-fix-all-invalid-ids
    "iidm.export.cgmes.profiles": "EQ,TP,SSH,SV",
    "iidm.export.cgmes.boundary-EQ-identifier": "",
    "iidm.export.cgmes.boundary-TP-identifier": "",
    "iidm.export.cgmes.modeling-authority-set": "powsybl.org"
}

merged_model = load_model(valid_models + [latest_boundary])

SV_ID = merged_model['NETWORK_META']['id'].split("uuid:")[-1]
CGM_meta = {'opdm:OPDMObject': {'pmd:fullModel_ID': SV_ID,
                                'pmd:creationDate': f"{datetime.datetime.utcnow():%Y-%m-%dT%H:%M:%SZ}",
                                'pmd:timeHorizon': time_horizon,
                                'pmd:cgmesProfile': 'SV',
                                'pmd:contentType': 'CGMES',
                                'pmd:modelPartReference': '',
                                'pmd:mergingEntity': 'BALTICRSC',
                                'pmd:mergingArea': area,
                                'pmd:validFrom': f"{parse_datetime(scenario_date):%Y%m%dT%H%MZ}",
                                'pmd:modelingAuthoritySet': 'http://www.baltic-rsc.eu/OperationalPlanning',
                                'pmd:scenarioDate': scenario_date,
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
current_name = sv_data.query("KEY == 'label'")
sv_data.iloc[current_name.index[0], current_name.columns.get_loc("VALUE")] = filename_from_metadata(CGM_meta['opdm:OPDMObject'])

# Update SV description
current_description = sv_data.query("KEY == 'Model.description'")
sv_data.iloc[current_description.index[0], current_description.columns.get_loc("VALUE")] = CGM_meta['opdm:OPDMObject']['pmd:description']
# Update SV metadata
sv_data = triplets.cgmes_tools.update_FullModel_from_filename(sv_data)



# Load original SSH data
ssh_data = load_opdm_data(valid_models, "SSH")
ssh_data = triplets.cgmes_tools.update_FullModel_from_filename(ssh_data)

# Get only terminal from original EQ, needed for some elements updating
eq_data = load_opdm_data(valid_models, "EQ")
terminals = eq_data.type_tableview("Terminal")
equiv_shunt = eq_data.query("KEY == 'Type' and VALUE == 'EquivalentShunt'")

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


# Export updated SSH
updated_ssh_data.export_to_cimxml(rdf_map=rdf_map,
                                  namespace_map=namespace_map,
                                  export_undefined=False,
                                  export_type="xml_per_instance_zip_per_xml",
                                  global_zip_filename="Export.zip",
                                  debug=False,
                                  export_to_memory=False)

# Update SV metadata
sv_data = triplets.cgmes_tools.update_FullModel_from_dict(sv_data, {"Model.version": CGM_meta['opdm:OPDMObject']['pmd:versionNumber'],
                                                                            "Model.created": CGM_meta['opdm:OPDMObject']['pmd:creationDate']})

# Fix SV - Remove Shunt sections for EQV Shunts
if len(equiv_shunt) > 0:
    shunts_to_remove = sv_data.merge(sv_data.query("KEY == 'SvShuntCompensatorSections.ShuntCompensator'").merge(equiv_shunt.ID, left_on='VALUE', right_on="ID", how='inner', suffixes=('', '_EQVShunt')).ID)
if len(shunts_to_remove) > 0:
    logger.warning(f'Removing invalid SvShuntCompensatorSections for EquivalentShunt')
    sv_data = triplets.rdf_parser.remove_triplet_from_triplet(sv_data, shunts_to_remove)

# Fix missing SV Tap Steps - add missing steps

ssh_tap_steps = ssh_data.query("KEY == 'TapChanger.step'")
sv_tap_steps = sv_data.query("KEY == 'SvTapStep.TapChanger'")

missing_sv_tap_steps = ssh_tap_steps.merge(sv_tap_steps[['VALUE']], left_on='ID', right_on="VALUE", how='left', indicator=True, suffixes=('', '_SV')).query("_merge == 'left_only'")

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


sv_data.export_to_cimxml(rdf_map=rdf_map,
                          namespace_map=namespace_map,
                          export_undefined=False,
                          export_type="xml_per_instance_zip_per_xml",
                          global_zip_filename="Export.zip",
                          debug=False,
                          export_to_memory=False)

