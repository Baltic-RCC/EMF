import pypowsybl
from helper import load_model, load_opdm_data, filename_from_metadata
from validator import validate_model
from emf.common.integrations.opdm import OPDM
from emf.loadflow_tool.scaler import query_hvdc_schedules, query_acnp_schedules, scale_balance
import sys
import os
import uuid
import triplets
import pandas
import datetime
from aniso8601 import parse_datetime
import tempfile

import logging

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

process_type_map = {
    "1D": "A01",
    "ID": "A18"
}


# Initialise connections
opdm_client = OPDM()

# Process setting #TODO - move to if main

time_horizon = '1D'
scenario_date = "2023-08-31T10:30"
area = "EU"
version = "101"

# 1.Query available IGM-s and latest BDS for given timestamp

latest_boundary = opdm_client.get_latest_boundary()
available_models = opdm_client.get_latest_models_and_download(time_horizon, scenario_date)

# 2.Validate each IGM

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

# Keep only valid models to save memory
del available_models
del invalid_models

# 3. Load all valid IGM-s to loadflowtool
merged_model = load_model(valid_models + [latest_boundary])

# 4. Generate EIC:AreaName Map
data = pandas.concat([load_opdm_data(valid_models, profile="EQ"), load_opdm_data([latest_boundary])], ignore_index=True)

# Get Control Area
CA = data.merge(data.query("VALUE == 'ControlArea'").ID).query('KEY == "IdentifiedObject.energyIdentCodeEic"')[['VALUE', 'INSTANCE_ID']]

# Get Atleast One subarea per Control Area
SGR = data.query("KEY == 'SubGeographicalRegion.Region'")[["VALUE", "INSTANCE_ID"]].drop_duplicates()

# Get Name from GR by merging with SGR
GR = data.merge(SGR, left_on='ID', right_on='VALUE', suffixes=('_GR', '_SGR')).query('KEY == "IdentifiedObject.name"')

# Generate mapping table
area_eic_map = CA.merge(GR, left_on='INSTANCE_ID', right_on='INSTANCE_ID_SGR')[['VALUE', 'VALUE_GR']].set_index('VALUE').to_dict()['VALUE_GR']

# Remove the raw data, to save memory
del data

# 5. Query Schedules from metadata storage for given timestamp
scenario_date_dt = datetime.datetime.fromisoformat(scenario_date)
utc_start = scenario_date_dt - datetime.timedelta(minutes=30)
utc_end = scenario_date_dt + datetime.timedelta(minutes=30)
dc_schedules = query_hvdc_schedules(process_type=process_type_map.get(time_horizon),
                                    utc_start=utc_start.isoformat(),
                                    utc_end=utc_end.isoformat(),
                                    area_eic_map=area_eic_map,
                                    )

ac_schedules = query_acnp_schedules(process_type=process_type_map.get(time_horizon),
                                    utc_start=utc_start.isoformat(),
                                    utc_end=utc_end.isoformat(),
                                    area_eic_map=area_eic_map,
                                    )

# 6. Perform Scaling using 4. Schedules on 3. Model
merged_model['NETWORK'] = scale_balance(network=merged_model['NETWORK'], ac_schedules=ac_schedules, dc_schedules=dc_schedules, debug=True)


# 7. Export Merged SV

# TODO - maybe make into function
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

# 8. Post Process (Fix SV Export, Generate updated SSH, Update Metadata)

# 9. Upload to OPDM (input OPDM object like items: Updated SSH and Merged SV)

