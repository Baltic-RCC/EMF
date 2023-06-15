import OPDM as opdm_api
import requests
import pandas as pd
import logging
import settings
import sys
import base64
import os

logger = logging.getLogger(__name__)

class OPDM:

    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password

        # Create connection to OPDM
        self.service = opdm_api.create_client(self.server, self.username, password=self.password)
        logger.info(f"Connection created to OPDM at {self.server} as {self.username}")

    def query_object(self, object_type, meta=None):

        logger.info(f"Sending query to OPDM for {object_type} with parameters {meta}")
        if meta is None:
            meta = {}

        query_id, raw_response = self.service.query_object(object_type, meta)
        response = raw_response['sm:QueryResult']['sm:part'][1:]

        if type(response) == str:
            response = []

        logger.info(f"Number of responses: {len(response)} for query {query_id}")

        return response

    def download_object(self, opdm_object, output_format='bytes', output_dir=None):

        if output_format == 'file':
            for cimxml_file in opdm_object['opdm:OPDMObject']['opde:Component']:
                file_id = cimxml_file['opdm:Profile']['opde:Id']
                file_name = cimxml_file['opdm:Profile']['pmd:fileName']
                file_path = os.path.join(output_dir, file_name)

                logger.info(f"Downloading {file_name} with ID -> {file_id}")
                response = self.service.get_content(file_id, return_payload=True)

                with open(file_path, 'wb') as file_object:
                    message64_bytes = response['sm:GetContentResult']['sm:part'][1]['opdm:Profile'][
                        'opde:Content'].encode()
                    file_object.write(base64.b64decode(message64_bytes))

                logger.info(f"Saved to {file_path}")

            return None

        if output_format == 'bytes':
            model_meta = opdm_object['opdm:OPDMObject']
            party = model_meta.get('pmd:modelPartReference',  model_meta.get('pmd:TSO', ''))

            for pos, model_part in enumerate(model_meta['opde:Component']):
                model_part_meta = model_part['opdm:Profile']
                model_part_name = model_part_meta['pmd:fileName']

                # Maybe the file is all ready there (if OPDM subscription is enabled)
                content_data = self.get_file(model_part_name)

                # If file is not available on local client, lets request it and download it
                if not content_data:
                    logger.warning("File not present on local client, requesting from OPDM service")
                    content_meta = self.service.get_content(model_part_meta['opde:Id'])
                    content_data = self.get_file(model_part_name)

                if not content_data:
                    logger.error(f"{model_part_name} not available on webdav")
                    opdm_object['opdm:OPDMObject']['opde:Component'][pos]['opdm:Profile']["DATA"] = None

                # Save data to metadata object
                opdm_object['opdm:OPDMObject']['opde:Component'][pos]['opdm:Profile']["DATA"] = content_data

            return opdm_object

    def get_file(self, file_id):

        logger.info(f"Retrieving file from OPDM local storage with ID -> {file_id}")
        auth = (settings.WEBDAV_USERNAME, settings.WEBDAV_PASSWORD)
        response = requests.request("GET",
                                    f"{settings.WEBDAV_SERVER}/{file_id}",
                                    verify=False,
                                    auth=auth)

        if response.status_code == 200:
            # logger.info(f"Retrieved file with ID -> {file_id}")
            return response.content
        else:
            logger.warning(f"Not available, file with ID -> {file_id}")
            logger.warning(f"Status Code {response.status_code}; Message: {response.content}")
            return None

    def get_latest_models_and_download(self, tso, time_horizon, scenario_date):

        models_metadata_raw = self.query_object(object_type="IGM", meta={'pmd:scenarioDate': scenario_date, 'pmd:timeHorizon': time_horizon, 'pmd:TSO': tso})

        models_downloaded = []
        if models_metadata_raw:
            # Sort for highest timeHorizon (for intraday) and for highest version
            models = pd.DataFrame([x['opdm:OPDMObject'] for x in models_metadata_raw])
            latest_models = models.sort_values(["pmd:timeHorizon", "pmd:versionNumber"], ascending=[True, False]).groupby("pmd:modelPartReference").first()

            for model in latest_models.to_dict("records"):
                try:
                    models_downloaded.append(self.download_object(opdm_object={'opdm:OPDMObject': model}))
                except:
                    logger.error(f"Could not download model for {time_horizon} {scenario_date} {model['pmd:TSO']}")
                    logger.error(sys.exc_info())
        else:
            logger.warning(f"Model for {tso} not available on OPDE")

        return models_downloaded

    def get_latest_boundary(self):

        # Query data from OPDM
        boundaries = self.query_object("BDS")

        # Convert to dataframe for sorting out the latest boundary
        boundary_data = pd.DataFrame([x['opdm:OPDMObject'] for x in boundaries])

        # Convert date and version to respective formats
        boundary_data['date_time'] = pd.to_datetime(boundary_data['pmd:scenarioDate'])
        boundary_data['version'] = pd.to_numeric(boundary_data['pmd:versionNumber'])

        # Sort out official boundary
        official_boundary_data = boundary_data[boundary_data["opde:Context"] == {'opde:IsOfficial': 'true'}]

        # Get the latest boundary meta
        latest_boundary_meta = boundaries[list(official_boundary_data.sort_values(["date_time", "version"], ascending=False).index)[0]]

        # Download the latest boundary
        return self.download_object(opdm_object=latest_boundary_meta)


if __name__ == '__main__':
    pass