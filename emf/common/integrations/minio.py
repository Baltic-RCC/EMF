import requests
from lxml import etree
import minio
import urllib3
import sys
import mimetypes
import re
import logging
import config
from datetime import datetime
from aniso8601 import parse_duration, parse_datetime
from emf.common.config_parser import parse_app_properties
urllib3.disable_warnings()

logger = logging.getLogger(__name__)

parse_app_properties(globals(), config.paths.integrations.minio)


class ObjectStorage:

    def __init__(self, server=MINIO_SERVER, username=MINIO_USERNAME, password=MINIO_PASSWORD):
        self.server = server
        self.username = username
        self.password = password
        self.token_expiration = datetime.utcnow()
        self.http_client = urllib3.PoolManager(
                cert_reqs='CERT_NONE',
                #cert_reqs='CERT_REQUIRED',
                #ca_certs='/usr/local/share/ca-certificates/CA-Bundle.crt'
            )

        # Init client
        self.__create_client()

    def __create_client(self):

        if self.token_expiration < (datetime.utcnow() + parse_duration("PT1M")):
            credentials = self.__get_credentials()

            self.token_expiration = parse_datetime(credentials['Expiration']).replace(tzinfo=None)
            self.client = minio.Minio(endpoint=self.server,
                                      access_key=credentials['AccessKeyId'],
                                      secret_key=credentials['SecretAccessKey'],
                                      session_token=credentials['SessionToken'],
                                      secure=True,
                                      http_client=self.http_client,
                                      )

    def __get_credentials(self, action="AssumeRoleWithLDAPIdentity", version="2011-06-15"):
        """
        Method to get temporary credentials for LDAP user
        :param action: string of action
        :param version: version
        :return:
        """
        # Define LDAP service user parameters
        params = {
            "Action": action,
            "LDAPUsername": self.username,
            "LDAPPassword": self.password,
            "Version": version,
            "DurationSeconds": TOKEN_EXPIRATION,
        }

        # Sending request for temporary credentials and parsing it out from returned xml
        response = requests.post(f"https://{self.server}", params=params, verify=False).content
        credentials = {}
        root = etree.fromstring(response)
        et = root.find("{*}AssumeRoleWithLDAPIdentityResult/{*}Credentials")
        for element in et:
            _, _, tag = element.tag.rpartition("}")
            credentials[tag] = element.text

        return credentials

    def upload_object(self, file_path_or_file_object, bucket_name, metadata=None):
        """
        Method to upload file to Minio storage
        :param file_path_or_file_object: file path or BytesIO object
        :param bucket_name: bucket name
        :param metadata: object metadata
        :return: response from Minio
        """
        file_object = file_path_or_file_object

        if type(file_path_or_file_object) == str:
            file_object = open(file_path_or_file_object, "rb")
            length = sys.getsizeof(file_object)
        else:
            length = file_object.getbuffer().nbytes

        # Just to be sure that pointer is at the beginning of the content
        file_object.seek(0)

        # TODO - check that bucket exists and it has access to it, maybe also try to create one

        response = self.client.put_object(
            bucket_name=bucket_name,
            object_name=file_object.name,
            data=file_object,
            length=length,
            content_type=mimetypes.guess_type(file_object.name)[0],
            metadata=metadata
        )

        return response

    def download_object(self, bucket_name, object_name):
        try:
            file_data = self.client.get_object(bucket_name, object_name)
            return file_data.read()

        except minio.error.S3Error as err:
            print(err)

    def object_exists(self, object_name, bucket_name):

        # TODO - add description
        # TODO - add logging

        exists = False
        try:
            self.client.stat_object(bucket_name, object_name)
            exists = True
        except minio.error.S3Error as e:
            pass

        return exists

    def list_objects(self, bucket_name, prefix=None, recursive=False, start_after=None, include_user_meta=True, include_version=False):

        try:
            objects = self.client.list_objects(bucket_name, prefix, recursive, start_after, include_user_meta, include_version)
            return objects
        except minio.error.S3Error as err:
            print(err)

    def query_objects(self, bucket_name: str, metadata: dict = None, prefix: str = None, use_regex: bool = False):

        """Example: service.query_objects(prefix="BRELL", metadata={'bamessageid': '20230215T1630Z-2D-RUSSIA-001'})"""

        objects = self.client.list_objects(bucket_name, prefix, recursive=True, include_user_meta=True)

        if not metadata:
            return objects

        result_list = []
        for object in objects:
            object_metadata = self.client.stat_object(bucket_name, object.object_name).metadata

            meta_match = True
            for query_key, query_value in metadata.items():
                meta_value = object_metadata.get(f"x-amz-meta-{query_key}")
                meta_match = meta_value != query_value

                if use_regex:
                    meta_match = re.search(pattern=query_value, string=meta_value)

            if meta_match:
                result_list.append(object)

        return result_list


if __name__ == '__main__':
    # Test Minio API
    service = ObjectStorage()
    buckets = service.client.list_buckets()
    print(buckets)
