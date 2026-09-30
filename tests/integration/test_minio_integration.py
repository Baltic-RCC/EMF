import os
import uuid
from io import BytesIO
from unittest import mock

import pytest

from emf.common.integrations import minio_api

pytestmark = pytest.mark.integration

MINIO_SERVER = os.environ.get("MINIO_SERVER", "localhost:9000")
# ObjectStorage logs in with STS AssumeRoleWithLDAPIdentity, the test server has no LDAP, so its root user is used directly
ROOT_CREDENTIALS = {
    "AccessKeyId": os.environ.get("MINIO_USERNAME", "minioadmin"),
    "SecretAccessKey": os.environ.get("MINIO_PASSWORD", "minioadmin"),
    "SessionToken": None,
    "Expiration": "2099-01-01T00:00:00Z",
}
OBJECT_NAME = "IGM/20250102T0930Z-1D-AST-001.xml"
CONTENT = b"<model>AST</model>"


@pytest.fixture
def storage():
    """ObjectStorage against a MinIO served over HTTPS (self-signed certificate, ObjectStorage does not verify it)"""
    with mock.patch.object(minio_api.ObjectStorage, "_get_credentials", return_value=ROOT_CREDENTIALS):
        yield minio_api.ObjectStorage(server=MINIO_SERVER)


@pytest.fixture
def bucket(storage):
    name = f"emfos-test-{uuid.uuid4().hex[:8]}"
    storage.client.make_bucket(name)
    yield name
    for stored in storage.client.list_objects(name, recursive=True):
        storage.client.remove_object(name, stored.object_name)
    storage.client.remove_bucket(name)


def upload(storage, bucket, name: str = OBJECT_NAME, content: bytes = CONTENT, **kwargs):
    file_object = BytesIO(content)
    file_object.name = name
    return storage.upload_object(file_object, bucket_name=bucket, **kwargs)


def test_upload_object_stores_content_metadata_and_tags(storage, bucket):
    upload(storage, bucket, metadata={"bamessageid": "20250102T0930Z-1D-AST-001"}, tags={"tso": "AST", "time-horizon": "1D"})

    stat = storage.client.stat_object(bucket, OBJECT_NAME)
    assert storage.download_object(bucket, OBJECT_NAME) == CONTENT
    assert dict(storage.client.get_object_tags(bucket, OBJECT_NAME)) == {"tso": "AST", "time-horizon": "1D"}
    assert stat.metadata["x-amz-meta-bamessageid"] == "20250102T0930Z-1D-AST-001"
    assert stat.content_type == "application/xml"
    assert stat.size == len(CONTENT)


def test_download_object_normalizes_double_slashes(storage, bucket):
    upload(storage, bucket)

    assert storage.download_object(bucket, OBJECT_NAME.replace("/", "//")) == CONTENT


def test_download_object_returns_none_for_missing_object(storage, bucket):
    assert storage.download_object(bucket, "IGM/missing.xml") is None


@pytest.mark.parametrize("object_name, bucket_suffix, expected", [
    (OBJECT_NAME, "", True),
    ("IGM/missing.xml", "", False),
    (OBJECT_NAME, "-missing", False),
])
def test_object_exists(storage, bucket, object_name, bucket_suffix, expected):
    upload(storage, bucket)

    assert storage.object_exists(object_name=object_name, bucket_name=bucket + bucket_suffix) is expected


@pytest.mark.parametrize("query, use_regex, expected", [
    ("20250102T0930Z-1D-AST-001", False, [OBJECT_NAME]),
    ("20250102T0930Z-1D-(AST|ELERING)", True, [OBJECT_NAME]),
    ("20250102T0930Z-1D-ELERING-001", False, []),
])
def test_query_objects_filters_by_user_metadata(storage, bucket, query, use_regex, expected):
    upload(storage, bucket, metadata={"bamessageid": "20250102T0930Z-1D-AST-001"})
    upload(storage, bucket, name="IGM/other.xml", metadata={"bamessageid": "20250102T1030Z-1D-AST-001"})

    found = storage.query_objects(bucket_name=bucket, metadata={"bamessageid": query}, prefix="IGM", use_regex=use_regex)

    assert [stored.object_name for stored in found] == expected


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="upload_object sizes a file path upload with sys.getsizeof(file object), not the file length")
def test_upload_object_from_file_path_stores_whole_file(storage, bucket, tmp_path, monkeypatch):
    content = b"x" * 1_000_000
    (tmp_path / "model.xml").write_bytes(content)
    monkeypatch.chdir(tmp_path)

    storage.upload_object("model.xml", bucket_name=bucket)

    downloaded = storage.download_object(bucket, "model.xml")
    assert len(downloaded) == len(content)
    assert downloaded == content
