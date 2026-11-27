import io
import types
import zipfile
from datetime import datetime, timedelta
from unittest import mock

import minio
import pytest
import requests
import urllib3
from minio.commonconfig import Tags

from emf.common.integrations import minio_api

STS_SUCCESS = b"""<AssumeRoleWithLDAPIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithLDAPIdentityResult>
    <Credentials>
      <AccessKeyId>ACCESS</AccessKeyId>
      <SecretAccessKey>SECRET</SecretAccessKey>
      <Expiration>2030-01-01T10:00:00Z</Expiration>
      <SessionToken>TOKEN</SessionToken>
    </Credentials>
  </AssumeRoleWithLDAPIdentityResult>
  <ResponseMetadata><RequestId>REQ1</RequestId></ResponseMetadata>
</AssumeRoleWithLDAPIdentityResponse>"""

STS_ERROR = b"""<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error><Type></Type><Code>InvalidParameterValue</Code><Message>LDAP login failed</Message></Error>
  <RequestId>REQ2</RequestId>
</ErrorResponse>"""


def _http_response(content, status_code=200):
    return mock.Mock(content=content, status_code=status_code)


def _s3_error():
    return minio.error.S3Error("NoSuchKey", "missing", "resource", "request", "host", mock.Mock())


@pytest.fixture
def sts_post(real_minio_login):
    with mock.patch.object(minio_api.requests, "post") as post, mock.patch.object(minio_api, "time") as patched_time:
        post.sleeps = patched_time.sleep
        yield post


def test_login_parses_sts_credentials(sts_post):
    sts_post.return_value = _http_response(STS_SUCCESS)

    storage = minio_api.ObjectStorage(server="minio.test", username="user", password="secret")

    assert storage.token_expiration == datetime(2030, 1, 1, 10, 0)
    assert sts_post.call_args.args == ("https://minio.test",)
    params = sts_post.call_args.kwargs["params"]
    assert (params["Action"], params["LDAPUsername"], params["LDAPPassword"]) == ("AssumeRoleWithLDAPIdentity", "user", "secret")
    assert storage._get_credentials() == {"AccessKeyId": "ACCESS", "SecretAccessKey": "SECRET",
                                          "Expiration": "2030-01-01T10:00:00Z", "SessionToken": "TOKEN"}


def test_login_retries_after_sts_error(sts_post):
    sts_post.side_effect = [_http_response(STS_ERROR, 400), _http_response(STS_SUCCESS)]

    storage = minio_api.ObjectStorage(server="minio.test")

    assert storage.token_expiration == datetime(2030, 1, 1, 10, 0)
    assert sts_post.call_count == 2
    sts_post.sleeps.assert_called_once_with(int(minio_api.SLEEP_DURATION))


@pytest.mark.parametrize("response, message", [
    (_http_response(STS_ERROR, 400), "STS error: Code=InvalidParameterValue, Message=LDAP login failed, RequestId=REQ2"),
    (_http_response(b"<Unavailable/>", 503), "HTTP error: status=503"),
])
def test_login_gives_up_after_three_failed_attempts(sts_post, response, message):
    sts_post.return_value = response

    with pytest.raises(RuntimeError, match=message):
        minio_api.ObjectStorage(server="minio.test")

    assert sts_post.call_count == 3
    assert sts_post.sleeps.call_count == 2


@pytest.mark.parametrize("outcome, message", [
    ({"side_effect": requests.ConnectionError("refused")}, "Request failed: refused"),
    ({"return_value": _http_response(b"<html>", 502)}, "Failed to parse XML response"),
    ({"return_value": _http_response(b"<AssumeRoleWithLDAPIdentityResponse/>")}, "Credentials not found"),
])
def test_login_fails_without_retry_on_unusable_response(sts_post, outcome, message):
    for key, value in outcome.items():
        setattr(sts_post, key, value)

    with pytest.raises(RuntimeError, match=message):
        minio_api.ObjectStorage(server="minio.test")

    assert sts_post.call_count == 1


@pytest.fixture
def storage():
    storage = minio_api.ObjectStorage(server="minio.test")
    storage.client = mock.MagicMock()
    return storage


def _fixed_now(now):
    class FixedDatetime(datetime):
        @classmethod
        def utcnow(cls):
            return now
    return mock.patch.object(minio_api, "datetime", FixedDatetime)


@pytest.mark.parametrize("seconds_to_expiry, renewed", [(3600, False), (121, False), (120, True), (30, True), (-60, True)])
def test_token_is_renewed_shortly_before_expiry(storage, seconds_to_expiry, renewed):
    now = datetime(2024, 5, 1, 12, 0, 0)
    storage.token_expiration = now + timedelta(seconds=seconds_to_expiry)
    old_client = storage.client

    with _fixed_now(now), mock.patch.object(minio_api.minio, "Minio") as new_client:
        storage.object_exists("object", "bucket")

    used_client = new_client.return_value if renewed else old_client
    assert storage.client is used_client
    used_client.stat_object.assert_called_once_with("bucket", "object")
    if renewed:
        assert storage.token_expiration == datetime(2099, 1, 1)


def test_upload_object_from_file_object(storage):
    data = io.BytesIO(b"zip content")
    data.name = "IGM/model.zip"
    data.seek(0, io.SEEK_END)
    positions = []
    storage.client.put_object.side_effect = lambda **kwargs: positions.append(kwargs["data"].tell())

    storage.upload_object(data, "bucket", metadata={"bamessageid": "id"}, tags={"tso": "AST"})

    kwargs = storage.client.put_object.call_args.kwargs
    assert (kwargs["bucket_name"], kwargs["object_name"], kwargs["length"]) == ("bucket", "IGM/model.zip", 11)
    assert kwargs["data"] is data and positions == [0]
    assert kwargs["content_type"] == "application/zip"
    assert kwargs["metadata"] == {"bamessageid": "id"}
    assert isinstance(kwargs["tags"], Tags) and dict(kwargs["tags"]) == {"tso": "AST"}


def test_upload_object_without_tags(storage):
    data = io.BytesIO(b"x")
    data.name = "a.xml"

    storage.upload_object(data, "bucket")

    assert storage.client.put_object.call_args.kwargs["tags"] is None


def test_upload_object_from_path_uploads_file_content(storage, tmp_path):
    path = tmp_path / "model.xml"
    path.write_bytes(b"<rdf/>")

    storage.upload_object(str(path), "bucket")

    kwargs = storage.client.put_object.call_args.kwargs
    assert kwargs["data"].read() == b"<rdf/>"
    assert kwargs["content_type"] == "application/xml"
    kwargs["data"].close()


@pytest.mark.xfail(strict=True, reason="upload_object from a path uses sys.getsizeof(file object) as length instead of the file size")
def test_upload_object_from_path_uses_file_size_as_length(storage, tmp_path):
    path = tmp_path / "model.xml"
    path.write_bytes(b"<rdf/>")

    storage.upload_object(str(path), "bucket")

    storage.client.put_object.call_args.kwargs["data"].close()
    assert storage.client.put_object.call_args.kwargs["length"] == 6


def test_download_object_returns_content_and_releases_connection(storage):
    file_data = storage.client.get_object.return_value
    file_data.read.return_value = b"content"

    assert storage.download_object("bucket", "IGM//model.zip") == b"content"

    storage.client.get_object.assert_called_once_with("bucket", "IGM/model.zip")
    file_data.close.assert_called_once()
    file_data.release_conn.assert_called_once()


def test_download_object_releases_connection_when_read_fails(storage):
    file_data = storage.client.get_object.return_value
    file_data.read.side_effect = urllib3.exceptions.ProtocolError("connection reset")

    with pytest.raises(urllib3.exceptions.ProtocolError):
        storage.download_object("bucket", "model.zip")

    file_data.release_conn.assert_called_once()


def test_download_object_returns_none_when_object_missing(storage, caplog):
    storage.client.get_object.side_effect = _s3_error()

    assert storage.download_object("bucket", "model.zip") is None
    assert "Failed to download object 'model.zip'" in caplog.text


@pytest.mark.parametrize("stat_outcome, exists", [({"return_value": mock.Mock()}, True), ({"side_effect": _s3_error()}, False)])
def test_object_exists(storage, stat_outcome, exists):
    storage.client.stat_object.configure_mock(**stat_outcome)

    assert storage.object_exists("model.zip", "bucket") is exists


def _objects_with_metadata(storage, metadata_by_name):
    objects = [types.SimpleNamespace(object_name=name) for name in metadata_by_name]
    storage.client.list_objects.return_value = iter(objects)
    storage.client.stat_object.side_effect = lambda bucket, name: mock.Mock(metadata=metadata_by_name[name])
    return objects


def test_query_objects_without_metadata_returns_listing(storage):
    listing = storage.query_objects("bucket", prefix="IGM")

    assert listing is storage.client.list_objects.return_value
    storage.client.list_objects.assert_called_once_with("bucket", "IGM", recursive=True, include_user_meta=True)


@pytest.mark.parametrize("query, use_regex, expected", [
    ({"bamessageid": "20240101T0030Z-1D-AST-001"}, False, ["a"]),
    ({"bamessageid": "20240101T0030Z-1D-(AST|PSE)"}, True, ["a", "b"]),
    ({"bamessageid": "20240101T0030Z-1D-(AST|PSE)"}, False, []),
    ({"bamessageid": "20240101T0030Z-1D-AST-001", "tso": "AST"}, False, ["a"]),
    ({"bamessageid": "20240101T0030Z-1D-AST-001", "tso": "PSE"}, False, []),
])
def test_query_objects_filters_by_metadata(storage, query, use_regex, expected):
    _objects_with_metadata(storage, {
        "a": {"x-amz-meta-bamessageid": "20240101T0030Z-1D-AST-001", "x-amz-meta-tso": "AST"},
        "b": {"x-amz-meta-bamessageid": "20240101T0030Z-1D-PSE-002"},
        "c": {"x-amz-meta-bamessageid": "20240101T0030Z-2D-AST-001"},
    })

    result = storage.query_objects("bucket", metadata=query, use_regex=use_regex)

    assert [item.object_name for item in result] == expected


@pytest.mark.xfail(strict=True, reason="query_objects reuses the previous object's regex hit for an object without the metadata key")
def test_query_objects_regex_skips_objects_without_metadata_key(storage):
    _objects_with_metadata(storage, {"with-meta": {"x-amz-meta-bamessageid": "20240101T0030Z-1D-AST-001"}, "without-meta": {}})

    result = storage.query_objects("bucket", metadata={"bamessageid": "AST"}, use_regex=True)

    assert [item.object_name for item in result] == ["with-meta"]


def _zipped_model(*file_names):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for file_name in file_names:
            archive.writestr(file_name, f"<{file_name}/>")
    return buffer.getvalue()


@pytest.mark.parametrize("time_horizon, expected_pattern", [
    ("1D", "20240820T1530Z-1D-(AST|ELERING)"),
    ("ID", "20240820T1530Z-(0[0-9]|1[0-9]|2[0-9]|3[0-6])-(AST|ELERING)"),
])
def test_get_latest_models_queries_by_message_id_pattern(storage, time_horizon, expected_pattern):
    with mock.patch.object(storage, "query_objects", return_value=[]) as query_objects:
        assert storage.get_latest_models_and_download(time_horizon, "2024-08-20T15:30:00Z", ["AST", "ELERING"],
                                                      bucket_name="models", prefix="IGM") == []

    query_objects.assert_called_once_with(bucket_name="models", prefix="IGM", metadata={"bamessageid": expected_pattern},
                                          use_regex=True)


def test_get_latest_models_downloads_latest_version_per_tso(storage):
    names = ["20240820T1530Z-1D-AST-001", "20240820T1530Z-1D-AST-002", "20240820T1530Z-1D-ELERING-001"]
    objects = [types.SimpleNamespace(object_name=f"IGM/{name}.zip", metadata={"X-Amz-Meta-Bamessageid": name}) for name in names]
    downloads = {f"IGM/{name}.zip": _zipped_model(f"20240820T1530Z_1D_{name.split('-')[2]}_EQ_{name[-3:]}.xml") for name in names}

    with mock.patch.object(storage, "query_objects", return_value=objects), \
            mock.patch.object(storage, "download_object", side_effect=lambda bucket_name, object_name: downloads[object_name]):
        models = storage.get_latest_models_and_download("1D", "2024-08-20T15:30:00Z", ["AST", "ELERING"],
                                                        bucket_name="models", prefix="IGM")

    assert sorted((model["pmd:TSO"], model["pmd:content-reference"]) for model in models) == [
        ("AST", "IGM/20240820T1530Z-1D-AST-002.zip"), ("ELERING", "IGM/20240820T1530Z-1D-ELERING-001.zip")]
    ast_profile = next(model for model in models if model["pmd:TSO"] == "AST")["opde:Component"][0]["opdm:Profile"]
    assert ast_profile["pmd:fileName"] == "20240820T1530Z_1D_AST_EQ_002.xml"
    assert ast_profile["DATA"] == b"<20240820T1530Z_1D_AST_EQ_002.xml/>"
    assert (ast_profile["pmd:cgmesProfile"], ast_profile["pmd:versionNumber"]) == ("EQ", "002")
