import io
from unittest import mock

import pytest
import requests

from emf.common.integrations import opdm


@pytest.fixture
def service():
    service = opdm.OPDM.__new__(opdm.OPDM)
    service.query_object = mock.Mock()
    service.get_content = mock.Mock()
    return service


@pytest.fixture
def webdav():
    with mock.patch.object(opdm, "WEBDAV_SERVER", "https://webdav.test"), \
            mock.patch.object(opdm, "WEBDAV_SERVER_PUT", "https://webdav-put.test/"), \
            mock.patch.object(opdm, "WEBDAV_USERNAME", "user"), \
            mock.patch.object(opdm, "WEBDAV_PASSWORD", "secret"):
        yield


def test_query_returns_metadata_objects_after_result_header(service):
    service.query_object.return_value = {"sm:QueryResult": {"sm:part": ["h1", "h2", "h3", "h4", {"id": 1}, {"id": 2}]}}

    assert service.query("IGM") == [{"id": 1}, {"id": 2}]
    service.query_object.assert_called_once_with("IGM", {})


@pytest.mark.parametrize("raw_response", [
    {"sm:OperationFailure": {"sm:part": "Invalid query"}},
    {"sm:QueryResult": {"sm:part": "No results"}},
    {"sm:QueryResult": {"sm:part": ["h1", "h2", "h3", "h4"]}},
], ids=["operation-failure", "text-result", "header-only"])
def test_query_returns_empty_list_without_results(service, raw_response):
    service.query_object.return_value = raw_response

    assert service.query("IGM", {"pmd:TSO": "AST"}) == []


def _model(*file_names):
    return {"opde:Id": "model-1", "opde:Component": [{"opdm:Profile": {"pmd:fileName": name}} for name in file_names]}


def test_download_object_uses_local_storage_when_all_files_present(service):
    service.get_file = mock.Mock(side_effect=lambda name: f"data:{name}".encode())

    model = service.download_object(_model("EQ.zip", "SSH.zip"))

    assert [c["opdm:Profile"]["DATA"] for c in model["opde:Component"]] == [b"data:EQ.zip", b"data:SSH.zip"]
    service.get_content.assert_not_called()


def test_download_object_requests_content_from_opdm_for_missing_files(service):
    local_files = {"EQ.zip": [b"eq"], "SSH.zip": [None, b"ssh"]}
    service.get_file = mock.Mock(side_effect=lambda name: local_files[name].pop(0))

    model = service.download_object(_model("EQ.zip", "SSH.zip"))

    service.get_content.assert_called_once_with("model-1", object_type="model")
    assert [c["opdm:Profile"]["DATA"] for c in model["opde:Component"]] == [b"eq", b"ssh"]
    assert [c.args[0] for c in service.get_file.call_args_list] == ["EQ.zip", "SSH.zip", "SSH.zip"]


def test_download_object_raises_when_file_still_missing_after_get_content(service):
    service.get_file = mock.Mock(return_value=None)

    with pytest.raises(Exception, match="Failure in model retrieving"):
        service.download_object(_model("EQ.zip"))


@pytest.mark.parametrize("status_code, expected", [(200, b"content"), (404, None)])
def test_get_file_reads_from_webdav(service, webdav, status_code, expected):
    with mock.patch.object(opdm.requests, "request", return_value=mock.Mock(status_code=status_code, content=b"content")) as request:
        assert service.get_file("EQ.zip") == expected

    request.assert_called_once_with("GET", "https://webdav.test/EQ.zip", verify=False, auth=("user", "secret"))


@pytest.mark.parametrize("content, expected_body", [(b"payload", b"payload"), (io.BytesIO(b"payload"), b"payload")],
                         ids=["bytes", "file-object"])
def test_put_file_uploads_with_content_length(service, webdav, content, expected_body):
    with mock.patch.object(opdm.requests, "put", return_value=mock.Mock(status_code=201)) as put:
        assert service.put_file("/folder/EQ.zip", content) is True

    assert put.call_args.args == ("https://webdav-put.test/folder/EQ.zip",)
    assert put.call_args.kwargs["data"] == expected_body
    assert put.call_args.kwargs["headers"]["Content-Length"] == "7"


@pytest.mark.parametrize("outcome", [{"return_value": mock.Mock(status_code=500, content=b"error")},
                                     {"side_effect": requests.ConnectionError("refused")}], ids=["http-error", "exception"])
def test_put_file_returns_false_when_upload_fails(service, webdav, outcome):
    with mock.patch.object(opdm.requests, "put", **outcome):
        assert service.put_file("EQ.zip", b"payload") is False


def test_put_file_rejects_unsupported_content(service, webdav):
    with mock.patch.object(opdm.requests, "put") as put:
        assert service.put_file("EQ.zip", 123) is False
    put.assert_not_called()


def test_get_latest_boundary_downloads_latest_official_boundary(service):
    def boundary(date, version, official="true"):
        return {"opdm:OPDMObject": {"opde:Id": f"{date}-{version}", "pmd:scenarioDate": date, "pmd:versionNumber": version,
                                    "opde:Context": {"opde:IsOfficial": official}}}

    service.query = mock.Mock(return_value=[boundary("2024-01-01T00:00:00Z", "003"), boundary("2024-02-01T00:00:00Z", "001"),
                                            boundary("2024-02-01T00:00:00Z", "002"), boundary("2024-03-01T00:00:00Z", "001", "false")])
    service.download_object = mock.Mock(side_effect=lambda opdm_object: opdm_object)

    assert service.get_latest_boundary()["opde:Id"] == "2024-02-01T00:00:00Z-002"
    service.query.assert_called_once_with("BDS")
