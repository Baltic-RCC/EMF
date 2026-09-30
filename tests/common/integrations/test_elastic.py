import datetime
import json
import types
import uuid
from unittest import mock

import pandas as pd
import pytest

from emf.common.integrations import elastic

SERVER = "http://elk.test:9200"


class FixedDatetime(datetime.datetime):
    @classmethod
    def utcnow(cls):
        return cls(2024, 3, 5, 10, 15, 0)

    @classmethod
    def today(cls):
        return cls(2024, 3, 5, 12, 0, 0)


@pytest.fixture(autouse=True)
def fixed_time():
    with mock.patch.object(elastic, "datetime", types.SimpleNamespace(datetime=FixedDatetime)):
        yield


def _response(content=b'{"result": "created"}', ok=True):
    return mock.Mock(content=content, text=content.decode(), ok=ok)


@pytest.fixture
def post():
    with mock.patch.object(elastic.requests, "post", return_value=_response()) as patched:
        yield patched


def _bulk_lines(call):
    body = call.kwargs["data"].decode()
    assert body.endswith("\n")
    return [json.loads(line) for line in body.splitlines()]


def test_send_to_elastic_posts_document_with_timestamp_to_monthly_index(post):
    message = {"msg": "hello", "when": datetime.date(2024, 1, 2)}

    response = elastic.Elastic.send_to_elastic("logs", message, server=SERVER, api_key="key", ssl_verify=False)

    assert response is post.return_value
    kwargs = post.call_args.kwargs
    assert kwargs["url"] == f"{SERVER}/logs-202403/_doc"
    assert kwargs["headers"]["Authorization"] == "ApiKey key"
    assert kwargs["verify"] is False
    assert json.loads(kwargs["data"]) == {"msg": "hello", "when": "2024-01-02", "@timestamp": "2024-03-05T10:15:00"}


@pytest.mark.parametrize("index_rollover, doc_id, expected_url", [
    (False, None, f"{SERVER}/logs/_doc"),
    (True, "abc", f"{SERVER}/logs-202403/_doc/abc"),
    (False, "abc", f"{SERVER}/logs/_doc/abc"),
])
def test_send_to_elastic_url(post, index_rollover, doc_id, expected_url):
    elastic.Elastic.send_to_elastic("logs", {}, id=doc_id, server=SERVER, index_rollover=index_rollover)

    assert post.call_args.kwargs["url"] == expected_url


def test_send_to_elastic_keeps_given_timestamp_and_drops_log_args(post):
    elastic.Elastic.send_to_elastic("logs", {"msg": "a 1", "args": ("1",)}, server=SERVER,
                                    iso_timestamp="2020-01-01T00:00:00")

    assert json.loads(post.call_args.kwargs["data"]) == {"msg": "a 1", "@timestamp": "2020-01-01T00:00:00"}


def test_send_to_elastic_logs_error_response(post, caplog):
    post.return_value = _response(b'{"error": {"type": "mapper_parsing_exception"}}', ok=False)

    elastic.Elastic.send_to_elastic("logs", {}, server=SERVER)

    assert "mapper_parsing_exception" in caplog.text
    assert caplog.records[-1].levelname == "ERROR"


def test_send_to_elastic_bulk_writes_action_line_before_each_document(post):
    documents = [{"a": 1}, {"a": 2}]

    assert elastic.Elastic.send_to_elastic_bulk("schedules", documents, server=SERVER) is True

    assert post.call_count == 1
    assert post.call_args.kwargs["url"] == f"{SERVER}/schedules-202403/_bulk"
    assert post.call_args.kwargs["headers"]["Content-Type"] == "application/x-ndjson"
    assert _bulk_lines(post.call_args) == [
        {"index": {"_index": "schedules-202403"}}, {"a": 1, "@timestamp": "2024-03-05T10:15:00"},
        {"index": {"_index": "schedules-202403"}}, {"a": 2, "@timestamp": "2024-03-05T10:15:00"},
    ]
    assert documents == [{"a": 1}, {"a": 2}]


@pytest.mark.parametrize("hashing, expected_ids", [
    (False, ["TSO1_001", "TSO2_"]),
    (True, [str(uuid.uuid5(uuid.NAMESPACE_OID, "TSO1_001")), str(uuid.uuid5(uuid.NAMESPACE_OID, "TSO2_"))]),
])
def test_send_to_elastic_bulk_ids_from_metadata(post, hashing, expected_ids):
    documents = [{"tso": "TSO1", "version": "001"}, {"tso": "TSO2"}]

    elastic.Elastic.send_to_elastic_bulk("models", documents, id_from_metadata=True, id_metadata_list=["tso", "version"],
                                         hashing=hashing, server=SERVER, index_rollover=False)

    actions = _bulk_lines(post.call_args)[::2]
    assert actions == [{"index": {"_index": "models", "_id": doc_id}} for doc_id in expected_ids]


def test_send_to_elastic_bulk_requires_id_metadata_list(post):
    with pytest.raises(Exception, match="id_metadata_list"):
        elastic.Elastic.send_to_elastic_bulk("models", [{"a": 1}], id_from_metadata=True, server=SERVER)
    post.assert_not_called()


def test_send_to_elastic_bulk_sends_batches_and_reports_failed_batch(post, caplog):
    post.side_effect = [_response(b'{"errors": false}'), _response(b'{"errors": true}', ok=False)]

    result = elastic.Elastic.send_to_elastic_bulk("logs", [{"n": n} for n in range(3)], batch_size=4, server=SERVER)

    assert result is False
    assert [len(_bulk_lines(call)) for call in post.call_args_list] == [4, 2]
    assert "responded with errors" in caplog.text


@pytest.mark.xfail(strict=True, reason="batch_size counts ndjson lines, an odd batch_size separates an action line from its document")
def test_send_to_elastic_bulk_keeps_action_and_document_in_same_batch(post):
    elastic.Elastic.send_to_elastic_bulk("logs", [{"n": n} for n in range(3)], batch_size=3, server=SERVER)

    for call in post.call_args_list:
        lines = _bulk_lines(call)
        assert len(lines) % 2 == 0
        assert all("index" in action and "index" not in document for action, document in zip(lines[::2], lines[1::2]))


@pytest.fixture
def service(monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    with mock.patch.object(elastic, "Elasticsearch"):
        service = elastic.Elastic(server=SERVER, api_key="key", ssl_verify=False)
    return service


def test_elastic_requires_ssl_cert_file_when_verifying(monkeypatch):
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    with mock.patch.object(elastic, "Elasticsearch") as client, pytest.raises(Exception, match="SSL_CERT_FILE"):
        elastic.Elastic(server=SERVER, ssl_verify=True)
    client.assert_not_called()


def test_elastic_uses_ssl_cert_file_when_verifying(monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", "/certs/ca.pem")
    with mock.patch.object(elastic, "Elasticsearch") as client:
        elastic.Elastic(server=SERVER, api_key="key", ssl_verify=True)

    assert client.call_args.kwargs["ca_certs"] == "/certs/ca.pem"
    assert client.call_args.kwargs["verify_certs"] is True


@pytest.mark.parametrize("index, searched_index", [("models", "models*"), ("models-2024*", "models-2024*")])
def test_get_docs_by_query_searches_index_pattern(service, index, searched_index):
    service.client.search.return_value = {"hits": {"hits": [], "total": {"value": 0}}}

    service.get_docs_by_query(index, query={"match_all": {}}, size=5)

    service.client.search.assert_called_once_with(index=searched_index, query={"match_all": {}}, size=5)


def test_get_docs_by_query_flattens_source_fields(service):
    hits = [{"_id": "1", "_source": {"tso": "AST", "meta": {"version": "001"}}}]
    service.client.search.return_value = {"hits": {"hits": hits, "total": {"value": 1}}}

    df = service.get_docs_by_query("models", query={})
    raw = service.get_docs_by_query("models", query={}, return_df=False)

    assert df.to_dict("records") == [{"_id": "1", "tso": "AST", "meta.version": "001"}]
    assert raw == hits


@pytest.mark.parametrize("period_overlap, start_range, end_range", [
    (False, {"gte": "2024-01-01T00:00:00"}, {"lte": "2024-01-01T01:00:00"}),
    (True, {"lte": "2024-01-01T00:00:00"}, {"gte": "2024-01-01T01:00:00"}),
])
def test_query_schedules_from_elk_builds_period_query(service, period_overlap, start_range, end_range):
    schedules = pd.DataFrame([{"value": 1}])
    with mock.patch.object(service, "get_docs_by_query", return_value=schedules) as get_docs:
        result = service.query_schedules_from_elk("schedules", "2024-01-01T00:00:00", "2024-01-01T01:00:00",
                                                  metadata={"TimeSeries.businessType": "B64"},
                                                  period_overlap=period_overlap)

    assert result is schedules
    get_docs.assert_called_once_with(index="schedules", size=10000, query={"bool": {"must": [
        {"range": {"utc_start": start_range}},
        {"range": {"utc_end": end_range}},
        {"match": {"TimeSeries.businessType": "B64"}},
    ]}})


@pytest.mark.parametrize("outcome", [{"return_value": pd.DataFrame()}, {"side_effect": RuntimeError("index missing")}],
                         ids=["no-hits", "query-error"])
def test_query_schedules_from_elk_returns_none_without_schedules(service, outcome):
    with mock.patch.object(service, "get_docs_by_query", **outcome):
        assert service.query_schedules_from_elk("schedules", "a", "b", metadata={}) is None


def test_handler_send_to_elastic_sends_message_and_passes_it_on():
    handler = elastic.HandlerSendToElastic(index="schedules", server=SERVER, id_from_metadata=True,
                                           id_metadata_list=["mRID"], hashing=True)
    message = json.dumps([{"mRID": "a"}]).encode()
    properties = mock.Mock()

    with mock.patch.object(elastic.Elastic, "send_to_elastic_bulk", return_value=True) as bulk:
        assert handler.handle(message, properties) == (message, properties)

    bulk.assert_called_once_with(index="schedules", json_message_list=[{"mRID": "a"}], id_from_metadata=True,
                                 id_metadata_list=["mRID"], hashing=True, server=SERVER, debug=False)
