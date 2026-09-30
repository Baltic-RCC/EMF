from unittest import mock

import pytest

from emf.common.integrations import object_storage
from emf.common.integrations.object_storage import models


@pytest.fixture
def elastic_client():
    with mock.patch.object(object_storage, "elastic_service") as service:
        yield service.client


@pytest.fixture
def minio_service():
    with mock.patch.object(object_storage, "minio_service") as service:
        yield service


@pytest.fixture
def opdm_service():
    service = mock.Mock()
    with mock.patch.object(models, "_get_opdm_service", return_value=service):
        yield service


@pytest.mark.parametrize("query_filter, expected", [
    (None, {"bool": {"must": [{"match": {"pmd:timeHorizon": "1D"}}, {"terms": {"pmd:TSO.keyword": ["AST", "PSE"]}}]}}),
    ("now-2w", {"bool": {"must": [{"match": {"pmd:timeHorizon": "1D"}}, {"terms": {"pmd:TSO.keyword": ["AST", "PSE"]}}],
                         "filter": {"range": {"pmd:scenarioDate": {"gte": "now-2w"}}}}}),
])
def test_compile_query(query_filter, expected):
    assert models.compile_query({"pmd:timeHorizon": "1D", "pmd:TSO.keyword": ["AST", "PSE"]}, filter=query_filter) == expected


def _hits(*sources):
    return {"_scroll_id": "scroll-1", "hits": {"hits": [{"_source": source} for source in sources]}}


def test_query_data_collects_all_scroll_pages_and_clears_scroll(elastic_client):
    elastic_client.search.return_value = _hits({"n": 1}, {"n": 2})
    elastic_client.scroll.side_effect = [_hits({"n": 3}), _hits()]

    result = models.query_data({"pmd:TSO": "AST"}, index="models")

    assert result == [{"n": 1}, {"n": 2}, {"n": 3}]
    elastic_client.search.assert_called_once_with(index="models*", query={"bool": {"must": [{"match": {"pmd:TSO": "AST"}}]}},
                                                  size="10000", sort=None, scroll="1m")
    assert elastic_client.scroll.call_args_list == [mock.call(scroll_id="scroll-1", scroll="1m")] * 2
    elastic_client.clear_scroll.assert_called_once_with(scroll_id="scroll-1")


def test_query_data_without_hits_does_not_scroll(elastic_client):
    elastic_client.search.return_value = _hits()

    assert models.query_data({"pmd:TSO": "AST"}) == []

    elastic_client.scroll.assert_not_called()
    elastic_client.clear_scroll.assert_called_once_with(scroll_id="scroll-1")


def test_query_data_returns_payload_when_requested(elastic_client):
    elastic_client.search.return_value = _hits({"opde:Id": "a"}, {"opde:Id": "b"})
    elastic_client.scroll.return_value = _hits()

    with mock.patch.object(models, "get_content", side_effect=lambda item: {**item, "downloaded": True}) as get_content:
        result = models.query_data({}, return_payload=True)

    assert result == [{"opde:Id": "a", "downloaded": True}, {"opde:Id": "b", "downloaded": True}]
    assert get_content.call_count == 2


def _metadata(*content_references, bucket=None):
    metadata = {"opde:Id": "model-1",
                "opde:Component": [{"opdm:Profile": {"pmd:content-reference": reference}} for reference in content_references]}
    if bucket:
        metadata["minio-bucket"] = bucket
    return metadata


@pytest.mark.parametrize("bucket, expected_bucket", [(None, "opdm-data"), ("pdn-data", "pdn-data")])
def test_get_content_downloads_every_component_from_minio(minio_service, opdm_service, bucket, expected_bucket):
    minio_service.download_object.side_effect = lambda bucket_name, reference: f"{bucket_name}:{reference}".encode()
    metadata = _metadata("EQ.zip", "SSH.zip", bucket=bucket)

    result = models.get_content(metadata)

    assert [c["opdm:Profile"]["DATA"] for c in result["opde:Component"]] == [
        f"{expected_bucket}:EQ.zip".encode(), f"{expected_bucket}:SSH.zip".encode()]
    opdm_service.download_object.assert_not_called()


def test_get_content_falls_back_to_opdm_when_a_component_is_missing(minio_service, opdm_service, caplog):
    minio_service.download_object.side_effect = [b"eq", None]
    metadata = _metadata("EQ.zip", "SSH.zip")

    result = models.get_content(metadata)

    opdm_service.download_object.assert_called_once_with(metadata)
    assert result is opdm_service.download_object.return_value
    assert "[FALLBACK]" in caplog.text


def _boundary(date, version, official=True):
    return {"opde:Id": f"{date}-{version}-{official}", "pmd:scenarioDate": date, "pmd:versionNumber": version,
            "opde:Context": {"opde:IsOfficial": "true" if official else "false"}}


def test_get_latest_boundary_picks_latest_official_date_then_version():
    boundaries = [
        _boundary("2024-01-01T00:00:00Z", "009"),
        _boundary("2024-03-01T00:00:00Z", "001"),
        _boundary("2024-03-01T00:00:00Z", "002"),
        _boundary("2024-06-01T00:00:00Z", "005", official=False),
    ]

    with mock.patch.object(models, "query_data", return_value=boundaries) as query_data, \
            mock.patch.object(models, "get_content", side_effect=lambda metadata: metadata) as get_content:
        latest = models.get_latest_boundary()

    query_data.assert_called_once_with({"opde:Object-Type.keyword": "BDS"})
    assert latest["opde:Id"] == "2024-03-01T00:00:00Z-002-True"
    get_content.assert_called_once()


def _model(tso, version, time_horizon="1D"):
    return {"opde:Id": f"{tso}-{time_horizon}-{version}", "pmd:TSO": tso, "pmd:modelPartReference": tso,
            "pmd:timeHorizon": time_horizon, "pmd:versionNumber": version}


@pytest.fixture
def query_models():
    with mock.patch.object(models, "query_data") as query_data, \
            mock.patch.object(models, "get_content", side_effect=lambda metadata: metadata) as get_content:
        yield query_data, get_content


def test_get_latest_models_builds_metadata_query(query_models):
    query_data, _ = query_models
    query_data.return_value = []

    assert models.get_latest_models_and_download("1D", "2024-05-01T10:30:00Z", tso=["AST", "PSE"], data_source="OPDM") == []

    query_data.assert_called_once_with(metadata_query={
        "pmd:validFrom": "20240501T1030Z", "pmd:timeHorizon": "1D", "opde:Object-Type": "IGM", "data-source": "OPDM",
        "pmd:TSO.keyword": ["AST", "PSE"], "valid": True}, return_payload=False)


def test_get_latest_models_expands_intraday_time_horizon(query_models):
    query_data, _ = query_models
    query_data.return_value = []

    models.get_latest_models_and_download("id", "2024-05-01T10:30:00Z")

    assert query_data.call_args.kwargs["metadata_query"]["pmd:timeHorizon"] == [f"{hour:02d}" for hour in range(1, 32)]


def test_get_latest_models_downloads_latest_version_per_model_part(query_models):
    query_data, get_content = query_models
    query_data.return_value = [_model("AST", "001"), _model("AST", "003"), _model("AST", "002"), _model("PSE", "001")]

    downloaded = models.get_latest_models_and_download("1D", "2024-05-01T10:30:00Z")

    assert sorted((model["pmd:TSO"], model["pmd:versionNumber"]) for model in downloaded) == [("AST", "003"), ("PSE", "001")]


def test_get_latest_models_skips_models_that_fail_to_download(query_models, caplog):
    query_data, get_content = query_models
    query_data.return_value = [_model("AST", "001"), _model("PSE", "001")]
    get_content.side_effect = lambda metadata: {"PSE": metadata}[metadata["pmd:TSO"]]

    downloaded = models.get_latest_models_and_download("1D", "2024-05-01T10:30:00Z")

    assert [model["pmd:TSO"] for model in downloaded] == ["PSE"]
    assert "Could not download model for 1D 2024-05-01T10:30:00Z AST" in caplog.text


@pytest.mark.xfail(strict=True, reason="groupby('pmd:modelPartReference').first() drops pmd:modelPartReference from the returned metadata")
def test_get_latest_models_returns_complete_metadata_of_selected_model(query_models):
    query_data, _ = query_models
    query_data.return_value = [_model("AST", "001"), _model("AST", "002")]

    assert models.get_latest_models_and_download("1D", "2024-05-01T10:30:00Z") == [_model("AST", "002")]


def test_fetch_unique_values_pages_through_composite_aggregation(elastic_client):
    def page(values, after_key=None):
        aggregation = {"buckets": [{"key": {"val": value}} for value in values]}
        if after_key:
            aggregation["after_key"] = after_key
        return {"aggregations": {"values_page": aggregation}}

    elastic_client.search.side_effect = [page(["AST", "ELERING"], after_key={"val": "ELERING"}), page(["PSE"])]

    assert models.fetch_unique_values({"pmd:timeHorizon": "1D"}, field="pmd:TSO.keyword", index="models") == ["AST", "ELERING", "PSE"]

    first, second = (c.kwargs for c in elastic_client.search.call_args_list)
    assert first["index"] == "models*"
    assert "after" not in first["body"]["aggs"]["values_page"]["composite"]
    assert second["body"]["aggs"]["values_page"]["composite"]["after"] == {"val": "ELERING"}
