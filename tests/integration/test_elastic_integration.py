import os
import re
import uuid

import pytest

from emf.common.converters import iec_schedule_to_ndjson
from emf.common.integrations.elastic import Elastic, HandlerSendToElastic

pytestmark = pytest.mark.integration

ELK_SERVER = os.environ.get("ELK_SERVER", "http://localhost:9200")
TIMESTAMP = "2025-01-01T00:00:00"
MESSAGES = [{"mRID": "S1", "position": 1, "value": 10.0}, {"mRID": "S1", "position": 2, "value": 20.0}]

SCHEDULE = b"""<Schedule_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-2:scheduledocument:5:0">
  <mRID>SCHEDULE-1</mRID><process.processType>A17</process.processType>
  <TimeSeries><mRID>TS-1</mRID><businessType>B63</businessType><curveType>A01</curveType>
    <Period><timeInterval><start>2025-01-01T23:00Z</start><end>2025-01-02T00:00Z</end></timeInterval><resolution>PT30M</resolution>
      <Point><position>1</position><quantity>100</quantity></Point>
      <Point><position>2</position><quantity>200</quantity></Point>
    </Period>
  </TimeSeries>
</Schedule_MarketDocument>"""


@pytest.fixture
def elastic():
    return Elastic(server=ELK_SERVER, ssl_verify=False)


@pytest.fixture
def index(elastic):
    """Unique index name, every index starting with it is deleted afterwards (rollover adds a monthly suffix)"""
    name = f"emfos-test-{uuid.uuid4().hex[:8]}"
    yield name
    for created in elastic.client.indices.get(index=f"{name}*"):
        elastic.client.indices.delete(index=created)


def refresh(elastic, index: str):
    elastic.client.indices.refresh(index=f"{index}*")


def test_send_to_elastic_indexes_document_under_given_id(elastic, index):
    response = Elastic.send_to_elastic(index=index, json_message={"name": "test", "value": 1}, id="doc-1",
                                       server=ELK_SERVER, ssl_verify=False, iso_timestamp=TIMESTAMP, index_rollover=False)

    assert response.status_code == 201
    assert elastic.get_doc_by_id(index=index, id="doc-1")["_source"] == {"name": "test", "value": 1, "@timestamp": TIMESTAMP}


def test_send_to_elastic_with_rollover_writes_to_monthly_index(elastic, index):
    Elastic.send_to_elastic(index=index, json_message={"name": "test"}, server=ELK_SERVER, ssl_verify=False)
    refresh(elastic, index)

    documents = elastic.get_docs_by_query(index=index, query={"match_all": {}})

    assert len(documents) == 1
    assert re.fullmatch(rf"{index}-\d{{6}}", documents["_index"][0])
    assert documents["name"][0] == "test"


@pytest.mark.parametrize("hashing, expected_ids", [
    (False, ["S1_1", "S1_2"]),
    (True, [str(uuid.uuid5(uuid.NAMESPACE_OID, "S1_1")), str(uuid.uuid5(uuid.NAMESPACE_OID, "S1_2"))]),
])
def test_send_to_elastic_bulk_indexes_documents_with_ids_from_metadata(elastic, index, hashing, expected_ids):
    assert Elastic.send_to_elastic_bulk(index=index, json_message_list=MESSAGES, id_from_metadata=True,
                                        id_metadata_list=["mRID", "position"], hashing=hashing, server=ELK_SERVER,
                                        ssl_verify=False, iso_timestamp=TIMESTAMP, index_rollover=False) is True
    refresh(elastic, index)

    documents = elastic.get_docs_by_query(index=index, query={"match_all": {}}).sort_values("position")

    assert list(documents["_id"]) == expected_ids
    assert list(documents["value"]) == [10.0, 20.0]
    assert set(documents["@timestamp"]) == {TIMESTAMP}


def test_send_to_elastic_bulk_overwrites_documents_with_same_ids(elastic, index):
    arguments = dict(index=index, id_from_metadata=True, id_metadata_list=["mRID", "position"], server=ELK_SERVER,
                     ssl_verify=False, index_rollover=False)
    Elastic.send_to_elastic_bulk(json_message_list=MESSAGES, **arguments)
    Elastic.send_to_elastic_bulk(json_message_list=[{**MESSAGES[0], "value": 11.0}], **arguments)
    refresh(elastic, index)

    documents = elastic.get_docs_by_query(index=index, query={"match_all": {}}).sort_values("position")

    assert list(documents["value"]) == [11.0, 20.0]


def test_send_to_elastic_bulk_without_ids_in_batches(elastic, index):
    messages = [{"position": position} for position in range(1, 6)]

    assert Elastic.send_to_elastic_bulk(index=index, json_message_list=messages, server=ELK_SERVER, ssl_verify=False,
                                        batch_size=4, index_rollover=False) is True
    refresh(elastic, index)

    assert sorted(elastic.get_docs_by_query(index=index, query={"match_all": {}})["position"]) == [1, 2, 3, 4, 5]


def test_converted_schedule_is_stored_and_queried_back(elastic, index):
    content, _ = iec_schedule_to_ndjson.convert(SCHEDULE)
    handler = HandlerSendToElastic(index=index, server=ELK_SERVER, id_from_metadata=True, ssl_verify=False,
                                   id_metadata_list=["mRID", "process.processType", "TimeSeries.mRID", "position"])

    assert handler.handle(content, properties=None) == (content, None)
    refresh(elastic, index)
    schedules = elastic.query_schedules_from_elk(index=index, utc_start="2025-01-01T23:00:00", utc_end="2025-01-02T00:00:00",
                                                 metadata={"TimeSeries.mRID": "TS-1"})

    schedules = schedules.sort_values("position")
    assert list(schedules["_id"]) == [f"SCHEDULE-1_A17_TS-1_{position}" for position in (1, 2, 3, 4)]
    assert list(schedules["value"]) == [100.0, 100.0, 200.0, 200.0]
