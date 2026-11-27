from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

from emf.common.helpers.loadflow import load_network_model
from emf.model_merger import merge_functions
from emf.model_merger.merge_functions import MergedModel

SCENARIO_TIME = "2014-06-01T10:30:00Z"
# MicroGrid boundary EICs of tie lines BE-Line_3/NL-Line_1 (380 kV) and BE-Line_1/NL-Line_4
AL11_EIC, ST23_EIC = "10T-ES-PT-10004U", "10T-AT-DE-00016Z"


@pytest.mark.parametrize("time_horizon, merge", [("MO", "Month"), ("YR", "Year"), ("WK", "Week"), ("1D", "Week")])
def test_uap_outages_are_read_from_given_index(time_horizon, merge):
    with mock.patch.object(merge_functions.elastic, "Elastic") as elastic:
        service = elastic.return_value
        service.client.search.return_value = {"hits": {"hits": [{"fields": {"reportParsedDate": ["2025-07-01T06:00:00"]}}]}}
        service.get_docs_by_query.side_effect = [
            pd.DataFrame({"eic": ["10T-1"], "mrid": ["_m1"]}),
            pd.DataFrame({"mrid": ["_m1", "None", "_g1"], "asset_type": ["LINE", "LINE", "PROD"]}),
        ]

        uap_outages, mrid_map = merge_functions.get_uap_outages(time_horizon, index="uap-test*")

    assert service.client.search.call_args.kwargs["index"] == "uap-test*"
    assert service.client.search.call_args.kwargs["body"]["query"] == {"bool": {"must": [{"match": {"Merge": merge}}]}}
    assert service.get_docs_by_query.call_args.kwargs == {"index": "uap-test*", "size": 10000, "query": {"bool": {"must": [
        {"match": {"reportParsedDate": "2025-07-01T06:00:00"}}, {"match": {"Merge": merge}}]}}}
    assert mrid_map["mrid"].tolist() == ["m1"]
    assert uap_outages["mrid"].fillna("missing").tolist() == ["m1", "missing"]


def uap_outage(name, eic, area="Belgium"):
    """UAP outage active at SCENARIO_TIME, without mRID so it is mapped by EIC"""
    return {"name": name, "eic": eic, "mrid": pd.NA, "Area": area,
            "start_date": "2014-06-01T00:00:00Z", "end_date": "2014-06-02T00:00:00Z"}


@pytest.fixture
def microgrid(microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    """Merged MicroGrid with tie line BE-Line_3/NL-Line_1 disconnected on both sides"""
    network = load_network_model([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])
    ids = network.get_boundary_lines().reset_index().set_index("name")["id"]
    network.update_boundary_lines(id=[ids["BE-Line_3"], ids["NL-Line_1"]], connected=[False, False])
    return SimpleNamespace(model=MergedModel(network=network), ids=ids)


def update_outages(microgrid, uap_outages, **settings):
    """Runs the outage update for ELIA with the UAP query patched out, settings override the defaults below"""
    mrid_map = pd.DataFrame({"eic": [AL11_EIC, ST23_EIC], "mrid": [microgrid.ids["BE-Line_3"], microgrid.ids["BE-Line_1"]]})
    settings = {"tso_areas": {"ELIA": ("Belgium", "BE")}, "excluded_eics": [], "ignored_unmapped_eics": [],
                "min_nominal_v": 330, **settings}
    with mock.patch.object(merge_functions, "get_uap_outages",
                           return_value=(pd.DataFrame(uap_outages), mrid_map)) as get_uap_outages:
        merge_functions.update_model_outages(microgrid.model, tso_list=["ELIA"], scenario_datetime=SCENARIO_TIME,
                                             time_horizon="WK", uap_index="uap-test*", **settings)
    get_uap_outages.assert_called_once_with(time_horizon="WK", index="uap-test*")
    return [(outage["name"], outage["status"]) for outage in microgrid.model.outages_updated]


@pytest.mark.pypowsybl
@pytest.mark.parametrize("settings, updated", [
    pytest.param({}, [("BE-Line_3", "connected"), ("NL-Line_1", "connected"), ("BE-Line_1", "disconnected")],
                 id="defaults"),
    pytest.param({"excluded_eics": [AL11_EIC]}, [("BE-Line_3", "connected"), ("BE-Line_1", "disconnected")],
                 id="excluded-border-line-keeps-neighbour-half"),
    pytest.param({"min_nominal_v": 400}, [("BE-Line_1", "disconnected")], id="model-outage-below-min-nominal-v"),
    pytest.param({"tso_areas": {}}, [], id="tso-without-area"),
])
def test_outage_update_settings(microgrid, settings, updated):
    assert update_outages(microgrid, [uap_outage("BE-Line_1", ST23_EIC)], **settings) == updated


@pytest.mark.pypowsybl
@pytest.mark.parametrize("ignored_unmapped_eics, warned", [([], True), (["10T-OLD"], False)])
def test_unmapped_uap_outages_are_reported_unless_ignored(microgrid, caplog, ignored_unmapped_eics, warned):
    update_outages(microgrid, [uap_outage("BE-Line_1", ST23_EIC), uap_outage("Old line", "10T-OLD")],
                   ignored_unmapped_eics=ignored_unmapped_eics)

    assert ("Unable to map following outage mRIDs" in caplog.text) is warned
