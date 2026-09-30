import io
import zipfile
from unittest import mock

import pypowsybl as pp
import pytest

from emf.common.helpers.loadflow import (get_connected_components_data, get_model_outages, get_network_elements,
                                         get_slack_generators, load_network_model, package_for_pypowsybl,
                                         parse_pypowsybl_report)

NODE_BREAKER_AS_BUS_BREAKER = "iidm.import.cgmes.import-node-breaker-as-bus-breaker"

# pypowsybl report layout: "+ " opens a node, the "Network info" node holds one line per fact
TWO_COMPONENT_REPORT = """+ Test report
   + Load flow on network 'merged'
      + Network CC0 SC0
         + Network info
            Network has 10 buses and 12 branches
            Network balance: active generation=100.0 MW, active load=98.0 MW
            Angle reference bus: VL1_0
            Slack bus: VL1_0
         Outer loop DistributedSlack
      + Network CC1 SC1
         + Network info
            Network has 2 buses and 1 branches
            Slack bus: VL9_0
"""


def inner_xml_files(opdm_objects):
    files = {}
    for opdm_object in opdm_objects:
        for component in opdm_object["opde:Component"]:
            with zipfile.ZipFile(io.BytesIO(component["opdm:Profile"]["DATA"])) as profile_zip:
                files.update({name: profile_zip.read(name) for name in profile_zip.namelist()})
    return files


def zip_content(zip_file):
    with zipfile.ZipFile(zip_file) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


# --- packaging and loading

def test_opdm_objects_are_packaged_into_one_flat_zip(microgrid_be_igm, microgrid_boundary):
    package = package_for_pypowsybl([microgrid_be_igm, microgrid_boundary])

    assert zip_content(package) == inner_xml_files([microgrid_be_igm, microgrid_boundary])
    assert len(zip_content(package)) == 6


def test_package_can_be_written_as_zip_file(microgrid_be_igm, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    file_name = package_for_pypowsybl([microgrid_be_igm], return_zip=True)

    assert file_name.endswith(".zip")
    assert zip_content(tmp_path / file_name) == inner_xml_files([microgrid_be_igm])


@pytest.mark.parametrize("parameters, skip_defaults, expected", [
    (None, False, {NODE_BREAKER_AS_BUS_BREAKER: "true"}),
    ({"iidm.import.cgmes.convert-sv-injections": "false"}, False,
     {NODE_BREAKER_AS_BUS_BREAKER: "true", "iidm.import.cgmes.convert-sv-injections": "false"}),
    ({NODE_BREAKER_AS_BUS_BREAKER: "false"}, False, {NODE_BREAKER_AS_BUS_BREAKER: "false"}),
    (None, True, None),
    ({"iidm.import.cgmes.convert-sv-injections": "false"}, True, {"iidm.import.cgmes.convert-sv-injections": "false"}),
])
def test_import_parameters_default_and_override(microgrid_be_igm, parameters, skip_defaults, expected):
    with mock.patch.object(pp.network, "load_from_binary_buffer") as load:
        network = load_network_model([microgrid_be_igm], parameters=parameters, skip_default_parameters=skip_defaults)

    assert network is load.return_value
    assert load.call_args.kwargs["parameters"] == expected
    assert zip_content(load.call_args.kwargs["buffer"]) == inner_xml_files([microgrid_be_igm])


@pytest.mark.pypowsybl
def test_igms_are_loaded_as_one_merged_network(microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    network = load_network_model([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])

    assert len(network.get_sub_networks()) == 2
    assert not network.get_tie_lines().empty


# --- element getters

@pytest.fixture
def micro_grid_be():
    return pp.network.create_micro_grid_be_network()


@pytest.mark.pypowsybl
def test_network_elements_get_voltage_level_and_substation_data(micro_grid_be):
    generators = get_network_elements(micro_grid_be, pp.network.ElementType.GENERATOR)

    assert generators[["name", "voltage_level_name", "substation_name", "nominal_v", "country"]].to_dict("records") == [
        {"name": "BE-G1", "voltage_level_name": "10.5", "substation_name": "PP_Brussels", "nominal_v": 10.5, "country": "BE"},
        {"name": "BE-G2", "voltage_level_name": "21.0", "substation_name": "PP_Brussels", "nominal_v": 21.0, "country": "BE"},
    ]


@pytest.mark.pypowsybl
def test_network_elements_reuse_given_voltage_levels_and_substations(micro_grid_be):
    voltage_levels = micro_grid_be.get_voltage_levels(all_attributes=True).assign(from_cache="voltage level")
    substations = micro_grid_be.get_substations(all_attributes=True).assign(from_cache_substation="substation")

    loads = get_network_elements(micro_grid_be, pp.network.ElementType.LOAD, voltage_levels=voltage_levels,
                                 substations=substations)

    assert set(loads["from_cache"]) == {"voltage level"}
    assert set(loads["from_cache_substation"]) == {"substation"}


@pytest.mark.pypowsybl
def test_network_elements_pass_filters_to_pypowsybl(micro_grid_be):
    generator_id = micro_grid_be.get_generators().index[0]

    generators = get_network_elements(micro_grid_be, pp.network.ElementType.GENERATOR, id=[generator_id])

    assert generators.index.tolist() == [generator_id]


@pytest.mark.pypowsybl
@pytest.mark.parametrize("network_factory, count", [(pp.network.create_micro_grid_be_network, 5),
                                                     (pp.network.create_eurostag_tutorial_example1_network, 0)])
def test_boundary_lines_always_have_hvdc_column(network_factory, count):
    boundary_lines = get_network_elements(network_factory(), pp.network.ElementType.BOUNDARY_LINE)

    assert len(boundary_lines) == count
    assert "isHvdc" in boundary_lines.columns


@pytest.mark.pypowsybl
def test_slack_generators_follow_slack_terminal_extension():
    network = pp.network.create_ieee14()
    network.create_extensions("slackTerminal", voltage_level_id="VL2", element_id="B2-G")

    slack_generators = get_slack_generators(network)

    assert sorted(slack_generators.index) == ["B1-G", "B2-G"]
    assert set(slack_generators["nominal_v"]) == {135.0}


# --- connected components

@pytest.mark.pypowsybl
def test_connected_components_list_countries_and_bus_count():
    components = get_connected_components_data(pp.network.create_eurostag_tutorial_example1_network())

    assert list(components) == [0]
    assert sorted(components[0]["countries"]) == ["BE", "FR"]
    assert components[0]["bus_count"] == 4


@pytest.mark.pypowsybl
@pytest.mark.parametrize("threshold, bus_counts", [(None, [13, 1]), (5, [13])])
def test_connected_components_below_bus_count_threshold_are_dropped(threshold, bus_counts):
    network = pp.network.create_ieee14()
    network.update_lines(id="L7-8-1", connected1=False, connected2=False)  # bus 8 becomes an island

    components = get_connected_components_data(network, bus_count_threshold=threshold)

    assert sorted((component["bus_count"] for component in components.values()), reverse=True) == bus_counts


# --- outages

@pytest.fixture
def eurostag():
    return pp.network.create_eurostag_tutorial_example1_network()


@pytest.mark.pypowsybl
def test_model_outages_list_disconnected_elements_of_330kv_and_above(eurostag):
    eurostag.update_lines(id="NHV1_NHV2_1", connected1=False)
    eurostag.update_generators(id="GEN", connected=False)  # 24 kV

    outages = get_model_outages(eurostag)

    assert [(outage["grid_id"], outage["element_type"], outage["nominal_v"]) for outage in outages] == [
        ("NHV1_NHV2_1", "LINE", 380.0)]


@pytest.mark.pypowsybl
@pytest.mark.parametrize("nominal_v, listed", [(330.0, True), (220.0, False)])
def test_model_outages_voltage_limit_is_inclusive(eurostag, nominal_v, listed):
    eurostag.update_voltage_levels(id="VLHV1", nominal_v=nominal_v)
    eurostag.update_lines(id="NHV1_NHV2_2", connected2=False)

    outages = get_model_outages(eurostag)

    assert [outage["grid_id"] for outage in outages] == (["NHV1_NHV2_2"] if listed else [])


@pytest.mark.pypowsybl
def test_model_outages_include_disconnected_boundary_lines(micro_grid_be):
    boundary_line_ids = micro_grid_be.get_boundary_lines().reset_index().set_index("name")["id"]
    micro_grid_be.update_boundary_lines(id=[boundary_line_ids["BE-Line_3"], boundary_line_ids["BE-Line_1"]],
                                        connected=[False, False])  # 380 kV, 225 kV

    outages = get_model_outages(micro_grid_be)

    assert [(outage["name"], outage["element_type"]) for outage in outages] == [("BE-Line_3", "BOUNDARY_LINE")]


@pytest.mark.pypowsybl
def test_model_without_disconnected_elements_has_no_outages(eurostag):
    assert get_model_outages(eurostag) == []


# --- report parsing

def test_report_network_info_is_parsed_per_component():
    assert parse_pypowsybl_report(TWO_COMPONENT_REPORT) == [
        {"buses": 10, "branches": 12, "Network balance": {"active generation": "100.0 MW", "active load": "98.0 MW"},
         "Angle reference bus": "VL1_0", "Slack bus": "VL1_0"},
        {"buses": 2, "branches": 1, "Slack bus": "VL9_0"},
    ]


def test_report_without_network_info_gives_empty_list():
    assert parse_pypowsybl_report("+ Load flow on network 'x'\n   AC load flow completed successfully\n") == []


@pytest.mark.pypowsybl
def test_real_load_flow_report_is_parsed():
    report_node = pp.report.ReportNode()
    pp.loadflow.run_ac(pp.network.create_ieee14(), report_node=report_node)

    network_info, = parse_pypowsybl_report(str(report_node))

    assert (network_info["buses"], network_info["branches"]) == (14, 20)
    assert network_info["Slack bus"] == "VL1_0"
    assert network_info["Network balance"]["active generation"] == "272.4 MW"
