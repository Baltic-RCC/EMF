import math
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import polars as pl
import pypowsybl as pp
import pytest

from emf.common.helpers.loadflow import load_network_model
from emf.model_merger import scaler
from emf.model_merger.merge_functions import MergedModel
from emf.model_merger.model_merger import HandlerMergeModels

# MicroGrid BaseCase ids
BE_380_VOLTAGE_LEVEL = "469df5f7-058f-4451-a998-57a48e8a56fe"
BE_380_BUS = "e44141af-f1dc-44d3-bfa4-b674e5c953d7"
BE_LOAD_2 = "1c6beed6-1acf-42e7-ba55-0cc9f04bddd8"

# MicroGrid BE imports ~235 MW from NL after the pre-loadflow corrections, targets below shift that by 50 MW
BE_IMPORTS_285 = {"value": 285.0, "in_domain": "BE", "out_domain": None}
NL_EXPORTS_285 = {"value": 285.0, "in_domain": None, "out_domain": "NL"}
# a DC schedule for a link that is not in the model, the scaler needs at least one DC schedule row
FOREIGN_HVDC_SCHEDULE = {"value": 100.0, "in_domain": "NL", "out_domain": "GB",
                         "registered_resource": "10T-XX-HVDC-000X", "hvdc_name": "OTHER"}

BALANCE_THRESHOLD = float(scaler.BALANCE_THRESHOLD)
POWER_FACTOR_THRESHOLD = float(scaler.POWER_FACTOR_THRESHOLD)


@pytest.fixture
def scalable_microgrid(microgrid_be_igm, microgrid_nl_igm, microgrid_boundary):
    """
    MicroGrid BE + NL merged the way the merger does it. MicroGrid only has EnergyConsumers and no HVDC boundary,
    so ConformLoad details and the boundary 'isHvdc' property (both present in real IGMs + ENTSO-E BDS) are added here
    """
    def _build(extra_boundary_lines=(), non_conform_loads=(), hvdc_property=True):
        model = MergedModel()
        model.network = load_network_model([microgrid_be_igm, microgrid_nl_igm, microgrid_boundary])
        model = HandlerMergeModels.apply_pre_loadflow_corrections(merged_model=model)
        network = model.network

        hvdc_lines = {}
        for line in extra_boundary_lines:
            network.create_boundary_lines(id=line["id"], voltage_level_id=BE_380_VOLTAGE_LEVEL, bus_id=BE_380_BUS,
                                          p0=line["p0"], q0=0.0, r=0.5, x=5.0, g=0.0, b=0.0)
            if line.get("eic"):
                hvdc_lines[line["id"]] = line["eic"]

        if hvdc_property:
            boundary_line_ids = network.get_boundary_lines().index.tolist()
            network.add_elements_properties(id=boundary_line_ids,
                                            isHvdc=["true" if i in hvdc_lines else "" for i in boundary_line_ids])
        if hvdc_lines:
            network.add_elements_properties(id=list(hvdc_lines), lineEnergyIdentificationCodeEIC=list(hvdc_lines.values()))

        loads = network.get_loads()
        conform = ~loads.index.isin(list(non_conform_loads))
        network.create_extensions("detail", id=loads.index.tolist(),
                                  fixed_p0=loads.p0.where(~conform, 0.0).tolist(),
                                  variable_p0=loads.p0.where(conform, 0.0).tolist(),
                                  fixed_q0=loads.q0.where(~conform, 0.0).tolist(),
                                  variable_q0=loads.q0.where(conform, 0.0).tolist())
        return model

    return _build


def _area_of_elements(network, elements: pd.DataFrame) -> pd.Series:
    substations = network.get_voltage_levels()["substation_id"]
    regions = network.get_substations(all_attributes=True)["CGMES.regionName"]
    return elements["voltage_level_id"].map(substations).map(regions)


def _ac_net_positions(network) -> pd.Series:
    """Export-positive AC net position per area from the solved AC boundary flows"""
    lines = network.get_boundary_lines(all_attributes=True)
    lines = lines[lines["isHvdc"] != "true"]
    return (-lines["boundary_p"]).groupby(_area_of_elements(network, lines)).sum()


def _loads(network) -> pd.DataFrame:
    loads = network.get_loads()[["voltage_level_id", "p0", "q0"]]
    return loads.assign(area=_area_of_elements(network, loads))


# ---------------------------------------------------------------- pure helpers

@pytest.mark.parametrize("index_name, expected_column", [("id", "id"), (None, "id"), ("element", "element")])
def test_pl_from_pypowsybl_keeps_element_ids_as_column(index_name, expected_column):
    frame = pd.DataFrame({"p": [1.0, 2.0]}, index=pd.Index(["a", "b"], name=index_name))

    result = scaler._pl_from_pypowsybl(frame)

    assert result.columns == [expected_column, "p"]
    assert result[expected_column].to_list() == ["a", "b"]


def test_area_key_frame_sums_values_per_area_and_component_rounded():
    frame = pl.DataFrame({
        "CGMES.regionName": ["BE", "BE", "NL", "BE"],
        "connected_component": [0, 0, 0, 1],
        "boundary_p": [10.04, 5.0, -3.33, 1.0],
    })

    result = scaler._area_key_frame(frame, value_col="boundary_p")

    assert scaler._to_area_dict(result) == {"BE-0": 15.0, "BE-1": 1.0, "NL-0": -3.3}
    assert result["area_key"].to_list() == sorted(result["area_key"].to_list())


def test_validate_converged_components_marks_single_area_island_without_boundary_lines_as_internal():
    components = {
        0: {"countries": ["BE", "NL"], "bus_count": 10},
        1: {"countries": ["BE"], "bus_count": 6},
        2: {"countries": ["NL"], "bus_count": 7},
    }
    boundary_lines = pl.DataFrame({"id": ["bl-0", "bl-2"], "connected_component": [0, 2]})

    result = scaler.validate_converged_components(boundary_lines=boundary_lines, converged_components=components)

    assert {k: v["state"] for k, v in result.items()} == {0: "valid", 1: "internal", 2: "valid"}


def _component_result(component: int, status: pp.loadflow.ComponentStatus):
    return SimpleNamespace(connected_component_num=component, status=status)


@pytest.mark.parametrize("main_status, island_status, expected_valid, expected_components", [
    (pp.loadflow.ComponentStatus.CONVERGED, pp.loadflow.ComponentStatus.CONVERGED, True, {0, 1}),
    (pp.loadflow.ComponentStatus.CONVERGED, pp.loadflow.ComponentStatus.MAX_ITERATION_REACHED, True, {0}),
    (pp.loadflow.ComponentStatus.FAILED, pp.loadflow.ComponentStatus.CONVERGED, False, {1}),
])
def test_validate_loadflow_status_drops_diverged_components_and_reports_main_island(
        main_status, island_status, expected_valid, expected_components):
    components = {0: {"countries": ["BE"]}, 1: {"countries": ["NL"]}}
    results = [_component_result(0, main_status), _component_result(1, island_status),
               _component_result(5, pp.loadflow.ComponentStatus.FAILED)]

    assert scaler.validate_loadflow_status(results=results, components=components) is expected_valid
    assert set(components) == expected_components


def test_get_countries_to_components_maps_each_area_to_all_its_islands():
    components = {0: {"countries": ["BE", "NL"]}, 1: {"countries": ["BE"]}, 2: {"state": "valid"}}

    assert scaler.get_countries_to_components(components) == {"BE": {0, 1}, "NL": {0}}


def test_get_fragmented_areas_participation_splits_area_by_fragment_flows():
    boundary_lines = pl.DataFrame({
        "CGMES.regionName": ["BE", "BE", "BE", "NL"],
        "connected_component": [0, 0, 1, 0],
        "boundary_p": [20.0, 10.0, 10.0, -40.0],
    })

    result = scaler.get_fragmented_areas_participation(boundary_lines, {"BE": {0, 1}, "NL": {0}})

    assert result.sort("connected_component").rows(named=True) == [
        {"connected_component": 0, "participation": 0.75, "registered_resource": "BE"},
        {"connected_component": 1, "participation": 0.25, "registered_resource": "BE"},
    ]


def test_get_fragmented_areas_participation_without_fragmented_areas_is_empty():
    boundary_lines = pl.DataFrame({"CGMES.regionName": ["BE"], "connected_component": [0], "boundary_p": [20.0]})

    result = scaler.get_fragmented_areas_participation(boundary_lines, {"BE": {0}})

    assert result.is_empty()
    assert result.columns == ["connected_component", "participation", "registered_resource"]


@pytest.mark.xfail(strict=True, reason="fragment participation divides by |sum| instead of the sum of |flows|, "
                                       "so fragments with opposite flows get participations summing to more than 1")
def test_get_fragmented_areas_participation_of_opposite_flow_fragments_sums_to_one():
    boundary_lines = pl.DataFrame({
        "CGMES.regionName": ["BE", "BE"],
        "connected_component": [0, 1],
        "boundary_p": [30.0, -10.0],
    })

    result = scaler.get_fragmented_areas_participation(boundary_lines, {"BE": {0, 1}})

    assert result["participation"].sum() == pytest.approx(1.0)


@pytest.mark.parametrize("p, q, expected", [
    (10.0, 5.0, 0.5),
    (-10.0, 5.0, -0.5),
    (0.0, 0.0, 0.0),
    (None, 1.0, 0.0),
    (1.0, 50.0, POWER_FACTOR_THRESHOLD),
    (1.0, -50.0, -POWER_FACTOR_THRESHOLD),
    (0.0, 5.0, POWER_FACTOR_THRESHOLD),
])
def test_set_power_ratio_to_boundary_lines_is_q_over_p_bounded_by_threshold(p, q, expected):
    frame = pl.DataFrame({"boundary_p": [p], "boundary_q": [q]}, schema={"boundary_p": pl.Float64, "boundary_q": pl.Float64})

    result = scaler._set_power_ratio_to_boundary_lines(frame)

    assert result["power_factor"].to_list() == [pytest.approx(expected)]


# ---------------------------------------------------------------- scale_balance on MicroGrid

@pytest.mark.pypowsybl
@pytest.mark.parametrize("debug", [False, True])
@pytest.mark.parametrize("be_import", [285.0, 200.0])
def test_scale_balance_brings_each_area_ac_net_position_within_balance_threshold(scalable_microgrid, be_import, debug):
    model = scalable_microgrid()
    ac_schedules = [{**BE_IMPORTS_285, "value": be_import}, {**NL_EXPORTS_285, "value": be_import}]

    model = scaler.scale_balance(model, ac_schedules, [FOREIGN_HVDC_SCHEDULE], debug=debug)

    acnp = _ac_net_positions(model.network)
    assert acnp["BE"] == pytest.approx(-be_import, abs=BALANCE_THRESHOLD)
    assert acnp["NL"] == pytest.approx(be_import, abs=BALANCE_THRESHOLD)
    assert model.scaled is True
    assert {entry["area"]: entry["success"] for entry in model.scaled_entity} == {"BE-0": True, "NL-0": True}
    assert all(abs(entry["final_offset_acnp"]) <= BALANCE_THRESHOLD for entry in model.scaled_entity)


@pytest.mark.pypowsybl
def test_scale_balance_scales_conform_loads_proportionally_and_keeps_their_power_factor(scalable_microgrid):
    model = scalable_microgrid()
    before = _loads(model.network)

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    after = _loads(model.network)
    ratio = after.p0 / before.p0
    for area, area_ratio in ratio.groupby(before.area):
        assert area_ratio.to_numpy() == pytest.approx(area_ratio.iloc[0]), area
    # BE has to import more, so its consumption goes up and NL's goes down
    assert ratio[before.area == "BE"].iloc[0] > 1 > ratio[before.area == "NL"].iloc[0]

    ratio_before, ratio_after = before.q0 / before.p0, after.q0 / after.p0
    within_threshold = ratio_before.abs() <= POWER_FACTOR_THRESHOLD
    assert ratio_after[within_threshold].to_numpy() == pytest.approx(ratio_before[within_threshold].to_numpy())


@pytest.mark.pypowsybl
def test_scale_balance_leaves_non_conform_loads_unchanged(scalable_microgrid):
    model = scalable_microgrid(non_conform_loads=[BE_LOAD_2])
    before = _loads(model.network)

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    after = _loads(model.network)
    assert after.loc[BE_LOAD_2, ["p0", "q0"]].tolist() == before.loc[BE_LOAD_2, ["p0", "q0"]].tolist()
    assert _ac_net_positions(model.network)["BE"] == pytest.approx(-285.0, abs=BALANCE_THRESHOLD)


@pytest.mark.pypowsybl
def test_scale_balance_ignores_islands_with_too_few_buses(scalable_microgrid):
    model = scalable_microgrid()
    network = model.network
    network.create_substations(id="ISLAND-S", country="BE")
    network.add_elements_properties(id=["ISLAND-S"], **{"CGMES.regionName": ["BE"]})
    network.create_voltage_levels(id="ISLAND-VL", substation_id="ISLAND-S", topology_kind="BUS_BREAKER",
                                  nominal_v=110.0, low_voltage_limit=90.0, high_voltage_limit=130.0)
    network.create_buses(id="ISLAND-B", voltage_level_id="ISLAND-VL")
    network.create_loads(id="ISLAND-L", voltage_level_id="ISLAND-VL", bus_id="ISLAND-B", p0=10.0, q0=2.0)
    network.create_generators(id="ISLAND-G", voltage_level_id="ISLAND-VL", bus_id="ISLAND-B", target_p=10.0,
                              target_v=110.0, voltage_regulator_on=True, min_p=0.0, max_p=50.0)
    network.create_extensions("detail", id="ISLAND-L", fixed_p0=0.0, variable_p0=10.0, fixed_q0=0.0, variable_q0=2.0)

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    assert network.get_loads().loc["ISLAND-L", ["p0", "q0"]].tolist() == [10.0, 2.0]
    assert model.scaled is True
    assert {entry["area"] for entry in model.scaled_entity} == {"BE-0", "NL-0"}


@pytest.mark.pypowsybl
def test_scale_balance_scales_areas_with_schedule_when_another_area_has_none(scalable_microgrid):
    model = scalable_microgrid()
    before = _loads(model.network)

    model = scaler.scale_balance(model, [BE_IMPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    after = _loads(model.network)
    assert _ac_net_positions(model.network)["BE"] == pytest.approx(-285.0, abs=BALANCE_THRESHOLD)
    assert after[before.area == "NL"][["p0", "q0"]].equals(before[before.area == "NL"][["p0", "q0"]])


@pytest.mark.pypowsybl
def test_scale_balance_uses_largest_duplicate_schedule_and_ignores_nan_domain_strings(scalable_microgrid):
    model = scalable_microgrid()
    ac_schedules = [
        {"value": 0.0, "in_domain": "BE", "out_domain": "NaN"},
        {"value": 285.0, "in_domain": "BE", "out_domain": "NaN"},
        {"value": 285.0, "in_domain": "nan", "out_domain": "NL"},
        {"value": 0.0, "in_domain": "", "out_domain": "NL"},
    ]

    model = scaler.scale_balance(model, ac_schedules, [FOREIGN_HVDC_SCHEDULE], debug=False)

    acnp = _ac_net_positions(model.network)
    assert acnp["BE"] == pytest.approx(-285.0, abs=BALANCE_THRESHOLD)
    assert acnp["NL"] == pytest.approx(285.0, abs=BALANCE_THRESHOLD)


@pytest.mark.pypowsybl
@pytest.mark.parametrize("in_domain, out_domain, expected_p0", [("GB", "BE", 100.0), ("BE", "GB", -100.0)])
def test_scale_balance_sets_hvdc_boundary_lines_to_dc_schedule(scalable_microgrid, in_domain, out_domain, expected_p0):
    model = scalable_microgrid(extra_boundary_lines=[{"id": "BE-HVDC", "p0": 0.0, "eic": "10T-BE-GB-HVDC01"}])
    dc_schedules = [{"value": 100.0, "in_domain": in_domain, "out_domain": out_domain,
                     "registered_resource": "10T-BE-GB-HVDC01", "hvdc_name": "BE-GB"}]

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], dc_schedules, debug=False)

    assert model.network.get_boundary_lines().loc["BE-HVDC", "p0"] == pytest.approx(expected_p0)
    assert model.scaled_hvdc == [{"name": "10T-BE-GB-HVDC01", "prescale_setpoint": 0.0, "postscale_setpoint": expected_p0}]
    assert _ac_net_positions(model.network)["BE"] == pytest.approx(-285.0, abs=BALANCE_THRESHOLD)


@pytest.mark.pypowsybl
def test_scale_balance_raises_when_hvdc_link_in_model_has_no_schedule_value(scalable_microgrid):
    model = scalable_microgrid(extra_boundary_lines=[{"id": "BE-HVDC", "p0": 0.0, "eic": "10T-BE-GB-HVDC01"}])
    dc_schedules = [{"value": None, "in_domain": "GB", "out_domain": "BE",
                     "registered_resource": "10T-BE-GB-HVDC01", "hvdc_name": "BE-GB"}]

    with pytest.raises(ValueError, match="10T-BE-GB-HVDC01"):
        scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], dc_schedules, debug=False)


@pytest.mark.pypowsybl
def test_scale_balance_aligns_island_net_position_through_unpaired_lines_proportionally_to_their_flow(scalable_microgrid):
    model = scalable_microgrid(extra_boundary_lines=[{"id": "BE-X1", "p0": -20.0}, {"id": "BE-X2", "p0": -60.0}])
    # island (BE + NL) imports 80 MW over the unpaired lines, the schedules sum to an import of 50 MW
    ac_schedules = [{**BE_IMPORTS_285, "value": 300.0}, {**NL_EXPORTS_285, "value": 250.0}]

    model = scaler.scale_balance(model, ac_schedules, [FOREIGN_HVDC_SCHEDULE], debug=False)

    p0 = model.network.get_boundary_lines().loc[["BE-X1", "BE-X2"], "p0"]
    assert p0.sum() == pytest.approx(-50.0, abs=0.1)
    assert (p0["BE-X2"] + 60.0) == pytest.approx(3 * (p0["BE-X1"] + 20.0))
    acnp = _ac_net_positions(model.network)
    assert acnp["BE"] == pytest.approx(-300.0, abs=BALANCE_THRESHOLD)
    assert acnp["NL"] == pytest.approx(250.0, abs=BALANCE_THRESHOLD)


@pytest.mark.pypowsybl
@pytest.mark.xfail(strict=True, reason="unpaired AC boundary lines with 0 MW give 0/0 participation, NaN is written to p0")
def test_scale_balance_does_not_write_nan_to_unpaired_lines_without_flow(scalable_microgrid):
    model = scalable_microgrid(extra_boundary_lines=[{"id": "BE-X1", "p0": 0.0}])
    ac_schedules = [{**BE_IMPORTS_285, "value": 300.0}, {**NL_EXPORTS_285, "value": 250.0}]

    model = scaler.scale_balance(model, ac_schedules, [FOREIGN_HVDC_SCHEDULE], debug=False)

    assert model.network.get_boundary_lines()["p0"].map(math.isfinite).all()


@pytest.mark.pypowsybl
@pytest.mark.xfail(strict=True, raises=pl.exceptions.ColumnNotFoundError,
                   reason="scale_balance reads boundary 'isHvdc' without the guard of get_network_elements, "
                          "networks without HVDC boundary points fail")
def test_scale_balance_works_on_networks_without_hvdc_boundary_points(scalable_microgrid):
    model = scalable_microgrid(hvdc_property=False)

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    assert model.scaled is True


@pytest.mark.pypowsybl
@pytest.mark.xfail(strict=True, reason="report keeps only the first and last row, so 'initial-offset-acnp' is never set")
def test_scaling_report_keeps_initial_and_final_offset_per_area(scalable_microgrid):
    model = scalable_microgrid()

    model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    be_report = next(entry for entry in model.scaled_entity if entry["area"] == "BE-0")
    assert {"prescale_acnp", "initial_offset_acnp", "final_offset_acnp"} <= be_report.keys()
    assert be_report["initial_offset_acnp"] == pytest.approx(be_report["prescale_acnp"] + 285.0, abs=0.2)
    assert abs(be_report["final_offset_acnp"]) <= BALANCE_THRESHOLD


@pytest.mark.pypowsybl
def test_scale_balance_stops_after_max_iteration_scaling_rounds(scalable_microgrid):
    model = scalable_microgrid()
    run_ac = mock.Mock(wraps=pp.loadflow.run_ac)

    # a negative threshold is never met, so every allowed iteration runs
    with mock.patch.object(scaler, "MAX_ITERATION", "3"), mock.patch.object(scaler, "BALANCE_THRESHOLD", "-1"), \
            mock.patch.object(scaler.pp.loadflow, "run_ac", run_ac):
        model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    # initial load flow + island alignment load flow + one per iteration
    assert run_ac.call_count == 2 + 3


@pytest.mark.pypowsybl
def test_scale_balance_reports_areas_still_above_threshold_as_unsuccessful(scalable_microgrid):
    model = scalable_microgrid()
    before = _loads(model.network)

    with mock.patch.object(scaler, "MAX_ITERATION", "0"):
        model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    assert {entry["area"]: entry["success"] for entry in model.scaled_entity} == {"BE-0": False, "NL-0": False}
    assert all(abs(entry["final_offset_acnp"]) > BALANCE_THRESHOLD for entry in model.scaled_entity)
    assert _loads(model.network)[["p0", "q0"]].equals(before[["p0", "q0"]])


@pytest.mark.pypowsybl
def test_scale_balance_marks_model_not_scaled_when_main_island_diverges(scalable_microgrid):
    model = scalable_microgrid()
    before = _loads(model.network)
    real_run_ac = pp.loadflow.run_ac
    calls = []

    def run_ac(network, parameters):
        calls.append(parameters)
        if len(calls) == 1:
            return real_run_ac(network=network, parameters=parameters)
        return [SimpleNamespace(connected_component_num=0, status=pp.loadflow.ComponentStatus.MAX_ITERATION_REACHED)]

    with mock.patch.object(scaler.pp.loadflow, "run_ac", side_effect=run_ac):
        model = scaler.scale_balance(model, [BE_IMPORTS_285, NL_EXPORTS_285], [FOREIGN_HVDC_SCHEDULE], debug=False)

    assert model.scaled is False
    assert _loads(model.network)[["p0", "q0"]].equals(before[["p0", "q0"]])
