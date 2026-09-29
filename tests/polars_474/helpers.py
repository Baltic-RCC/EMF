"""Builders and comparison helpers shared by the #474 test modules."""
import copy
import math
import random
from datetime import datetime, timedelta

import pandas as pd
import polars as pl
import pypowsybl as pp


# --------------------------------------------------------------------------- triplets

def triplets(rows, instance_id="INST"):
    """
    Build a pandas triplet table from (ID, KEY, VALUE[, INSTANCE_ID]) tuples with the same dtypes
    triplets' read_RDF produces in production (pyarrow strings, dictionary-encoded KEY/INSTANCE_ID).
    """
    import pyarrow as pa
    records = []
    for row in rows:
        if len(row) == 3:
            records.append((*row, instance_id))
        else:
            records.append(row)
    df = pd.DataFrame(records, columns=["ID", "KEY", "VALUE", "INSTANCE_ID"])
    dictionary = pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))
    return df.astype({"ID": "string[pyarrow]", "KEY": dictionary, "VALUE": "string[pyarrow]",
                      "INSTANCE_ID": dictionary})


def to_pl(df):
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)


def to_pd(df):
    return df.to_pandas() if isinstance(df, pl.DataFrame) else df


def _norm_value(value):
    """Normalise a triplet VALUE so '1', 1, 1.0 and '1.0' compare equal."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value)
    try:
        number = float(text)
        if math.isnan(number):
            return None
        return f"num:{round(number, 6)}"
    except ValueError:
        return f"str:{text}"


def triplet_set(df, with_instance=False):
    """Order-independent representation of a triplet table for equality checks."""
    df = to_pd(df)
    cols = ["ID", "KEY", "VALUE"] + (["INSTANCE_ID"] if with_instance else [])
    out = set()
    for row in df[cols].itertuples(index=False):
        values = list(row)
        values[2] = _norm_value(values[2])
        out.add(tuple(str(v) if i != 2 else v for i, v in enumerate(values)))
    return out


def assert_same_triplets(actual, expected, with_instance=False):
    a, e = triplet_set(actual, with_instance), triplet_set(expected, with_instance)
    missing, extra = sorted(e - a)[:15], sorted(a - e)[:15]
    assert a == e, f"triplets differ\n  missing (in pandas only): {missing}\n  extra (in polars only): {extra}"


# --------------------------------------------------------------------------- networks

def cgm_network(hvdc_line_index=None):
    """
    MicroGrid BE + NL merged into one network with subnetworks, the properties the scaler
    reads (isHvdc, lineEnergyIdentificationCodeEIC) and ConformLoad 'detail' extensions.
    """
    network = pp.network.create_micro_grid_be_network()
    network.merge([pp.network.create_micro_grid_nl_network()])
    boundary_lines = network.get_boundary_lines()
    ids = boundary_lines.index.tolist()
    is_hvdc = [""] * len(ids)
    eic = [f"10T-TEST-{i:04d}" for i in range(len(ids))]
    if hvdc_line_index is not None:
        is_hvdc[hvdc_line_index] = "true"
    network.add_elements_properties(id=ids, isHvdc=is_hvdc, lineEnergyIdentificationCodeEIC=eic)
    loads = network.get_loads()
    load_ids = loads[loads.p0 > 0].index.tolist()
    network.create_extensions(
        "detail", id=load_ids,
        fixed_p0=[0.0] * len(load_ids), variable_p0=loads.loc[load_ids, "p0"].tolist(),
        fixed_q0=[0.0] * len(load_ids), variable_q0=loads.loc[load_ids, "q0"].tolist(),
    )
    return network


class MergedModelStub:
    """Minimal stand-in for merge_functions.MergedModel used by scale_balance."""

    def __init__(self, network):
        self.network = network
        self.scaled = None
        self.scaled_entity = []
        self.scaled_hvdc = []


def ac_schedules(be=250.0, nl=250.0):
    """BE exports `be` MW (out_domain), NL imports `nl` MW (in_domain)."""
    return [
        {"value": be, "in_domain": None, "out_domain": "BE",
         "TimeSeries.in_Domain.party": None, "TimeSeries.out_Domain.party": "BE"},
        {"value": nl, "in_domain": "NL", "out_domain": None,
         "TimeSeries.in_Domain.party": "NL", "TimeSeries.out_Domain.party": None},
    ]


def dc_schedules(resource="10T-NOT-IN-MODEL", value=0.0, in_domain="BE", out_domain="GB"):
    return [{"value": value, "in_domain": in_domain, "out_domain": out_domain,
             "registered_resource": resource, "hvdc_name": "test"}]


def network_state(network):
    """Setpoints the scaler is allowed to change, rounded for comparison."""
    loads = network.get_loads()[["p0", "q0"]].round(4)
    bl = network.get_boundary_lines()[["p0", "q0"]].round(4)
    return loads.to_dict("index"), bl.to_dict("index")


def run_scaler(module, network, ac, dc, **kwargs):
    model = MergedModelStub(network)
    kwargs.setdefault("debug", False)
    return module.scale_balance(model=model, ac_schedules=copy.deepcopy(ac), dc_schedules=copy.deepcopy(dc), **kwargs)


def cgmes_triplets(network):
    """Export a pypowsybl network to CGMES (EQ, TP, SSH, SV) and parse to pandas triplets."""
    import triplets as _triplets  # noqa: F401  (registers pd.read_RDF)
    buffer = network.save_to_binary_buffer(format="CGMES")
    buffer.name = "model.zip"
    return pd.read_RDF([buffer])


# --------------------------------------------------------------------------- replacement data

TIME_HORIZONS = ["ID", "1D", "2D", "WK", "MO", "YR"] * 4 + [f"{h:02d}" for h in range(1, 25)]


def _first_monday_of_previous_month(dt):
    first_this = dt.replace(day=1)
    prev = (first_this - timedelta(days=1)).replace(day=1)
    return prev + timedelta(days=(0 - prev.weekday()) % 7)


def es_replacement_records(seed, tsos=("AST", "ELERING", "LITGRID", "PSE"), days_back=16, per_tso=60,
                           anchor="2026-09-29T10:30:00Z"):
    """
    Deterministic synthetic Elasticsearch documents like query_data() returns.
    Besides random documents, each TSO gets a few month-ahead candidates on the first Monday of
    the previous month (the MO special-handling rule in replacement_conf.json).
    """
    rng = random.Random(seed)
    anchor_dt = datetime.strptime(anchor, "%Y-%m-%dT%H:%M:%SZ")
    mo_day = _first_monday_of_previous_month(anchor_dt)
    records = []
    counter = 0
    for tso in tsos:
        scenarios = []
        for _ in range(per_tso):
            day = anchor_dt - timedelta(days=rng.randint(0, days_back))
            scenarios.append((day.replace(hour=rng.randint(0, 23), minute=30, second=0),
                              rng.choice(TIME_HORIZONS)))
        for hour in rng.sample([7, 8, 9, 10, 11, 12], 3):
            scenarios.append((mo_day.replace(hour=hour, minute=30, second=0), rng.choice(["1D", "2D"])))
        for scenario, horizon in scenarios:
            creation = scenario - timedelta(hours=rng.randint(1, 48), minutes=rng.randint(0, 59),
                                            seconds=rng.randint(0, 59))
            counter += 1
            records.append({
                "opde:Id": f"{tso}-{counter}",
                "pmd:TSO": tso,
                "pmd:scenarioDate": scenario.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pmd:timeHorizon": horizon,
                "pmd:versionNumber": str(rng.randint(1, 4)).zfill(3),
                "pmd:creationDate": creation.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "ac_net_position": round(rng.uniform(-900, 900), 1),
                "sum_conform_load": round(rng.uniform(500, 4000), 1),
                "valid": True,
                "data-source": "OPDM",
            })
    return records


def fake_query_data(records):
    """Mimic object_storage.models.query_data filtering by TSO list."""

    def _query(query, query_filter=None, *args, **kwargs):
        tsos = query.get("pmd:TSO.keyword", [])
        return [copy.deepcopy(r) for r in records if r["pmd:TSO"] in tsos]

    return _query


def identity_get_content(model, *args, **kwargs):
    return model
