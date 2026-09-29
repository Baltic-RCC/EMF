"""
Configuration and dependency checks for the polars migration.

Production images are built from pyproject.toml + uv.lock (docker/Dockerfile: uv export / uv pip
sync). requirements.txt is what a developer is likely to install in PyCharm.
"""
import configparser
import inspect
import re
import tomllib
from importlib import metadata

import polars as pl
import pytest

pytestmark = pytest.mark.config

# API -> first polars release that has it (verified by installing 1.0.0 ... 1.25.2)
JOIN_MAINTAIN_ORDER_MIN = (1, 17, 1)


def _version_tuple(text):
    return tuple(int(x) for x in re.findall(r"\d+", text)[:3])


@pytest.fixture(scope="module")
def pyproject(repo_root):
    return tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def lock(repo_root):
    return tomllib.loads((repo_root / "uv.lock").read_text(encoding="utf-8"))


def _declared(pyproject, name):
    for dep in pyproject["project"]["dependencies"]:
        if re.match(rf"^{re.escape(name)}\b", dep):
            return dep
    return None


def _locked_versions(lock, name):
    return [p["version"] for p in lock["package"] if p["name"] == name]


@pytest.mark.parametrize("package", ["polars", "pyarrow", "triplets", "pandas"])
def test_runtime_dependencies_declared(pyproject, package):
    assert _declared(pyproject, package), f"{package} missing from pyproject.toml dependencies"


def test_polars_locked(lock):
    assert _locked_versions(lock, "polars"), "polars not in uv.lock -> docker image would not get it"


def test_installed_polars_matches_lock(lock):
    """The environment running these tests should be the one production uses."""
    assert metadata.version("polars") in _locked_versions(lock, "polars")


def test_polars_lower_bound_supports_used_api(pyproject):
    """
    scaler.py calls DataFrame.join(..., maintain_order=...), not available before polars 1.17.
    'polars>=1.0' lets pip/uv resolve a version where scaling fails with TypeError at runtime.
    """
    uses_maintain_order = "maintain_order" in inspect.signature(pl.DataFrame.join).parameters
    assert uses_maintain_order, "installed polars too old for scaler.py"
    spec = _declared(pyproject, "polars")
    lower = _version_tuple(spec.split(">=")[1]) if ">=" in spec else (0, 0, 0)
    lower = lower + (0,) * (3 - len(lower))
    assert lower >= JOIN_MAINTAIN_ORDER_MIN, f"pyproject declares '{spec}', code needs >= 1.17.1"


def test_triplets_has_polars_engine():
    """validator_functions / post_processing rely on triplets' polars namespace (triplets >= 0.1)."""
    import triplets  # noqa: F401
    assert hasattr(pl.DataFrame({"ID": ["a"], "KEY": ["Type"], "VALUE": ["X"], "INSTANCE_ID": ["i"]}), "triplets")
    assert _version_tuple(metadata.version("triplets")) >= (0, 1, 0)


def test_requirements_txt_consistent_with_lock(repo_root, lock):
    """
    requirements.txt is UTF-16 and pins an older stack without polars. Anyone installing from it
    (e.g. PyCharm's 'install requirements' prompt) gets ImportError: polars, or pypowsybl 1.11.
    """
    raw = (repo_root / "requirements.txt").read_bytes()
    text = raw.decode("utf-16") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode("utf-8")
    pins = dict(re.findall(r"^([A-Za-z0-9_.\-]+)==([^\s;]+)", text, flags=re.M))
    problems = []
    if "polars" not in pins:
        problems.append("polars missing")
    for package in ("pypowsybl", "triplets", "pandas", "pyarrow"):
        locked = _locked_versions(lock, package)
        if package in pins and pins[package] not in locked:
            problems.append(f"{package}=={pins[package]} but uv.lock has {locked}")
    assert not problems, "requirements.txt out of date: " + "; ".join(problems)


@pytest.mark.parametrize("rel_path,keys", [
    ("config/cgm_worker/scaler.properties",
     ["MAX_ITERATION", "BALANCE_THRESHOLD", "CONSTANT_POWER_FACTOR", "POWER_FACTOR_THRESHOLD"]),
    ("config/cgm_worker/post_processing.properties", ["FIX_INJECTION_ERRORS", "INJECTION_THRESHOLD", "SMALL_ISLAND_SIZE"]),
    ("config/model_validator/model_validator.properties",
     ["CHECK_NON_RETAINED_SWITCHES", "CHECK_KIRCHHOFF_FIRST_LAW", "OPEN_NON_RETAINED_SWITCHES", "MODIFY_DK_REGIONS"]),
])
def test_properties_read_by_migrated_code_exist_and_parse(repo_root, rel_path, keys):
    import json
    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read(repo_root / rel_path)
    section = dict(parser.items("MAIN"))
    for key in keys:
        assert key in section, f"{key} missing in {rel_path}"
        value = section[key].strip()
        if value.lower() in ("true", "false"):
            json.loads(value.lower())
        else:
            float(value)


def test_replacement_config_covers_all_time_horizons(replacement_pl):
    cfg = replacement_pl.replacement_config
    for horizon in ["ID", "1D", "2D", "WK", "MO", "YR"]:
        assert "replacement_length" in cfg["time_horizons"][horizon]
        assert cfg["time_horizons"][horizon]["request_list"]
    assert {h["hour"] for h in cfg["hours"]} >= {f"{h:02d}:30" for h in range(24)}
    assert {d["day"] for d in cfg["days"]} == set(range(7))
