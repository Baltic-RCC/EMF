"""
Shared setup for the issue #474 (polars migration) test suite.

What this file does, in order:
1. Puts the repository root on sys.path and makes it the working directory, because
   `config` and `emf` are imported as top-level packages and config paths are relative.
2. Sets placeholder service URLs so module-level clients can be constructed.
3. Replaces the Elastic and MinIO client classes with MagicMock before any EMF module that
   builds them at import time is loaded (object_storage/__init__.py does this). No network
   access is needed to run the suite.
4. Exposes the pandas reference implementations (tests/polars_474/oracle/*_pd.py), extracted
   from git history, as fixtures so every polars function can be compared with the pandas
   behaviour it replaced.
"""
import importlib.util
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
ORACLE_DIR = Path(__file__).resolve().parent / "oracle"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

os.environ.setdefault("ELK_SERVER", "https://localhost:9200")
os.environ.setdefault("MINIO_SERVER", "localhost:9000")

import emf.common.integrations.elastic as _elastic  # noqa: E402
import emf.common.integrations.minio_api as _minio_api  # noqa: E402

_elastic.Elastic = mock.MagicMock(name="Elastic")
_minio_api.ObjectStorage = mock.MagicMock(name="ObjectStorage")

_ORACLE_CACHE = {}


def load_oracle(name: str):
    """Load tests/polars_474/oracle/<name>.py as an isolated module (cached)."""
    if name not in _ORACLE_CACHE:
        path = ORACLE_DIR / f"{name}.py"
        spec = importlib.util.spec_from_file_location(f"oracle_{name}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _ORACLE_CACHE[name] = module
    return _ORACLE_CACHE[name]


@pytest.fixture(scope="session")
def repo_root():
    return REPO_ROOT


@pytest.fixture(scope="session")
def replacement_pl():
    from emf.model_merger import replacement
    return replacement


@pytest.fixture(scope="session")
def replacement_pd():
    return load_oracle("replacement_pd")


@pytest.fixture(scope="session")
def scaler_pl():
    from emf.model_merger import scaler
    return scaler


@pytest.fixture(scope="session")
def scaler_pd():
    return load_oracle("scaler_pd")


@pytest.fixture(scope="session")
def validator_pl():
    from emf.model_validator import validator_functions
    return validator_functions


@pytest.fixture(scope="session")
def validator_pd():
    module = load_oracle("validator_pd")
    # The pandas original only accepted OPDM objects; tests feed triplets directly.
    module.load_opdm_objects_to_triplets = lambda opdm_objects, **_: opdm_objects
    return module


@pytest.fixture(scope="session")
def post_processing_pl():
    from emf.model_merger import post_processing
    return post_processing


@pytest.fixture(scope="session")
def post_processing_pd():
    return load_oracle("post_processing_pd")


def pytest_configure(config):
    for marker, text in [
        ("inventory", "static checks that every place of the migration is found and wired"),
        ("parity", "polars result compared with the pandas implementation it replaced"),
        ("integration", "several migrated components used together through their real callers"),
        ("errors", "error handling, missing and malformed input"),
        ("config", "configuration and dependency checks"),
        ("performance", "timing comparison pandas vs polars (slow)"),
    ]:
        config.addinivalue_line("markers", f"{marker}: {text}")
