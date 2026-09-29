"""
Shared setup for all tests. Module level code below runs before any test module imports `emf`, which matters because:
- config values from config/**/*.properties are bound at import time, placeholders are overridden via env variables
- importing emf.common.integrations.object_storage creates a MinIO client, whose login is an HTTP call
- HandlerMergeModels attaches an ELK log handler, which sends every log record over HTTP
Unit tests must not open network connections, tests marked with `integration` are exempt.
"""
import io
import json
import os
import socket
import uuid
import zipfile
from functools import cache
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

TEST_ENVIRONMENT = {
    "ELK_SERVER": "http://localhost:9200",
    "ELK_SSL_VERIFY": "False",
    "RMQ_SERVER": "localhost",
    "RMQ_PORT": "5672",
    "MINIO_SERVER": "localhost:9000",
}
for key, value in TEST_ENVIRONMENT.items():
    os.environ.setdefault(key, value)

_socket_connect = socket.socket.connect


def _blocked_connect(self, address):
    raise RuntimeError(f"Network access blocked in unit tests (tried {address}), mark the test with @pytest.mark.integration")


socket.socket.connect = _blocked_connect

from emf.common.integrations import minio_api
from emf.common.logging import custom_logger

FAKE_MINIO_CREDENTIALS = {
    "AccessKeyId": "test",
    "SecretAccessKey": "test",
    "SessionToken": "test",
    "Expiration": "2099-01-01T00:00:00Z",
}
mock.patch.object(minio_api.ObjectStorage, "_get_credentials", return_value=FAKE_MINIO_CREDENTIALS).start()
mock.patch.object(custom_logger, "get_elk_logging_handler",
                  new=lambda: mock.MagicMock(spec=custom_logger.ElkLoggingHandler)).start()


@pytest.fixture(autouse=True)
def _allow_network_for_integration_tests(request, monkeypatch):
    if request.node.get_closest_marker("integration"):
        monkeypatch.setattr(socket.socket, "connect", _socket_connect)


@cache
def _export_cgmes_profiles(network_name: str) -> dict[str, bytes]:
    import pypowsybl as pp
    network = getattr(pp.network, network_name)()
    pp.loadflow.run_ac(network)
    buffer = network.save_to_binary_buffer(format="CGMES")
    with zipfile.ZipFile(io.BytesIO(buffer.getvalue())) as export:
        # exported names are like "file_EQ.xml"
        return {name.rsplit("_", 1)[-1].removesuffix(".xml"): export.read(name) for name in export.namelist()}


def _opdm_object_from_network(network_name: str,
                              tso: str = "TSO",
                              time_horizon: str = "1D",
                              version: str = "001",
                              scenario_time: str = "20250706T0930Z") -> dict:
    """OPDM metadata object as retrieved from OPDM/MinIO, each profile is a zipped CGMES file in DATA"""
    components = []
    for profile, xml in _export_cgmes_profiles(network_name).items():
        file_name = f"{scenario_time}_{time_horizon}_{tso}_{profile}_{version}"
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as profile_zip:
            profile_zip.writestr(f"{file_name}.xml", xml)
        components.append({"opdm:Profile": {
            "pmd:cgmesProfile": profile,
            "pmd:fileName": f"{file_name}.zip",
            "pmd:content-reference": f"CGMES/{time_horizon}/{tso}/{scenario_time[:8]}/{scenario_time[9:13]}00/{profile}/{file_name}.zip",
            "DATA": data.getvalue(),
        }})

    sv_profile = next(c["opdm:Profile"] for c in components if c["opdm:Profile"]["pmd:cgmesProfile"] == "SV")
    return {
        "opde:Id": str(uuid.uuid4()),
        "opde:Object-Type": "IGM",
        "data-source": "OPDM",
        "minio-bucket": "opdm-data",
        "pmd:TSO": tso,
        "pmd:modelPartReference": tso,
        "pmd:timeHorizon": time_horizon,
        "pmd:scenarioDate": f"{scenario_time[:4]}-{scenario_time[4:6]}-{scenario_time[6:8]}T{scenario_time[9:11]}:{scenario_time[11:13]}:00Z",
        "pmd:versionNumber": version,
        "pmd:fullModel_ID": str(uuid.uuid4()),
        "pmd:content-reference": sv_profile["pmd:content-reference"],
        "opde:Component": components,
    }


@pytest.fixture
def igm_factory():
    """Builds an IGM from a pypowsybl built-in network, e.g. igm_factory("create_ieee9", tso="AST")"""
    return _opdm_object_from_network


@pytest.fixture
def ieee14_igm():
    """Self-contained IGM, loads into pypowsybl without a boundary set"""
    return _opdm_object_from_network("create_ieee14")


@pytest.fixture
def micro_grid_be_igm():
    """ENTSO-E conformity MicroGrid BE. References boundary set objects, so it does not load
    into pypowsybl on its own. Use it as triplets until a boundary set fixture is added."""
    return _opdm_object_from_network("create_micro_grid_be_network", tso="ELIA")


@pytest.fixture
def merge_task():
    return json.loads((REPO_ROOT / "examples" / "merge_task_example.json").read_text())
