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
import re
import socket
import urllib.request
import uuid
import zipfile
from functools import cache
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DATA_DIR = Path(os.environ.get("EMFOS_TEST_DATA", Path.home() / ".cache" / "emfos-tests"))

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
_minio_login_patch = mock.patch.object(minio_api.ObjectStorage, "_get_credentials", return_value=FAKE_MINIO_CREDENTIALS)
_elk_handler_patch = mock.patch.object(custom_logger, "get_elk_logging_handler",
                                       new=lambda: mock.MagicMock(spec=custom_logger.ElkLoggingHandler))
_minio_login_patch.start()
_elk_handler_patch.start()


@pytest.fixture(autouse=True)
def _allow_network_for_integration_tests(request, monkeypatch):
    if request.node.get_closest_marker("integration"):
        monkeypatch.setattr(socket.socket, "connect", _socket_connect)


@pytest.fixture(autouse=True)
def _reset_logging_context():
    # HandlerMergeModels sets the log context of its task and never clears it
    token = custom_logger.log_context.set({})
    yield
    custom_logger.log_context.reset(token)


@pytest.fixture
def real_minio_login():
    """Restores the real ObjectStorage._get_credentials for tests of the MinIO login itself"""
    _minio_login_patch.stop()
    yield
    _minio_login_patch.start()


@pytest.fixture
def real_elk_logging_handler():
    """Restores the real custom_logger.get_elk_logging_handler for tests of the ELK log handler itself"""
    _elk_handler_patch.stop()
    yield
    _elk_handler_patch.start()


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


# ENTSO-E CGMES 2.4.15 conformity MicroGrid BaseCase, owned and provided by ENTSO-E, downloaded from powsybl-core.
# Not committed to the repo, cached in TEST_DATA_DIR instead. Offline, put the files there manually.
MICROGRID_URL = ("https://raw.githubusercontent.com/powsybl/powsybl-core/v6.8.0/cgmes/cgmes-conformity/src/main/resources/"
                 "conformity/cas-1.1.3-data-4.0.3/MicroGrid/BaseCase/")
MICROGRID_FILES = {
    "BE": ("CGMES_v2.4.15_MicroGridTestConfiguration_BC_BE_v2",
           {p: f"MicroGridTestConfiguration_BC_BE_{p}_V2.xml" for p in ("EQ", "SSH", "TP", "SV")}),
    "NL": ("CGMES_v2.4.15_MicroGridTestConfiguration_BC_NL_v2",
           {p: f"MicroGridTestConfiguration_BC_NL_{p}_V2.xml" for p in ("EQ", "SSH", "TP", "SV")}),
    "BD": ("CGMES_v2.4.15_MicroGridTestConfiguration_BD_v2",
           {"EQ_BD": "MicroGridTestConfiguration_EQ_BD.xml", "TP_BD": "MicroGridTestConfiguration_TP_BD.xml"}),
}
MICROGRID_SCENARIO_TIME = "20140601T1030Z"


def _microgrid_file(folder: str, file_name: str) -> bytes:
    path = TEST_DATA_DIR / "microgrid" / file_name
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        socket.socket.connect = _socket_connect
        try:
            urllib.request.urlretrieve(f"{MICROGRID_URL}{folder}/{file_name}", path.with_suffix(".part"))
            path.with_suffix(".part").rename(path)
        except OSError as error:
            pytest.skip(f"MicroGrid test models not available, download them into {path.parent}: {error}")
        finally:
            socket.socket.connect = _blocked_connect
    return path.read_bytes()


def _microgrid_object(area: str, tso: str) -> dict:
    folder, files = MICROGRID_FILES[area]
    components = []
    for profile, source_name in files.items():
        xml = _microgrid_file(folder, source_name)
        if area == "BD":
            file_name = f"{MICROGRID_SCENARIO_TIME[:9]}0000Z__ENTSOE_{profile.replace('_', '')}_001"
        else:
            file_name = f"{MICROGRID_SCENARIO_TIME}_1D_{tso}_{profile}_001"
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as profile_zip:
            profile_zip.writestr(f"{file_name}.xml", xml)
        components.append({"opdm:Profile": {"pmd:cgmesProfile": profile, "pmd:fileName": f"{file_name}.zip", "DATA": data.getvalue()}})

    key_xml = _microgrid_file(folder, files["EQ_BD" if area == "BD" else "SV"]).decode("utf-8")
    return {
        "opde:Id": str(uuid.uuid4()),
        "opde:Object-Type": "BDS" if area == "BD" else "IGM",
        "data-source": "OPDM",
        "pmd:TSO": tso,
        "pmd:modelPartReference": tso,
        "pmd:timeHorizon": "" if area == "BD" else "1D",
        "pmd:scenarioDate": re.search(r"<md:Model.scenarioTime>([^<]+)<", key_xml).group(1).rstrip("Z") + "Z",
        "pmd:versionNumber": "001",
        "pmd:fullModel_ID": re.search(r'<md:FullModel rdf:about="(?:urn:uuid:)?([^"]+)"', key_xml).group(1),
        "opde:Component": components,
    }


@pytest.fixture
def microgrid_be_igm():
    """MicroGrid BE IGM (ELIA), needs microgrid_boundary to load into pypowsybl"""
    return _microgrid_object("BE", "ELIA")


@pytest.fixture
def microgrid_nl_igm():
    """MicroGrid NL IGM (TENNET), needs microgrid_boundary to load into pypowsybl"""
    return _microgrid_object("NL", "TENNET")


@pytest.fixture
def microgrid_boundary():
    """MicroGrid boundary set (BDS) matching microgrid_be_igm and microgrid_nl_igm"""
    return _microgrid_object("BD", "ENTSOE")


@pytest.fixture
def make_triplets():
    """Builds triplets from (ID, KEY, VALUE) rows, e.g. make_triplets([("sw1", "Type", "Breaker")])"""
    def _make_triplets(rows, instance_id: str = "test-instance"):
        return pd.DataFrame(rows, columns=["ID", "KEY", "VALUE"]).assign(INSTANCE_ID=instance_id)
    return _make_triplets


@pytest.fixture
def merge_task():
    return json.loads((REPO_ROOT / "examples" / "merge_task_example.json").read_text())
