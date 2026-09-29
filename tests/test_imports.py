import importlib
import py_compile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Entrypoint scripts connect to services or run a whole pipeline at import, so they are only compiled
SCRIPTS = {
    "emf.model_merger.worker",
    "emf.model_quality.worker",
    "emf.model_retriever.local_worker",
    "emf.model_retriever.opdm_worker",
    "emf.model_validator.worker",
    "emf.report_publisher.worker",
    "emf.schedule_retriever.worker",
    "emf.task_generator.worker",
    "emf.layout_generator.geo_profile_generator",
}

# Known import failures, strict xfail turns red once fixed so the entry gets removed
KNOWN_BROKEN = {
    "emf.common.loadflow_tool.local_file_import": (ImportError, "imports emf.model_validator.validate_model, which does not exist"),
    "emf.common.xslt_engine.saxonpy_api": (RuntimeError, "connects to RabbitMQ at import"),
    "emf.layout_generator.geo_functions": (ImportError, "geopandas, shapely and thefuzz are not declared in pyproject.toml"),
}


def _module_name(path: Path) -> str:
    return ".".join(path.relative_to(REPO_ROOT).with_suffix("").parts).removesuffix(".__init__")


ALL_MODULES = sorted(_module_name(path) for path in (REPO_ROOT / "emf").rglob("*.py"))


@pytest.mark.parametrize("module_name", [
    pytest.param(name, marks=pytest.mark.xfail(raises=KNOWN_BROKEN[name][0], reason=KNOWN_BROKEN[name][1], strict=True))
    if name in KNOWN_BROKEN else name
    for name in ALL_MODULES if name not in SCRIPTS
])
def test_module_imports(module_name):
    importlib.import_module(module_name)


@pytest.mark.parametrize("module_name", sorted(SCRIPTS))
def test_script_compiles(module_name):
    py_compile.compile(str(REPO_ROOT.joinpath(*module_name.split(".")).with_suffix(".py")), doraise=True)
