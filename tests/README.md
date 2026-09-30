# Tests

```
uv sync
uv run pytest                                          # offline tests, integration tests are deselected by default
uv run pytest -m "not integration and not pypowsybl"   # skip tests that run the pypowsybl engine
uv run pytest -m integration                           # needs real RabbitMQ/Elasticsearch/MinIO, see below
```

## Integration tests
`tests/integration/` runs against real services. CI runs them nightly (`.github/workflows/ci-integration-tests.yml`). Locally:
```
docker compose -f tests/integration/docker-compose.yml up -d --wait
RMQ_USERNAME=guest RMQ_PASSWORD=guest MINIO_USERNAME=minioadmin MINIO_PASSWORD=minioadmin uv run pytest -m integration
docker compose -f tests/integration/docker-compose.yml down -v
```
Config values are read at import time, so set the env variables before pytest starts.

## Rules
- Test files mirror the `emf/` layout, e.g. `tests/model_merger/test_replacement.py`.
- Unit tests must not reach the network, `conftest.py` blocks socket connections. Mark tests that need real services with `@pytest.mark.integration`.
- Never commit real TSO models. Build test models from pypowsybl built-in networks or public ENTSO-E test configurations.

## What `conftest.py` provides
- Env overrides for the placeholder values in `config/**/*.properties`, so every `emf` module can be imported offline.
- MinIO login and the ELK log handler are patched out.
- The logging context set by `HandlerMergeModels` is reset between tests.
- Fixtures:
  - `ieee14_igm`: OPDM object built from pypowsybl IEEE 14, loads and solves without a boundary set.
  - `igm_factory`: same OPDM object shape from any pypowsybl built-in network, e.g. `igm_factory("create_ieee9", tso="AST", version="002")`.
  - `microgrid_be_igm`, `microgrid_nl_igm`, `microgrid_boundary`: ENTSO-E CGMES conformity MicroGrid BaseCase (BE, NL, boundary set).
    Owned and provided by ENTSO-E, downloaded on first use from powsybl-core into `~/.cache/emfos-tests` (`EMFOS_TEST_DATA` overrides).
    Tests using them are skipped when the files can't be downloaded; offline, copy the files into that folder.
  - `make_triplets`: builds a triplets DataFrame from `(ID, KEY, VALUE)` rows for dummy data tests.
  - `merge_task`: fresh copy of `examples/merge_task_example.json`.
  - `real_minio_login`, `real_elk_logging_handler`: undo the conftest patches for tests of that code itself.
- Every fixture returns a fresh object, so tests can modify it.

## Known bugs
Tests for confirmed bugs are marked `@pytest.mark.xfail(strict=True, reason=...)`. They show as `xfailed` while the bug
exists and turn red once it's fixed, which is the reminder to remove the marker. `uv run pytest -rx` lists them.
