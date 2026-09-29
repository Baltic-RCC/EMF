# Tests

```
uv sync
uv run pytest                                          # offline tests, integration tests are deselected by default
uv run pytest -m "not integration and not pypowsybl"   # skip tests that run the pypowsybl engine
uv run pytest -m integration                           # needs real RabbitMQ/Elasticsearch/MinIO, configured via env variables
```

## Rules
- Test files mirror the `emf/` layout, e.g. `tests/model_merger/test_replacement.py`.
- Unit tests must not reach the network, `conftest.py` blocks socket connections. Mark tests that need real services with `@pytest.mark.integration`.
- Never commit real TSO models. Build test models from pypowsybl built-in networks or public ENTSO-E test configurations.

## What `conftest.py` provides
- Env overrides for the placeholder values in `config/**/*.properties`, so every `emf` module can be imported offline.
- MinIO login and the ELK log handler are patched out.
- Fixtures:
  - `ieee14_igm`: OPDM object built from pypowsybl IEEE 14, loads and solves without a boundary set.
  - `micro_grid_be_igm`: ENTSO-E conformity MicroGrid BE, needs a boundary set to load into pypowsybl, so use it as triplets only.
  - `igm_factory`: same OPDM object shape from any pypowsybl built-in network, e.g. `igm_factory("create_ieee9", tso="AST", version="002")`.
  - `merge_task`: fresh copy of `examples/merge_task_example.json`.
