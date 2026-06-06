# ChronoQuant Testing

## Default

Run the full test suite from the repo root:

```bash
pytest tests/
```

## Focused Runs

Use focused tests while changing a narrow behavior:

```bash
pytest tests/test_smoke.py::test_config_loads
pytest tests/test_modeling_metrics.py
```

## Expectations

- Add or update focused tests when changing shared behavior.
- Prefer deterministic fixtures for modeling, sampling, metrics, and cutoff logic.
- For data rebuild or training changes, verify table uniqueness and artifact outputs
  in addition to unit tests.
