# ChronoQuant Commands

Run commands from the repo root.

## Tests

```bash
pytest tests/
pytest tests/test_modeling_metrics.py
pytest tests/test_smoke.py::test_config_loads
```

## App

```bash
streamlit run src/streamlit_app/main.py
```

## Data Pipeline

```bash
python scripts/sync_ohlcv.py
python scripts/rebuild_derived_tables.py --drop
python scripts/rebuild_derived_tables.py --features-only --asset-id solusdt_fw60
```

## Modeling

```bash
python scripts/create_sample_splits.py --sample-id base_fw240_dev
python scripts/create_sample_splits.py --sample-id base_solusdt_fw60_dev --asset-id solusdt_fw60 --target-horizon-minutes 60
python scripts/train_model.py --model-id logit_l_fw240_q90_pval_v1
```

## Backtesting

```bash
python scripts/backtest_strategy.py lasso_long_fw240_q90_managed_v1
```
