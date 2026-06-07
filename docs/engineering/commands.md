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

## LightGBM Distribution Search

```bash
# Smoke test (5 trial, 2 fold, ~5 perc)
python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage smoke

# Explore (60 trial, mind 5 fold, ~2-3 ora)
python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage explore --n-trials 60

# Refine (30 trial, row_stride=10, ~2-4 ora)
python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_local_v2 --stage refine --n-trials 30
```

## Strategy Threshold Sweep

```bash
python scripts/sweep_strategy.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v2 \
    --asset-id solusdt_fw60 \
    --start 2024-01-01 --end 2025-12-31 \
    --side long --top-n 20
```

## Model Promotion (lasd docs/engineering/lgbm_model_development.md)

```bash
# 1. Final fit manualis script (lasd doksit)
# 2. Config frissites: models.json + env.json
# 3. Predictions sync (csak az utolso 7 nap!)
# 4. Strategy sweep + strategies.json frissites
# 5. UI ellenorzes: load_dashboard_config(asset_id='solusdt_fw60')
```
