# Evaluation Report: SOLUSDT Local v3

This page indexes the strategy evidence for the v3 SOLUSDT long/short models.

## Models

| Side | Model ID | Model Card |
|---|---|---|
| Long | `lgbm_solusdt_l_fw60_q90_local_v3` | `../../modeling/model_cards/lgbm_solusdt_l_fw60_q90_local_v3.md` |
| Short | `lgbm_solusdt_s_fw60_q10_local_v3` | `../../modeling/model_cards/lgbm_solusdt_s_fw60_q10_local_v3.md` |

## Strategy Artifacts

| Artifact | Path |
|---|---|
| Long sweep | `backtests/sweep_lgbm_solusdt_l_fw60_q90_local_v3.csv` |
| Short sweep | `backtests/sweep_lgbm_solusdt_s_fw60_q10_local_v3.csv` |
| Long backtest directory | `backtests/solusdt_long_fw60_q90_local_v3/` |
| Short backtest directory | `backtests/solusdt_short_fw60_q10_local_v3/` |

## Documentation Rule

Keep final selected trigger parameters in `config/strategies.json`. Keep
detailed generated reports in `backtests/`. Use this file for the human
decision summary and links.

