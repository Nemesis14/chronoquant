# Model Card: `lgbm_solusdt_s_fw60_q10_local_v3`

## Summary

| Field | Value |
|---|---|
| Model ID | `lgbm_solusdt_s_fw60_q10_local_v3` |
| Asset ID | `solusdt_fw60` |
| Side | Short |
| Target | `trg_s_fw60_q10` |
| Model family | LightGBM binary classifier |
| Status | Active in `config/models.json` at the time this page was created |

## Intended Use

Estimate whether a SOLUSDT 1-minute bar is followed by a strong downward
60-minute event. The probability is consumed by strategy evaluation, dashboard,
and runtime trading logic.

## Data And Training

| Field | Value |
|---|---|
| Sample ID | `base_solusdt_fw60_dev` (legacy sample artifact removed) |
| Feature source | `solusdt_1m_features` |
| Feature count | See `models/lgbm_solusdt_s_fw60_q10_local_v3/features.json` |
| Search artifacts | `models/lgbm_solusdt_s_fw60_q10_local_v3/search/` |
| Final artifacts | `models/lgbm_solusdt_s_fw60_q10_local_v3/` |

## Notes

This page is the documentation home for the model. Metric values should be
kept in sync with `model_card.json`, `search_best.json`, and evaluation reports.
