---
epic: epic_065
id: t871
title: K=10 clustering friss training (2021-2025-06) + OOS validáció (2025-07—2026-06)
assignee: modeling_agent
status: todo
blocks: []
blocked_by: [t870]
---

## Goal

Egy friss K-means clustering (k=10) modell illesztése az összes korábbi training data-n (2021-01 — 2025-06), majd OOS backtesting a 2025-07 — 2026-06 perióduson **reális trading szimulációval** (slippage, costs, fixed $1,000 position size).

Ez az **épület Phase 3 validation** az ideális MFE-alapú k=3 helyett, reális piaci feltételek alatt.

## Scope

### Training Phase (2021-01-01 — 2025-06-30)
- Feature selection: MI-alapú redundancia analízis (t863-like)
- Window representation: 360-bar rolling, z-score normalizálás (t865-like)
- K-means clustering: **k=10**, determinisztikus (random_state=42)
- Output: trained model + feature metadata

### OOS Backtesting Phase (2025-07-01 — 2026-06-30)
- Cluster assignment: trained k=10 model alkalmazása OOS data-ra
- Entry signal: cluster ID per bar
- Trading rules:
  - Entry: $1,000 per cluster signal
  - Exit 1 (TP): +2% return → zárás
  - Exit 2 (SL): -1% loss → zárás
  - Exit 3 (timeout): 24h hold → zárás az MFE-n
- Costs: 0.3% per trade (spread + commission)
- Slippage: 0.1-0.5% entry/exit (reális market impact)
- Output: CSV trade log + JSON summary

## Acceptance Criteria

- [x] K=10 model trained az összes 2021-2025-06 adaton
- [x] Feature selection dokumentálva (which 14 features, why)
- [x] OOS cluster assignment kiszámítva 2025-07 — 2026-06 range-re
- [x] Backtest CSV: minden trade (entry/exit time, price, MFE, realized P&L)
- [x] Summary JSON: trade count, win rate (%), avg P&L, net P&L, drawdown
- [x] Model artifacts: `kmeans_k10_trained_2021_2025_06.pkl` saved
- [x] Kóminőség: ruff check pass

## Metodológia

Ugyanaz az approach mint t866-t870, de:
1. Train data: 2021-06-ig (összes korábbi) — nem csak 2025
2. OOS: 2025-07-06, exact mint t870, de **reális execution** (nem MFE-ideális)
3. Per-trade P&L: actual TP/SL hit, slippage-vel

## Output Files

- `artifacts/research_cluster_regime/kmeans_k10_trained_2021_2025_06.pkl` — trained model
- `artifacts/research_cluster_regime/k10_oos_backtest_2025_07_2026_06_realistic.csv` — trade log
- `artifacts/research_cluster_regime/k10_oos_backtest_2025_07_2026_06_summary.json` — stats
- `_doc_/models_doc/6501_k10_validation_realistic_backtest.md` — analysis report

## Notes

- Phase 3 döntéshez szükséges: valós trading scenario (nem ideális MFE)
- Összehasonlítandó: t870 backtest (MFE, 98.3% WR) vs. t871 backtest (reális, ~60-80% WR expected)
- Elvárt result: ha k=10 edge valódi, akkor 60-80% win rate és $10-20k net profit az 1 év alatt

