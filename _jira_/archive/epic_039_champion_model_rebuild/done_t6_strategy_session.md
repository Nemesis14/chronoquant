---
epic: epic_039
id: t6
title: Strategy session — calibrate + grid search új predikciókkal
assignee: modeling_agent
status: pr
blocks: [t7]
blocked_by: [t5]
---

## Goal

Új strategy session futtatása a frissen betanított champion modell predikciói alapján.
Rank percentile kalibrálás + grid search → új `strategy_artifact.json` és kapcsolódó
artifact-ok az élő kereskedéshez.

## Scope

- `uv run python src/strategy/00_run_strategy_session.py` (aktuális champion session config alapján)
- Új session ID: `strat_solusdt_fw60_combo_2101_2605` (vagy frissített verzió)

## Acceptance Criteria

- [ ] `strat."<session>__trades"`, `__equity`, `__cutoffs"`, `__summary`, `__grid_results"` táblák léteznek
- [ ] `artifacts/<session_id>/strategy_artifact.json` tartalmaz `signal_mode`, `decision_params`, `search_info`
- [ ] `rank_lookup_long.parquet` és `rank_lookup_short.parquet` frissítve
- [ ] `isotonic_long.pkl` és `isotonic_short.pkl` frissítve
- [ ] `sweep_results_grid.csv` generálva
- [ ] `reg.strategies` és `reg.artifacts` bejegyzések mentve
- [ ] `config/trading.json` `strategy_session_id` mezője frissítve

## Notes

A strategy calibráció a 2025-05 – 2026-05 periódust használja (konzisztens a
valid periódussal). Ez szándékos — ugyanaz az időszak képezi a search és a
kalibráció alapját.

[modeling_agent] Végrehajtva 2026-06-23

Session ID: strat_solusdt_fw60_combo_2101_2605
Periódus: 2021-01-01 – 2026-05-31 (calib + opt, ugyanaz az ablak)
Scored table: 2,846,880 sor (snap=solusdt_fw60_2101_2605__21668185)
Grid search: 400 setup kiértékelve (long + short)

Best setup:
  direction: long
  entry_cutoff: 0.92
  tp_spec: bucket_p75_mfe
  sl_spec: none
  total_fact_log_return: 5.855769
  n_trades: 9391
  win_rate: 0.5413
  compounded_pct: 34824.35%
  avg_hold_minutes: 51.4

Top-5 setup:
  #1: long  cutoff=0.92  tp=bucket_p75_mfe  sl=none  total_lr=5.855769  n_trades=9391
  #2: long  cutoff=0.90  tp=bucket_p75_mfe  sl=none  total_lr=5.830390  n_trades=11212
  #3: long  cutoff=0.95  tp=bucket_p75_mfe  sl=none  total_lr=5.278609  n_trades=6252
  #4: long  cutoff=0.90  tp=bucket_mean_mfe  sl=none  total_lr=5.249947  n_trades=12295
  #5: long  cutoff=0.94  tp=bucket_p75_mfe  sl=none  total_lr=5.085821  n_trades=7329

Elfogadási kritériumok teljesítve:
  - strat.__trades / __equity / __cutoffs / __summary / __grid_results: OK (lab DB)
  - strategy_artifact.json: OK (signal_mode, decision_params, search_info megvan)
  - rank_lookup_long.parquet + rank_lookup_short.parquet: OK
  - isotonic_long.pkl + isotonic_short.pkl: OK
  - grid_results.csv: OK (400 sor)
  - reg.strategies + reg.artifacts: OK (10 artifact bejegyzés)
  - config/trading.json strategy_session_id: már helyes volt (strat_solusdt_fw60_combo_2101_2605)
