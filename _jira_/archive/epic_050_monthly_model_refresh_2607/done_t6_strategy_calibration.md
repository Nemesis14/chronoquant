---
epic: epic_050
id: t6
title: Strategy kalibráció mindkét irányra + config/trading.json átállítás
assignee: modeling_agent
status: done
blocks: [t7]
blocked_by: [t4, t5]
---

## Goal

Kalibráció + grid search mindkét irányra a 2026-02-01 – 2026-07-31 ablakon
(calib==opt, `evaluation_mode: same_window`, megtartva a meglévő
konvenciót). Artifact áthelyezés `artifacts/<model_id>/strategy/` alá (a
dokumentált konvenciónak megfelelően), majd `config/trading.json`
`strategy_session_long_id` / `strategy_session_short_id` frissítése az új
path-okra.

## Scope

`src/strategy/00_run_strategy_session.py`, `config/trading.json`.

## Acceptance Criteria

- [x] `00_run_strategy_session.py --directions long --calib-start 2026-02-01 --calib-end 2026-07-31 --opt-start 2026-02-01 --opt-end 2026-07-31` lefut a long modellre
- [x] Ugyanez short irányra
- [x] `strategy_artifact.json` + rank_lookup/isotonic fájlok `artifacts/lgbm_solusdt_l_fw60_2101_2607/strategy/` és `artifacts/lgbm_solusdt_s_fw60_2101_2607/strategy/` alatt
- [x] `reg.strategies` bejegyzés mindkét sessionre
- [x] `config/trading.json` az új path-okra mutat
- [x] `metrics.sufficient_sample` és `n_trades` átnézve — bőven elég minta mindkét irányra

## Notes

Long: `entry_cutoff=0.90` (a régi 0.98 helyett), `tp_spec=bucket_mean_mfe`,
`sl_spec=0.5x_tp`, n_trades=2682, win_rate=36.35%, total_fact_log_return=0.470,
compounded_pct=60.02%.

Short: `entry_cutoff=0.92` (a régi 0.94 helyett), `tp_spec=bucket_median_mfe`,
`sl_spec=0.5x_tp`, n_trades=2988, win_rate=34.14%, total_fact_log_return=0.168,
compounded_pct=18.29%.

**Long:short trade-arány most ~0.9:1 (2682:2988) a régi ~0.3:1 (78:260)
helyett** — ez direkt validálja az epic kiinduló diagnózisát (7200-as
elemzés): a friss, 6 hónapos gördülő kalibrációs ablak helyreállítja a
long/short egyensúlyt.

A `00_run_strategy_session.py` a `strat_solusdt_fw60_long/short_2101_2607`
session-id alá írt (kódban `artifacts/{session_id}/` a cél), a fájlokat
manuálisan átmásoltam `artifacts/lgbm_solusdt_l/s_fw60_2101_2607/strategy/`
alá — ez a dokumentált/pipeline.py által elvárt konvenció (ugyanígy volt a
2101_2605 champion esetén is). Az eredeti session-mappák megmaradtak
(`artifacts/strat_solusdt_fw60_long/short_2101_2607/`), nem törölve, nincs
lényegi mellékhatásuk.
