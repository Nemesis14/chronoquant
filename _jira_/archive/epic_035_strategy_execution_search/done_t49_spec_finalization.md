---
id: t49
title: execution-aware strategy search spec és search-space kontraktus véglegesítése
epic: epic_035_strategy_execution_search
assignee: modeling_agent
status: pr
blocks: [t50]
blocked_by: []
---

## Description

A meglévő epic_035/epic.md alapján véglegesíteni a következő implementációs döntéseket (kód írása nélkül), hogy t50 egyértelmű specifikációval indulhasson.

## Döntések, amik t49-ben rögzítendők

1. **Entry timing**: belépés az entry bar `close` árán (konzisztens a meglévő kóddal).

2. **TP/SL intrabar touch szabályok (long):**
   - TP aktiválódik, ha `high >= entry_close * exp(tp_lr)`
   - SL aktiválódik, ha `low <= entry_close * exp(-sl_lr)`
   - Ha ugyanazon baron MINDKETTŐ teljesül → **SL-t vesszük** (konzervatív szabály: rosszabb kimenet a kereskedő számára)

3. **TP/SL intrabar touch szabályok (short):**
   - TP aktiválódik, ha `low <= entry_close * exp(-tp_lr)` (ár esik → profit short-nál)
   - SL aktiválódik, ha `high >= entry_close * exp(sl_lr)` (ár emelkedik → veszteség)
   - Ugyanazon bar konflikt → **SL-t vesszük** (konzervatív)

4. **`fact_log_return` számítás:**
   - Long TP: `fact_log_return = tp_lr`
   - Long SL: `fact_log_return = -sl_lr`
   - Long timeout: `fact_log_return = log(close_exit / entry_close)`
   - Short TP: `fact_log_return = tp_lr`
   - Short SL: `fact_log_return = -sl_lr`
   - Short timeout: `fact_log_return = log(entry_close / close_exit)`

5. **TP spec → konkrét `tp_lr` mapping** (per-entry, a kalibrációs bucket stats alapján):
   - `"bucket_mean_mfe"` → bucket mean MFE of entry bar (long: `long_mfe_fw60`, short: `short_mfe_fw60`)
   - `"bucket_median_mfe"` → bucket median MFE (új stat, calibrate.py-ban hozzáadandó)
   - `"bucket_p75_mfe"` → bucket 75. percentilis MFE (új stat, calibrate.py-ban hozzáadandó)
   - `"0.75x_bucket_mean"` → 0.75 * bucket_mean_mfe
   - `"0.50x_bucket_mean"` → 0.50 * bucket_mean_mfe

6. **SL spec → `sl_lr` mapping** (a konkrét tp_lr ismert, entry bárban):
   - `"none"` → sl_lr = 0 (nincs stop)
   - `"0.5x_tp"` → sl_lr = 0.5 * tp_lr
   - `"1.0x_tp"` → sl_lr = 1.0 * tp_lr
   - `"1.5x_tp"` → sl_lr = 1.5 * tp_lr
   - `"2.0x_tp"` → sl_lr = 2.0 * tp_lr

7. **Keresési tér (grid search, nem Optuna):**
   - entry_cutoff: [0.90, 0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99]
   - tp_spec: 5 opció (fent)
   - sl_spec: 5 opció (fent)
   - Összesen: 8 × 5 × 5 = 200 setup per direction

8. **Direction keresés:** long és short irányonként külön-külön futtatható; a szimulációban nincs conflict resolution (csak az adott irány signal számít)

9. **Re-entry:** TP/SL/timeout exit után az első következő bartól újra vizsgálható entry (nincs cooldown)

10. **Timeout:** 60 bar fix (60 perc, 1-perces bar)

## Notes

Spec confirmed — no contradictions found with existing codebase.

Key consistency checks:
- `build_scored_table` returns `high`, `low`, `close` columns (from live.ohlcv join, NULL-safe fallback), coherent with TP/SL price checks.
- `calibrate.py` outputs `score_pct_{direction}` and `bucket_mean_mfe_{direction}` — spec entry cutoff column names match exactly.
- `bucket_median_mfe` and `bucket_p75_mfe` are new columns to be added in calibrate.py `_build_rank_lookup()` — no conflict.
- Entry at `close` price is consistent with `calibrated_df` carrying the `close` column from the OHLCV join.
- `fact_log_return` formulas: long TP = +tp_lr, long SL = -sl_lr, timeout = log(close/entry_close) — mathematically consistent, no contradiction.
- Short TP direction: `low <= entry_close * exp(-tp_lr)` means price fell to target → fact_lr = +tp_lr — spec is internally consistent.
- Re-entry: no cooldown period in the new engine (unlike the old Optuna state machine) — spec explicitly allows this.
