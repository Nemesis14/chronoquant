---
id: t59
epic: epic_036
title: Metodológia dokumentáció — execution-aware grid search
assignee: methodology_agent
status: pr
blocks: [t60]
blocked_by: []
---

## Context

Az epic_035 az Optuna-alapú stratégia-keresőt egy determinisztikus, végrehajtás-tudatos
grid search-re cserélte. A metodológiai rationale még nem dokumentált.

## Task

Írj új metodológiai dokumentumot a `_doc_/methodology_doc/` zónába.

Fájlnév javasolt: `_doc_/methodology_doc/6100_strategy_grid_search.md`
(vagy a zóna meglévő számozásához illeszkedő szám)

A dokumentum tartalmazza:
- **Motiváció**: miért váltottunk Optuna TPE-ről grid search-re
  - Determinizmus és reprodukálhatóság
  - Kisebb keresési tér (200 setup/irány) → teljes lefedés lehetséges
  - Objektív: total realized `fact_log_return` (nem proxy `bucket_mean_mfe`)
- **Végrehajtási modell (TP/SL)**
  - Intrabar high/low touch: Long TP ha `high >= entry * exp(tp_lr)`, SL ha `low <= entry * exp(-sl_lr)`
  - Same-bar konflikt → SL nyer (konzervatív)
  - 60-bar timeout → close áron zár
  - Re-entry: következő bar, amint az exit lezárult
- **Short irány invertált rankingjának magyarázata**
  - `short_mfe_fw60 = log(fw_min / close[t]) < 0` (ár esett = profitable short = negatív érték)
  - Ezért alacsony `score_pct_short` = legjobb short lehetőség
  - Entry feltétel: `(1 - score_pct_short) >= entry_cutoff`
- **Keresési tér**: 8 cutoff × 5 TP spec × 5 SL spec = 200 setup/irány
  - TP spec-ek: bucket_mean_mfe, bucket_median_mfe, bucket_p75_mfe, 0.75×mean, 0.50×mean
  - SL spec-ek: none, 0.5×TP, 1.0×TP, 1.5×TP, 2.0×TP
- **Eredmények értelmezése**
  - `total_fact_log_return` = log-return összeg
  - `compounded_return_pct = (exp(total_lr) - 1) × 100` — teljes periódus hozama (nem éves)
  - `win_rate` = TP exit / (TP + SL + timeout) arány

Stílus: kód-mentes, methodológia zóna konvenciói szerint.
Hivatkozz: `_doc_/database_and_code_doc/` kód-referencia lapokra ahol releváns.

## Acceptance

- Fájl létezik a `_doc_/methodology_doc/` alatt
- Kód-mentes (nincs Python snippet, csak leírás)
- Hivatkozások konzisztensek a meglévő zóna struktúrával

## Notes

Elvégzett munka:

- Létrehozva: `_doc_/methodology_doc/6300_strategy_grid_search.md`
  - A `6100` szám már foglalt (`6100_strategy_calibration.md`), ezért a következő
    szabad szám (`6300`) lett használva
  - Tartalom: motiváció (Optuna TPE → grid search váltás), végrehajtási modell
    (intrabar TP/SL, same-bar konfliktus, 60-bar timeout, re-entry), short irány
    invertált ranking magyarázata, teljes keresési tér (8×5×5=200 setup/irány),
    kalibráció vs. keresési periódus szétválasztás, eredmények értelmezése
    (total_fact_log_return, compounded_return_pct, win_rate), referencia eredmény
    (long, cutoff=0.97, 0.75×bucket_mean, none SL, 319 trade, 63.3% win rate,
    49.3% compounded, 16 hónap)
- Frissítve: `_doc_/methodology_doc/6000_strategy.md` — alfejezetek táblázatába
  bekerült az új `6300_strategy_grid_search.md` bejegyzés
