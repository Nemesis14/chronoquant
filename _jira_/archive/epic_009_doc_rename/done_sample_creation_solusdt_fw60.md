---
epic: epic_009_doc_rename
id: t4
title: Sample generálás — solusdt_fw60_2010_2605
assignee: modeling_agent
status: pr
---

## Goal

Létrehozni az első sample definíciót az új sample ID konvencióval (YYMM_YYMM)
és az egész-hónapos kerekítési szabállyal.

## Scope

- `src/modeling/quantitative/sampling/config.py` — `round_to_months: bool = True` mező hozzáadva
- `src/modeling/quantitative/sampling/create_sample.py` — `_month_start()` / `_month_end()` helper + kerekítési logika
- `src/modeling/quantitative/00_create_sample.py` — `--no-round-to-months` CLI flag
- `database/solusdt/samples/solusdt_fw60_2010_2605/` — 3 JSON artifact létrehozva

## Acceptance Criteria

- [x] Sample artifact létrejött: metadata.json, folds.json, audit.json
- [x] data_start = 2020-10-01 00:00:00 (kerekítve 2020-09-15-ről)
- [x] data_end = 2026-05-31 23:59:00 (kerekítve 2026-06-14-ről)
- [x] n_folds = 5, embargo = 60 perc, test = 365 nap
- [x] round_to_months default True a SamplingConfig-ban

## Notes

Audit eredmény (raw): data_start_safe=2020-09-15 07:01:00, data_end_safe=2026-06-14 11:00:00, 
gap_count=0, target_nulls=60.

A sample a trg_l_fw60_q90 targettel lett auditálva (data_end_safe meghatározásához).
Ugyanez a sample használható a trg_s_fw60_q10 (short) modell tanításához is —
az időhatárok azonosak, a training pipeline a saját target_col-ját olvassa.
