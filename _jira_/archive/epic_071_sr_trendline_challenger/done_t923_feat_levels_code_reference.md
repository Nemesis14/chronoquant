---
epic: epic_071
id: t923
title: Dokumentáld a feat_levels táblát, a sync-et és a challenger quant_train profilt
assignee: code_doc_agent
status: done
blocks: []
blocked_by: [t912]
---

## Goal
Kód-referencia a t912 változásaihoz a `_doc_/database_and_code_doc/` zónában.

## Scope
- `feat_levels` tábla (live main, migráció v6): séma, feature lista + definíciók (`_levels_features.FEATURE_DOCS`), NULL-szemantika, t-1 / look-ahead
- `sync_tables/sync_levels.py` CLI + `02_sync_pipeline.py` új `levels` lépés
- `rebuild_quant_train(extra_feature_tables, table_name)`, `quant_train__<profile>` tábla, `create_snapshot(source_table=...)`, `--features-profile` kapcsolók (`03_build_quant_train.py`, `05_create_snapshot.py`)
- `utils`: `base_profile`, `feature_profile_extra_tables()`, `quant_train_table()`; `config/features.json` `solusdt_fw60_sr` profil
- Érintett docok (a meglévők közül, ahol a téma él): `1200_sync_tables.md`, `0002_data_architecture.md` (live táblák), a quant_train / snapshot / store referencia oldalai

## Acceptance Criteria
- [ ] Szignatúrák a kódból, a meglévő stílusban
- [ ] A t922 által írt `_levels_polars.py` szakasz megmarad

## Notes

### code_doc_agent (2026-11-19) — módosított doc fájlok (`_doc_/database_and_code_doc/`)
- `1200_sync_tables.md` — új szakasz `_levels_features.py` + `sync_levels.py` (API, feature-tábla, look-ahead, NULL, CLI, pipeline); a `_levels_polars.py` (t910) szakasz érintetlen; sorrend + idempotencia tábla kiegészítve
- `0002_data_architecture.md` — live `main` séma: `feat_levels`, `quant_train__<profile>`
- `1000_database.md` — ER: `feat_levels`; új `feat_levels` tábla-szakasz; challenger quant_train megjegyzés
- `1110_duckdb_store.md` — migráció v6 (`create_feat_levels`); `rebuild_quant_train` új paraméterei (`extra_feature_tables`, `table_name`)
- `1100_store.md` — migráció v1–v6; írási mód táblázat (`quant_train__<profile>`, `feat_levels`)
- `4100_quant_train.md` — új szakasz: challenger `quant_train__solusdt_fw60_sr` (join, ellenőrzés, build/snapshot parancs)
- `4110_sync_quant_train.md` — `features_profile` paraméter, `--features-profile` példa
- `1410_snapshots_code.md` — `create_snapshot` szignatúra (`exclude_features`, `source_table`), `--features-profile` CLI flag
- Metodológia link: `1200` → `methodology_doc/1700_features_sr_trendline.md` (felfelé, egyirányú); a methodology zóna nem lett módosítva
- Nem nyitottam methodology `todo_`-t: a `1700_features_sr_trendline.md` már létezik. Ha a feat_levels feature-definíciók (pl. `is_round`, retest) módszertani indoklása hiányzik ott, a methodology_agent-nek kell külön ticket.
- Kód nem módosult; a ticket neve változatlan, status `todo`.

[validator] Validated — 2026-11-19 → done
- Szignatúrák a kódból ellenőrizve: `rebuild_quant_train(conn, start_time, end_time, extra_feature_tables=None, table_name="quant_train")` (ValueError: extra tábla + `quant_train` név, hiányzó tábla, duplikált oszlop), `create_snapshot(..., exclude_features=None, source_table="quant_train")` + `_live_source` név-validálás, `sync_levels(asset_id, start_time, end_time, full, params)`, `upsert_feat_levels(conn, df)`, `sync_quant_train(..., features_profile=None)`, `utils.feature_profile_extra_tables` / `quant_train_table`, `--features-profile` (03, 05; default None), `02_sync_pipeline` `levels` lépés a predictions után, migráció v6 `create_feat_levels`, `_feat_levels_v1_columns`.
- 1200 feature-táblázat: a `FEATURE_DOCS` mind a 12 csoportja szerepel, 50 oszlop (= `feature_columns()`); look-ahead / t-1 és NULL-szemantika dokumentálva.
- A t922 `_levels_polars.py` szakasza (1200, 87. sortól) megmaradt. Kód nem módosult.
