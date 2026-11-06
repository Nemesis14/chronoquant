---
epic: epic_068
id: t898
title: Altdata/univerzum DB-maradvanyok torlese
assignee: database_agent
status: done
blocks: [t892]
blocked_by: []
---

## Goal
A t894 utan arva altdata/univerzum DB-tablak es fajlok torlese (user explicit jovahagyasa: visszavonhatatlan, nincs gitben).

## Scope
- `database/_universe/` (universe.duckdb)
- `database/solusdt/solusdt.duckdb`: `feat_altdata`, `altdata_5m`
- `database/analyst_signal/analyst_signal.duckdb`: 9 scoring/social/validation tabla
- `src/data_handling/store/duckdb_store.py`, `src/data_handling/store/analyst_signal_store.py`

## Acceptance Criteria
- [x] `solusdt.duckdb`: `feat_altdata` (3 127 763 sor) + `altdata_5m` (497 789 sor) DROP; `funding_rate` (5 187 sor) es a tobbi tabla erintetlen
- [x] `analyst_signal.duckdb`: DROP reddit_seen_authors (2 408 925), social_hourly (632 592), social_features (315 552), call_returns (85 305), author_scores (39 995), score_params (275), oos_validation (15), gate_validation (200), gate_power (2); `calls` (17 061 sor) marad
- [x] `duckdb_store` quant_train `feat_altdata` LEFT JOIN ag eltavolitva
- [x] migraciok m002-m010 no-op, uj DB-n nem hozza letre a torolt tablakat
- [x] ruff/pyright tiszta; pytest 281 passed, 3 ismert elo-DB sanity bukas; registry_validator OK
- [ ] `database/_universe/` torles (11 GB): a futtato kornyezet auto-mode engedelyezoje elutasitotta az `rm -rf`-et, user futtassa (lasd Notes)
- [x] doksik: a `_doc_/database_and_code_doc/` alatt nincs emlites a torolt tablakrol (grep 0 talalat), nem kellett modositani

## Notes
- Zarolas nem volt: a DROP-ok hibatlanul lefutottak (a :8501 Streamlit nem tartotta irasra). Egyetlen user folyamatot sem allitottam le.
- Migraciok: a `run_migrations` a mar rogzitett verziokat atugorja es nem erdekli a fel nem sorolt extra verzio. Ezert a v2-v10 bejegyzesek megmaradtak `_noop` torzzsel (nev/verzio valtozatlan): meglevo DB allapota (max v10) es uj DB allapota azonos, a torolt tablak uj DB-n nem jonnek letre.
- Fajlmeret: `analyst_signal.duckdb` DROP utan 228 MB maradt (1227/1245 blokk szabad), ezert ATTACH + `COPY FROM DATABASE` osszeget keszitettem, ellenorzes (calls 17 061 sor, migracio max v10, tablak: calls, _schema_migrations), majd csere: 228 MB -> 3,9 MB.
- `solusdt.duckdb` DROP utan 13,26 GB (12,3 GiB): 15 675 / 50 583 blokk szabad (~3,8 GiB visszanyerheto). Kockazatos eles fajlcsere miatt NEM csereltem. Javaslat (mikor sem a sync, sem a worker/BFF/Streamlit nem fut): `ATTACH 'solusdt.duckdb' AS s (READ_ONLY); ATTACH 'solusdt.new.duckdb' AS d; COPY FROM DATABASE s TO d;` majd ellenorzes es csere.
- `quant_train` tabla nem tartalmaz altdata oszlopot (ellenorizve), a DROP nem hat ra.
- A `_universe` torlese, ha a user is akarja: `Remove-Item -Recurse -Force D:\repos\chronoquant\database\_universe` (10,9 GiB).
- pytest: 281 passed (a t894 baseline 320 volt, a kulonbseg a t894-ben torolt tesztek), 3 failed = ismert elo-DB sanity (no_time_gap, target_row_count, predictions_score_range).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
Megjegyzés: az AC 'database/_universe/ törlés (11 GB)' szándékosan user-teendő (a user kézzel törli), a ticket ettől függetlenül elfogadva; database/_registry/registry.duckdb.bak_068 (a t890 mentése) a user döntése, törölhető, ha a registry_validator OK (OK).
