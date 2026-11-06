---
epic: epic_068
id: t891
title: Overview, agent manifestek, scriptek frissítése
assignee: code_doc_agent
status: done
blocks: [t892]
blocked_by: [t890]
---

## Goal
`_doc_/0000_project_overview.md` (architektúra, repo layout, tesztelési szabályok), CLAUDE.md delegációs tábla, `.agent/agents/*.md`, kód-referencia doc; `scripts/dev.ps1`, start/stop, autostart az új BFF+frontendre.

## Scope
`_doc_/**`, `.agent/**`, `CLAUDE.md`, `scripts/**`

## Acceptance Criteria
- [x] Dokumentáció a valós állapotot tükrözi
- [x] Egy paranccsal indítható dev és prod mód (`scripts\dev.ps1` / `scripts\start.ps1`; a doksi egyezik a scriptekkel)

## Notes

[code_doc_agent] t891 elvégezve (2026-11-06).

**Frissítve:** `_doc_/0000_project_overview.md` (üzleti cél, modul-architektúra, adatfolyam, persistence, repo layout, sampling = csak train_valid_split, tesztelés/scriptek, konvenciók, ownership), `_doc_/0001_agentic_system.md` (ownership táblák/mermaid), `CLAUDE.md` (csak a delegációs tábla), `.agent/agents/{ui,analyst,modeling,database,validator,code_doc,methodology}_agent.md`, `.agent/skills/{analyst (teljes átírás: report builder), docs, coding, deploy, methodology_doc}_skill.md`, `.agent/tools/permissions_tool.md`, `_doc_/_plans_/068_bff_ui.md` (státusz "megvalósítva", eltérés-tábla), `_doc_/database_and_code_doc/` 0002, 0003, 0004, 5050, 5300, 5510 (train.py/artifacts/fit_lgbm), 5520 (rank-arány objektív, nincs patience), 5530 (builder FE + analyze), 6140, 7000 (journal-áttekintés), 7130 (journal).
**Új:** `8300_api.md`, `8310_worker.md`, `8320_cq_mcp.md`, `8330_reports.md`, `8340_sentiment.md`, `8350_frontend.md`, `8360_scripts.md`.
**Törölt (a kód már nem létezik):** 5100, 5200, 5410, 5420, 7100, 7110, 7120, 7140, 7150, 8000, 8100, 8110, 8120, 8130, 8140, 8150, 8200.
**Linkjavítás:** a `database_and_code_doc` felfelé mutató (korábban törött) methodology/arch linkjei az új számozásra (1000_features, 2000_targets, 3000_sampling, 4000_feature_engineering, 5000_hyper_param_search, 5100_training) igazítva; ellenőrző script: 0 törött link a saját fájlokban, kivéve az alábbi nyitott pontokat.
**scripts/ ellenőrzés:** `dev/start/stop/install_autostart/_common.ps1` egyezik a 8360 és az overview leírásával; hibát nem találtam.

**Nyitott pontok:**
- `_doc_/_plans_/data_process_architecture.md` (hivatkozza: 1410, 1510) nem létezik: régi törött link, nem törlés miatt.
- `.agent/skills/methodology_doc_skill.md` és `docs_skill.md` példa-fájlnevei (`5100_sampling_config.md`, `7100_live_trading.md`) csak sablon-példák.
- `_doc_/methodology_doc/` nem módosítva (t899); a `database_and_code_doc` -> methodology linkek már az ottani jelenlegi fájlnevekre mutatnak.
- A DB-oldali törölt táblák (feat_altdata, altdata_5m, analyst_signal) nincsenek előírva a doksiban (t898).
- A 5510 `cv_results.csv` a `TRAINING_ARTIFACT_FILES` listában marad, de a jelenlegi fit nem feltétlen írja: modeling_agent döntse el.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
scripts\start.ps1 -NoBrowser / stop.ps1 (CQ_PORT=8698) kipróbálva: API + worker egyben indul, worker alive=true, stop után :8698 szabad és nincs 'python -m worker' folyamat. registry_validator: OK. install_autostart.ps1 nem futott (user-teendő).
