---
epic: epic_069
id: t905
title: Dokumentáció: multi-asset
assignee: code_doc_agent
status: done
blocks: ["t909"]
blocked_by: ["t904", "t907"]
---

## Goal
A multi-asset működés és a BCH modell dokumentálása.

## Scope
`_doc_/0000_project_overview.md` (Business Goal, adatbázis, asset lista), `_doc_/database_and_code_doc/0002/0003/0004`, `83xx` (api asset-kontextus, worker, frontend), `.agent/skills/*` ahol az asset-parancsok szerepelnek, `CLAUDE.md` delegation táblához nem kell nyúlni.

## Acceptance Criteria
- [x] Az új asset-onboarding lépéssor (assets.json -> bootstrap -> pipeline -> deploy -> UI fül) leírva egy helyen (`model_lifecycle_skill` vagy 0004)
- [x] Az overview a két aktív assetet mutatja; nincs elavult 'csak solusdt' állítás
- [x] Nincs törött link

## Notes

**code_doc_agent, 2026-11-07**

- `_doc_/0000_project_overview.md`: Business Goal (két aktív asset), repository layout (`database/bchusdt/`), Database szekció (asset-onkénti DB-pár, assetek táblázata, auto-sync szabály, funding hiány), Modellek (9 db; SOL és BCH champion pár + 3 mondatos BCH eredmény, a modell-doksi "készül" hivatkozás, nincs link), Trading Strategy (`trading.json` asset-blokkok, `utils` API), Key Conventions "Assets" sor (az "Active asset: solusdt" állítás törölve), `--asset-id` megjegyzés a pipeline parancsoknál.
- `0002_data_architecture.md` (bchusdt layout, asset-onkénti DB-pár, funding), `0003_runtime_flow.md` (Multi-asset ütemezés, `?asset=`), `0004_model_lifecycle.md` (új "Új asset onboarding" 12 lépéses tábla, deploy `--asset-id`), `8330_reports.md` (asset-kontextus). 8300/8310/8320/8350 t903/t904 már lefedte, nem duplikáltam.
- `.agent/skills/model_lifecycle_skill.md` (snapshot `--asset-id`, `asset_id` a models.json-ban, onboarding összefoglaló), `deploy_skill.md` (`--asset-id`, `<asset>` az SQL-ben), `.agent/agents/database_agent.md` és `modeling_agent.md` (elavult "csak SOLUSDT" sor frissítve).
- `rg "only one active asset|inaktív asset|inactive asset|Active asset|csak solusdt"` a `_doc_`/`.agent` alatt: nincs találat. Új hivatkozás csak létező fájlokra mutat; a `_doc_/models_doc/` BCH doksi még nem létezik, ezért nincs rá link (t908).
- Kód nem módosult; `_jira_/archive/` nem érintve.
