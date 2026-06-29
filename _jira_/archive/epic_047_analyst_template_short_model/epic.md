# Epic 047 — Analyst template refaktor + short model analysis

## Summary
A long model analysis notebook-jait template-síteni kell: közös kód kiemelése `analyst/lib/`-be,
hardcoded MODEL_ID eltávolítása. Ezután a short model (`lgbm_solusdt_s_fw60_2101_2605`)
ugyanolyan 4 analysis notebookot kap (01_sampling, 02_fe, 03_search, 04_strategy),
maximális shared code-dal. Mindkét modell UI élesség ellenőrzése.

## Scope
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/` — 4 notebook refaktor
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/analysis/` — 4 notebook létrehozás
- `analyst/lib/` — közös kód kiemelés
- `config/trading.json`, `src/ui/` — élesség ellenőrzés
- `_doc_/database_and_code_doc/` — template struktúra dokumentálás

## Tasks
- t1: Long model doc + notebook audit (referenciák, reprodukálhatóság) → analyst_agent [haiku]
- t2: Közös kód kiemelése analyst/lib/-be → analyst_agent [sonnet]
- t3: Long model 4 notebook template-esítése → analyst_agent [sonnet]
- t4: Short model analysis elkészítése (01–04 notebooks) → analyst_agent [sonnet]
- t5: UI/trading config ellenőrzés → ui_agent [haiku]
- t6: Code doc frissítés (analyst template struktúra) → code_doc_agent [sonnet]
- t7: Validáció → validator_agent

## Dependencies
t1 → t2 → t3 → t4; t5 (független); t4 + t5 + t6 → t7

## Status
todo

## Kapcsolódik
epic_045 (artifact restructure), epic_046 (strategy notebook)
