---
epic: epic_047
id: t5
title: UI/trading config ellenőrzés — mindkét modell éles-e?
assignee: ui_agent
status: todo
blocks: [t7]
blocked_by: []
---

## Goal
Ellenőrizni hogy a long és short champion modellek (`lgbm_solusdt_l/s_fw60_2101_2605`)
mindkettő élesben van-e a trading service és UI mögött. Ha valamelyik hiányzik vagy
eltérő konfiguráció van, azt a Notes-ban dokumentálni.

## Scope
- `config/trading.json` — `strategy_session_long_id` + `strategy_session_short_id`
- `src/trading/live/` — strategy artifact betöltési logika
- `src/ui/` — predictions és signals megjelenítés
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/strategy/strategy_artifact.json`
- `artifacts/lgbm_solusdt_s_fw60_2101_2605/strategy/strategy_artifact.json`

## Acceptance Criteria
- [ ] `config/trading.json` mindkét session ID-t tartalmaz
- [ ] Mindkét strategy_artifact.json létezik és érvényes
- [ ] UI data loading mindkét modell predikciót megjeleníti
- [ ] Eltérések (ha van) dokumentálva a Notes-ban

## Notes
