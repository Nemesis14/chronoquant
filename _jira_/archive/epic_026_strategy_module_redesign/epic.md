---
epic: epic_026
title: Strategy calibration modul újratervezés
status: active
---

## Cél

Szétválasztani a három felelősséget, amelyek jelenleg össze vannak keverve:

1. **Modell tanítás** (`src/modeling/`) — csak model.pkl + artifacts, nincs OOS, nincs backtest
2. **Strategy calibration** (`src/strategy/` — ÚJ) — strategy table build, score calibration, entry/exit optimalizálás
3. **Live trading** (`src/trading/`) — csak live signal alkalmazás, trade execution, journal, visszamérés

## Motiváció

- A fit lépés jelenleg OOS scoring-ot is végez → nem szabad, hogy a fit tudjon a backtest-ről
- A calibration logika a `src/trading/` alatt élt, de nem trading-felelősség
- A target nem bináris → a strategy optimalizáció continuous MFE-alapú kell legyen
- A long + short modell score-jait kalibrálni kell (isotonic regression) mielőtt stratégiát futtatunk
- A strategy table közös artifact-ba kerül: `artifacts/strategy_<long>_<short>_<date>/`

## Taskok

| ID | Cím | Assignee | Blokkolja | Blokkolt általa |
|----|-----|----------|-----------|-----------------|
| t1 | fit_lgbm.py OOS cleanup | modeling_agent | — | — |
| t2 | Strategy methodology doc | methodology_agent | — | t3 |
| t3 | src/strategy/ modul | modeling_agent | t2 | t5 |
| t4 | src/trading/ cleanup | ui_agent | — | t5 |
| t5 | Validator session | validator_agent | t3, t4 | — |
