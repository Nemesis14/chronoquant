---
epic: epic_018
id: t4
title: Feature engineering futtatása — ipynb + HTML generálás
assignee: analyst_agent
status: done
blocked_by: [t2]
---

## Goal

Lefuttatni a `01_feature_engineering.py` scriptet a solusdt asseten, hogy az analyst report elkészüljön `.ipynb` + HTML formátumban. A user átnézi az eredményt és beállítja a feature szelekciót (`feature_set.json`).

## Scope

- `src/modeling/01_feature_engineering.py` futtatása
- Output: `database/solusdt/feature_engineering/<run_id>/`
  - `feature_set.json`
  - `analyst_report.ipynb`
  - `report.html`

## Acceptance Criteria

- [ ] `uv run python src/modeling/01_feature_engineering.py --asset-id solusdt` sikeresen lefut
- [ ] `analyst_report.ipynb` elkészül és megnyitható
- [ ] `report.html` elkészül és böngészőben megtekinthető
- [ ] `feature_set.json` tartalmaz feature listát
- [ ] A run_id és output path kiíródik a konzolra

## Notes

Ha a jelenlegi script csak `.md` reportot ír (nem `.ipynb`), azt jelezd a Notes-ban — a `reporting.py` frissítése külön taskon belül történik.

A feature szelekció véglegesítése (melyik feature-ök maradnak) a user feladata az HTML report megtekintése után.

**2026-06-19 — DONE (user által):** A notebook (`src/modeling/01_feature_engineering.ipynb`) struktúrálisan validálva.
Megjegyzések:
- Script `.ipynb` formátumú (nem `.py` — a ticket tévesen hivatkozott `.py`-re)
- `reporting.py` jelenleg `analyst_report.md`-t ír, nem `.ipynb`-t — a HTML/ipynb output
  igény esetén külön task (de user elfogadta a jelenlegi `.md` outputot)
- A user a "feature engineering - 0.0 done" committal jelezte hogy az analízis lefutott
