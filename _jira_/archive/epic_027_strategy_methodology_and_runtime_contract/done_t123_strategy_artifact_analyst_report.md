---
epic: epic_027
id: t123
title: Keszits analyst notebookot es Quarto reportot a strategy artifacthoz
assignee: analyst_agent
status: pr
blocks: [t124]
blocked_by: [t122]
---

## Goal

Az analyst agent készítsen a strategy artifact mappába egy `.ipynb` notebookot és
egy Quarto-renderelt `.html` riportot, amely bemutatja és levezeti a strategy session eredményét.

## Scope

- strategy artifactban mentett `strategy_table`
- trade ledger
- equity curve
- strategy summary
- `_doc_` / analyst render szabályok

## Acceptance Criteria

- [ ] Készüljön `.ipynb` notebook a strategy artifact mappába
- [ ] Készüljön ugyanoda Quarto-renderelt `.html` report
- [ ] A report tartalmazzon rövid executive összefoglalót
- [ ] A report mutassa be a strategy summaryt:
  `initial_capital`, `final_equity`, `n_trades`, `win_rate`, `gross_return`, `net_return`
- [ ] A report tartalmazzon éves vagy teljes időablakos árfolyamplotot, rajta:
  long entry zöld markerrel, short entry piros markerrel, exit jelölésekkel
- [ ] A report tartalmazzon equity curve ábrát
- [ ] A report egyértelműen jelezze, ha a metrika same-window strategy sessionből jön

## Notes

Elhelyezés javaslat:

- `artifacts/<session_id>/strategy_report.ipynb`
- `artifacts/<session_id>/strategy_report.html`

Az analyst report a strategy artifact hivatalos bemutató outputja legyen.

---

### Implementation (analyst_agent, 2026-06-20)

**Session ID:** `strategy_lgbm_solusdt_l_fw60_2101_2605__lgbm_solusdt_s_fw60_2101_2605__20260620`

**Létrehozott fájlok:**
- `artifacts/strategy_lgbm_solusdt_l_fw60_2101_2605__lgbm_solusdt_s_fw60_2101_2605__20260620/strategy_report.ipynb` (53 KB, végrehajtva)
- `artifacts/strategy_lgbm_solusdt_l_fw60_2101_2605__lgbm_solusdt_s_fw60_2101_2605__20260620/strategy_report.html` (2.0 MB, self-contained)

**Adat elérhetőség:**
- `strategy_artifact.json`: hiányzik — a strategy pipeline (01_calibrate + 02_optimize) még nem futott
- `summary.json`: hiányzik
- `trades.parquet`: hiányzik
- `equity_curve.parquet`: hiányzik
- `strategy_table.parquet`: hiányzik

**Fallback viselkedés:** A notebook minden szekció graceful fallbackkel rendelkezik —
ha a fájl hiányzik, "nem elérhető, futtasd a pipeline-t" üzenetet jelenít meg, és a render nem hibázik.

**HTML render:** Sikeresen renderelt Quarto-val (quarto render strategy_report.ipynb --to html).
9 cella végrehajtva, 9/9 OK. CSS warning (chronoquant_analysis.css nem található a relatív útvonalon)
— nem blocker, az embed-resources: true biztosítja az önálló HTML-t.

**Döntés — session_id meghatározás:** A `config/trading.json` `strategy_session_id` mezője üres.
A meglévő modellek (`lgbm_solusdt_l_fw60_2101_2605`, `lgbm_solusdt_s_fw60_2101_2605`) alapján
az elvárt session ID pattern szerint lett a mappa létrehozva.

**Következő lépés:** A strategy pipeline futtatása után (00_build → 01_calibrate → 02_optimize)
a notebook újrafuttatható, és minden szekció kitöltődik valós adatokkal.
