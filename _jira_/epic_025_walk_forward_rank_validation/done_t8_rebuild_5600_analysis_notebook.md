---
epic: epic_025
id: t8
title: 5600 analysis notebook és HTML újrakészítése
assignee: analyst_agent
status: pr
blocks: [t9]
blocked_by: [t5, t7]
---

## Goal

A 5600-as analysis notebookot és renderelt HTML-t újra elkészíteni az új
sampling/search/train eredményekkel és az új metodológiai narratívával.

## Scope

- `_doc_/5600_model_2021_train_valid_analysis.ipynb`
- `_doc_/5600_model_2021_train_valid_analysis.html`
- kapcsolódó cache / metrics snapshotok

## Acceptance Criteria

- [ ] Az analysis notebook az új artifactokat használja
- [ ] A renderelt HTML sikeresen elkészül
- [ ] Az output a Top10 Lift, Spearman és decile monotonicity nézeteket tartalmazza
- [ ] A végső interpretáció az új model selection logikát tükrözi

## Notes

2026-06-20: Notebook elkészítve és sikeresen renderelve.

**Technikai döntések:**
- A `sample_train_valid.parquet` tartalmaz `pred_long` oszlopot (modell már train-nél kiprediktált) — nem szükséges újra train/predict futtatás
- `sample_oos.parquet`: 525 600 sor (2022 teljes év, minden perc), `pred_long` szintén megvan
- `search_best.json` csak 2 foldot tartalmaz (smoke stage 2 fold volt aktív) — a 4 fold CV metrikák a parquet `pred_long` alapján kerülnek számításra
- Az első 60 search trial (RMSE objective) nem tartalmaz top10_lift mezőt — a vizualizációk csak a 3 walk-forward trialt (61-63) mutatják

**Szekciók:**
1. Cél és metodológia leírás
2. Walk-forward fold séma tábla + Gantt diagram
3. Sample eloszlás fold_id szerint (15 312 sor total)
4. Search eredmények Top10 Lift (trial 61-63) + scatter
5. Best trial fold-szintű metrikák (search_best.json, 2 fold)
6. CV stabilitás: 4 fold pred_long alapján számítva — Top10 Lift, Spearman, decile monotonicity
7. OOS értékelés (2022): metrika summary + decile bar chart
8. Döntési szempontok: programmatikusan generált interpretáció

**Output:** `_doc_/5600_model_2021_train_valid_analysis.html` — 2344 KB, sikeresen renderelve.
