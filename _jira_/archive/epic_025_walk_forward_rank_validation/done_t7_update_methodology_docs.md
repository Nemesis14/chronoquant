---
epic: epic_025
id: t7
title: Módszertani dokumentáció frissítése a _doc mappában
assignee: analyst_agent
status: pr
blocks: [t8, t9]
blocked_by: [t2, t3, t4]
---

## Goal

Az érintett `_doc_` dokumentációkat frissíteni az új walk-forward validation,
Top10 Lift model selection és rank audit metodológia szerint.

## Scope

- `_doc_/5010_sampling_yearly.md` vagy annak utódja
- `_doc_/5400_sampling.md` releváns történeti megjegyzések
- `_doc_/5500_hyper_param_search.md`
- `_doc_/6100_calibration.md`
- szükség esetén `_doc_/5000_modelling.md`

## Acceptance Criteria

- [ ] A sampling metodológia új fold sémája dokumentálva van
- [ ] A search objective dokumentációja Top10 Liftre frissítve
- [ ] A Spearman és decile monotonicity audit kötelezőként szerepel
- [ ] A régi random-week validation aktívként nem marad a doksikban

## Notes

2026-06-20: Elvégzett frissítések:

**5010_sampling_yearly.md:**
- Hozzáadva kiemelő callout az Üzleti háttér szekcióban: Legacy vs. Walk-forward (ACTIVE) két mód megkülönböztetése
- Új `## Walk-forward CV` szekció a 4-fold weekly szekció után: fold séma tábla, konfiguráció, fold_time_windows metadata különbség, purge logika, miért walk-forward indoklás
- Per-fold méret összehasonlítás tábla bővítve 3 sorra: régi 12-fold, 4-fold stratifikált (Legacy), Walk-forward (ACTIVE)

**5500_hyper_param_search.md:**
- Intro sor: `stability-penalized RMSE` → `Top10 Lift fold-stability penalty`
- CV struktúra szekció: `selected_valid_weeks` → `fold_time_windows`, 12 fold → 4 walk-forward fold, explict időablakok
- Search Objective szekció teljes csere: RMSE-alapú formula → `## Elsődleges Objective` (Top10 Lift + Optuna direction)
- Új `## Rank Audit Metrikák` szekció: Spearman és decile monotonicity kötelező leírása
- Input tábla: `selected_valid_weeks` → `fold_time_windows`; `segment` → `fold_id`
- Search Stages tábla: 12 fold → 4 fold
- Fold metrikák tábla: bővítve top10_lift, spearman_rho, decile_monotonicity sorokkal
