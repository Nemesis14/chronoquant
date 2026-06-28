---
epic: epic_039
id: t10
title: Code docs frissítés — 5300_create_sample + 5520_search + 0004_model_lifecycle
assignee: code_doc_agent
status: pr
blocks: []
blocked_by: [t2, t3]
---

## Goal

A kód-referencia dokumentumok frissítése az új sampling és search implementáció
alapján. A `0004_model_lifecycle.md` a teljes modell-pipeline-t dokumentálja —
a train/valid split bevezetése után a sampling és search lépések leírása változik.

## Scope

- `_doc_/database_and_code_doc/5300_create_sample.md`
- `_doc_/database_and_code_doc/5520_search.md`
- `_doc_/database_and_code_doc/0004_model_lifecycle.md`

## Acceptance Criteria

- [x] `5300_create_sample.md`: új `train_valid_split` sampling mode dokumentálva
      (paraméterek, embargo logika, split indicator oszlop)
- [x] `5300_create_sample.md`: walk-forward CV referenciák eltávolítva vagy archív-jelöléssel ellátva
- [x] `5520_search.md`: CV fold-split függvény referenciák frissítve (eltávolítva)
- [x] `5520_search.md`: új objective, patience stopping, best trial selection dokumentálva
- [x] `5520_search.md`: `search_trials.jsonl` séma frissítve (`train_top10_lift`, `valid_top10_lift`, `train_valid_gap` mezők)
- [x] `0004_model_lifecycle.md`: sampling és search lépések leírása konzisztens az új kóddal

## Notes

Csak kód-referencia és technikai leírás — metodológiai rationale a
`5400_sampling.md`-ben és a `5500_hyper_param_search.md`-ben van (t9 task).

[code_doc_agent] Elvégezve — 2026-06-23

**5300_create_sample.md** — teljes újraírás:
- Új `train_valid_split` mode mint aktív path dokumentálva (top-szintű táblázat mode-ok összehasonlítással)
- `create_snapshot_sample_train_valid_split` függvény részletes leírása (paraméter tábla, visszatérési kulcsok, flowchart, sequenceDiagram)
- `TrainValidSplitConfig` dataclass összes mezője dokumentálva
- SQL logika részletesen: feature lookback embargo + target purge mechanizmus, QUALIFY ROW_NUMBER hourly select, `split` TINYINT oszlop definíciója
- `create_snapshot_sample` (walk-forward) megtartva legacy/archív jelöléssel
- Walk-forward CV referenciák archív-jelöléssel ellátva (`[aktív]` / `[walk-forward, legacy]` annotáció)

**5520_search.md** — teljes újraírás:
- CV fold-split függvény referenciák (`_fold_split_walk_forward`, `_fold_split_4fold`, `_load_model_sample_meta`, `fold_limit` paraméter) eltávolítva
- `run_search` paraméter tábla frissítve (eltávolítva: `fold_limit`; frissítve: `n_trials` default 100)
- `_SearchDataset` és `DatasetSplit` dataclass-ok dokumentálva
- Új objective `valid_top10_lift` dokumentálva (top 10% mechanizmus diagram)
- `_check_patience` logika dokumentálva (patience=20, epsilon=0.001, flowchart)
- `_select_best_trial` logika dokumentálva (valid max + gap tiebreaker, top-5 pool)
- `search_trials.jsonl` séma frissítve: `train_top10_lift`, `valid_top10_lift`, `train_valid_gap` mezők, régi `mean_top10_lift`/`std_top10_lift`/`fold_summary` mezők eltávolítva

**0004_model_lifecycle.md** — célzott frissítés:
- 2. Sample szekció: új mód-aware flowchart (`train_valid_split` vs `walk_forward` ág), `split` TINYINT dokumentálva, walk-forward paraméterek helyett train/valid split paraméterek
- 4. Hyperparameter Search szekció: flowchart frissítve (`split col` felirat), `valid_top10_lift` objektív és patience stopping megemlítve, `_select_best_trial` logika leírva
