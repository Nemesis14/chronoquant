---
epic: epic_026
id: t10
title: Champion model config és walk-forward sampling bővítés
assignee: modeling_agent
status: done
blocks: []
blocked_by: []
---

## Goal

Konfigurálni a champion modellt (`lgbm_solusdt_l/s_fw60_2101_2605`), amely a 2023-01 → 2026-05
periódust fedi, 4-fold walk-forward CV-vel (train=9m, valid=8m, shift=8m).

## Scope

- `config/models.json` — 2 új model entry (l + s, `_2101_2605` suffix)
- `src/modeling/sampling/config.py` — `WalkForwardSamplingConfig`: `n_folds` mező hozzáadva
- `src/modeling/sampling/create_sample.py`:
  - `create_walk_forward_sample`: `config.n_folds` helyett hardcoded 4
  - `create_model_walk_forward_sample`: year + params olvasása `meta["sampling"]`-ból

## Acceptance Criteria

- [x] `lgbm_solusdt_l_fw60_2101_2605` és `s` elérhető models.json-ban
- [x] `WalkForwardSamplingConfig` rendelkezik `n_folds: int = 4` mezővel
- [x] `create_walk_forward_sample` `config.n_folds`-t használ (nem hardcoded 4)
- [x] `create_model_walk_forward_sample` olvassa a `year`, `train_months`, `valid_months`, `shift_months`, `n_folds` értékeket `meta["sampling"]`-ból
- [x] Backward compatible: meglévő modellek (ahol nincs `year` a sampling-ban) a régi logikával működnek

## Notes

CV struktúra (anchor=2023, train=9, valid=8, shift=8, n_folds=4):

| Fold | Train ablak        | Valid ablak          |
|------|--------------------|----------------------|
| 1    | 2023-01 → 2023-09  | 2023-10 → 2024-05    |
| 2    | 2023-09 → 2024-05  | 2024-06 → 2025-01    |
| 3    | 2024-05 → 2025-01  | 2025-02 → 2025-09    |
| 4    | 2025-01 → 2025-09  | 2025-10 → 2026-05    |

Implementáció: 2026-06-20 — modeling_agent
Validáció: 2026-06-20 — validator_agent
  - ruff check: 0 hiba
  - pyright: 0 hiba, 0 warning
  - 8 smoke teszt átment (_tests/modeling/sampling/test_walk_forward_config.py):
    WalkForwardSamplingConfig n_folds default/explicit/frozen, year-olvasás 3 ág (explicit, _yearly_, generic fallback), n_folds default fallback, champion model full config build
