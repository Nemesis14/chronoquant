---
epic: epic_020
id: t2
title: _doc_/5500_hyper_param_search.md létrehozás
assignee: methodology_agent
status: pr
---

## Goal

X100 szintű módszertani dokumentáció a hyperparameter search folyamathoz.

## Scope

- `_doc_/5500_hyper_param_search.md` — új fájl
- `_doc_/5000_modelling.md` — fejezetek táblájába 5500 felvétele

## Acceptance Criteria

- [ ] Search objective leírva (stability-penalized log loss formula)
- [ ] Stage-ek (smoke/explore/refine) célja és paraméterei
- [ ] CV struktúra: 12 fold a selected_valid_weeks alapján
- [ ] Bináris label generálás: quantile thresholding módszertana
- [ ] Guardrail döntés indoklásal (legacy champion guardrail eltávolítva)
- [ ] Input/output artifact layout leírva
- [ ] 5000_modelling.md fejezetek táblájában megjelenik a 5500-as oldal

## Notes

