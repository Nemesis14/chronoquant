---
epic: epic_075
id: t945
title: ETHUSDT fw60 long/short modell + stratégia
assignee: modeling_agent
status: cancelled
blocks: [t946]
blocked_by: [t944]
---

## Goal
A BCH bevezetés (epic_069, `lgbm_bchusdt_l/s_fw60_2101_2607`) mintájára: `lgbm_ethusdt_l/s_fw60_2101_2607` — snapshot, sampling, training, hyperparam search, isotonic kalibráció + Optuna stratégia sweep, artifactok, reg bejegyzések, riportok a UI-ra. Ugyanaz a train range/holdout fegyelem mint SOL/BCH. A worker-t ne állítsd le; ha DB lock van, várj/retry.

## Scope
`src/modeling/`, `src/strategy/`, `artifacts/`, lab DB, model_lifecycle_skill

## Acceptance Criteria
- [ ] Modell + stratégia artifact, riportok
- [ ] Eredmények összevetése SOL/BCH-val a ticketben
- [ ] Doksi: models_doc / 0004 érintett része

## Notes
- (orchestrátor, 2026-11-24) **CANCELLED** — user döntés: ebben a fejlesztésben nincs quant modell implementáció; a quant modellek újragondolása a Scout után külön epicben.
