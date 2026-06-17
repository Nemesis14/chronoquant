---
epic: epic_7
id: t3
title: Hozd létre _doc_/3110_sampling_config.md
assignee: doc_agent
status: pr
blocked_by: [t2]
---

## Goal

Dokumentálni a `SamplingConfig` dataclass-t.

## Scope

- Létrehozandó: `_doc_/3110_sampling_config.md`
- Forrás: `src/modeling/quantitative/sampling/config.py`

## Acceptance Criteria

- [ ] `classDiagram` a SamplingConfig mezőivel és típusaival
- [ ] Mezők táblázata: name | type | default | leírás
- [ ] Kiemelve: `embargo_minutes` None-semantikája (fallback = target_horizon_minutes)
- [ ] Példa inicializálás kódblokkban
