---
epic: epic_7
id: t7
title: Hozd létre _doc_/3150_create_sample.md
assignee: doc_agent
status: pr
blocked_by: [t2]
---

## Goal

Dokumentálni a sampling orchestratort (`create_sample.py`) és a CLI belépési pontot
(`00_create_sample.py`).

## Scope

- Létrehozandó: `_doc_/3150_create_sample.md`
- Forrás:
  - `src/modeling/quantitative/sampling/create_sample.py`
  - `src/modeling/quantitative/00_create_sample.py`

## Acceptance Criteria

- [ ] `sequenceDiagram`: CLI → create_sample → audit → splits → write_sample_artifacts
- [ ] `create_sample(config)` lépései dokumentálva (resolve paths → audit → splits → metadata → write)
- [ ] `utils.load_asset_config` hívás kontextusa: miért csak ez importál utils-t
- [ ] CLI argumentumok táblázata (00_create_sample.py argparse)
- [ ] Példa CLI hívás kódblokkban
- [ ] Output summary (print sorok) leírva
