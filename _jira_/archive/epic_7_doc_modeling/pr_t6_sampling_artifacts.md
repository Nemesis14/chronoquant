---
epic: epic_7
id: t6
title: Hozd létre _doc_/3140_sampling_artifacts.md
assignee: doc_agent
status: pr
blocked_by: [t2]
---

## Goal

Dokumentálni az artifact IO modulját: írás, olvasás, validálás.

## Scope

- Létrehozandó: `_doc_/3140_sampling_artifacts.md`
- Forrás: `src/modeling/quantitative/sampling/artifacts.py`

## Acceptance Criteria

- [ ] `flowchart TD`: write_sample_artifacts → 3 JSON fájl a sample_dir alatt
- [ ] `sequenceDiagram`: load_sample_definition → metadata.json + folds.json → merged dict
- [ ] `write_sample_artifacts` paramétertáblázat + `generated_at` injektálás megemlítve
- [ ] `load_sample_definition` return dict struktúrája (mit vár a lightgbm_model)
- [ ] `validate_sample_definition` logikája: chronológiai sorrend ellenőrzés
- [ ] A 3 JSON fájl (metadata.json, folds.json, audit.json) tartalmának sémája
