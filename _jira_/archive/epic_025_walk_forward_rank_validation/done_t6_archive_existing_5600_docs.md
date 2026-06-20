---
epic: epic_025
id: t6
title: Meglévő 5600 modell dokumentáció archiválása
assignee: analyst_agent
status: pr
blocks: [t9]
blocked_by: [t5]
---

## Goal

A jelenlegi 5600-as modellhez tartozó elemzési és modell-dokumentációs anyagokat
archív állapotba mozgatni, hogy az új rerun eredménye legyen az aktív referencia.

## Scope

- `_doc_/5600_model_2021_train_valid_analysis.*`
- kapcsolódó cache / output artifactok
- szükség esetén `_jira_` hivatkozások

## Acceptance Criteria

- [ ] A jelenlegi 5600-as analysis anyag archívként megkülönböztethető
- [ ] Nem marad félreérthető aktív referencia a régi validációs logikára
- [ ] Az új analysis output számára tiszta célfájl-struktúra van

## Notes

2026-06-20: Átnevezve PowerShell Rename-Item-mel:
- `5600_model_2021_train_valid_analysis.ipynb` → `5600_model_2021_train_valid_analysis_ARCHIVE_random_week_cv.ipynb`
- `5600_model_2021_train_valid_analysis.html` → `5600_model_2021_train_valid_analysis_ARCHIVE_random_week_cv.html`
Cache mappa nem létezett. Az új 5600-as fájlok (t8) számára tiszta célhely.
