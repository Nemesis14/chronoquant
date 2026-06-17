---
epic: epic_013
id: t1
title: Feature és target layer metodológiai dokumentáció
assignee: methodology_agent
status: pr
---

## Goal

Létrehozni a `_doc_/3200_features.md` és `_doc_/3300_targets.md` X100 szintű metodológiai dokumentumokat, amelyek rögzítik a feature layer és a target layer tervezési döntéseinek rationale-ját.

## Scope

- `_doc_/3200_features.md` — Feature layer metodológia (új fájl)
- `_doc_/3300_targets.md` — Target layer metodológia (új fájl)
- `_doc_/3000_modelling.md` — Fejezetek táblázat frissítése + aktív modellek target oszlopai javítva

## Acceptance Criteria

- [x] `_doc_/3200_features.md` létrehozva, mind a 6 szekció jelen van és nem üres
- [x] `_doc_/3300_targets.md` létrehozva, mind a 6 szekció jelen van és nem üres
- [x] Mindkét fájlban minimum 2–3 Mermaid diagram
- [x] `_doc_/3000_modelling.md` Fejezetek táblázatban szerepel 3200 és 3300 (kész)
- [x] `_doc_/3000_modelling.md` aktív modellek target oszlopai: `long_mfe_fw60`, `short_mfe_fw60` (régi bináris target referencia eltávolítva)
- [x] Alternatíva-táblázat mindkét fájlban legalább egy elvetett alternatívát tartalmaz
- [x] Validációs checklist >= 5 pont

## Notes

Forrásanyagok:
- `_jira_/story_target_layer_refactor_fw60_logreturns.md` — target rationale és as-is/to-be leírás
- `_jira_/story_features_overview.md` — feature csoportok, ablakok, warmup audit
- `_doc_/1240_sync_targets.md` — ground truth: jelenlegi target tábla oszlopok
- `_doc_/1250_features_polars.md` — ground truth: feature implementáció
- `docs/concepts/quantitative_features.md` és `docs/data/dictionary/features.md` — részben elavult, de feature groups átvéve
- `docs/data/dictionary/features.md` target szekció ELAVULT (`trg_l_fw60_q90` — nem egyezik a jelenlegi kóddal)

Megjegyzés: `docs/concepts/targets.md` és `docs/data/dictionary/features.md` legacy state-ben maradt — stale referenciákat tartalmaz. Ezek frissítése code_doc_agent hatásköre (X110+).
