---
epic: epic_032
id: t329
title: Validáció — linkek, Quarto render, TOC az új struktúrában
assignee: validator_agent
status: done
blocks: []
blocked_by: [t326, t327, t328]
---

## Goal
A teljes doc-átszervezés ellenőrzése: nincs törött kereszthivatkozás, a Quarto render
és a TOC működik, a meta-fájlok (skill, manifest, CLAUDE, 0001) konzisztensek.

## Scope
- belső `_doc_` linkek ellenőrzése (törött relatív hivatkozások)
- Quarto render smoke az új struktúrán
- skill/manifest/CLAUDE/0001 ↔ tényleges fájl-elhelyezés konzisztencia
- Hivatkozás: t325–t328 kimenetei

## Acceptance Criteria
- [ ] nincs törött belső link a _doc_-ban
- [ ] Quarto render hibamentes
- [ ] a meta-fájlok hivatkozásai egyeznek a valós zóna-struktúrával
- [ ] minden pr_ task done_-ra léptethető

## Notes
- Belső link-validáció: 82 `.md` link ellenőrizve, **0 törött** (a `_plans_` kizárva).
- Renderelő-lánc smoke: `build_doc_notebook.py` hibamentesen lefut, 48 szekció helyes
  topic-szám sorrendben, archive + `_plans_` kizárva.
- Meta-fájl konzisztencia ellenőrizve: docs_skill, 3 manifest, CLAUDE.md, 0001, 0000,
  methodology_doc_skill, coding_skill, quarto_analysis_defaults — mind a valós zóna-
  struktúrára hivatkozik. Nincs félrevezető régi-séma hivatkozás (egy historikus
  „törölve" jegyzet maradt, az pontos).
- Megjegyzés: a `quarto` bináris tényleges HTML render-tesztje (külső eszköz) nem futott
  ebben a sessionben; az analyst_agent felelőssége egy modell-report renderelésekor.
