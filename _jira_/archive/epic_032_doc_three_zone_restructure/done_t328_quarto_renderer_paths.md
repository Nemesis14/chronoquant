---
epic: epic_032
id: t328
title: Doc-renderer + _quarto.yml + CSS útvonalak frissítése
assignee: analyst_agent
status: done
blocks: [t329]
blocked_by: [t327]
---

## Goal
Az analyst doc-renderelő lánc igazítása az új 3-zónás `_doc_` struktúrához, hogy a
Quarto HTML render és a konszolidált doc-notebook továbbra is működjön.

## Scope
- `src/analyst/_quarto.yml` — input/output útvonalak, projekt-struktúra
- doc-renderer (`analyst/doc_renderer/build_doc_notebook.py` és társai) — zóna-bejárás
- `analyst/chronoquant_analysis.css` — ha útvonal-függő
- Hivatkozás: t327 (új fájl-elhelyezés)

## Acceptance Criteria
- [ ] a doc-renderer bejárja a három zónát a helyes sorrendben
- [ ] Quarto render hibamentesen lefut az új struktúrán
- [ ] TOC + Mermaid render változatlanul működik
- [ ] nincs törött útvonal a renderelő láncban

## Notes
- `analyst/doc_renderer/build_doc_notebook.py`: `doc_sources()` átírva — a `_doc_` gyökér
  (0000/0001) + a három zóna-alkönyvtár bejárása, topic-szám szerint rendezve, így a
  reading order (methodology X000/X100 → kód X110) megmarad. Archive és `_plans_` kizárva.
- Smoke: build hibamentes, 48 doc-szekció helyes sorrendben (0000…8150).
- `_quarto.yml` + CSS (`analyst/chronoquant_analysis.css`) útvonal változatlanul érvényes
  (analyst/ nem mozdult). `quarto_analysis_defaults.md` render-output útvonal models_doc-ra
  + CSS relatív útvonal (`../../analyst/...`) frissítve.
- Megjegyzés: a `quarto` bináris tényleges HTML-renderje az analyst_agent felelőssége;
  a renderelő-lánc (notebook-build) hibamentesen lefut.
