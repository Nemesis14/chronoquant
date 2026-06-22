---
epic: epic_032
id: t326
title: Manifestek + CLAUDE.md + 0001 scope-frissítés a 3-zónára
assignee: code_doc_agent
status: done
blocks: [t329]
blocked_by: [t325]
---

## Goal
A meta-réteg igazítása az új doc-struktúrához: az érintett agent manifestek scope path-jai,
a CLAUDE.md delegation table, és a 0001_agentic_system.md dokumentációs szakasza.

## Scope
- `.agent/agents/code_doc_agent.md` — scope: `_doc_/database_and_code_doc/` (kizárólagos író)
- `.agent/agents/methodology_agent.md` — scope: `_doc_/methodology_doc/` (kizárólagos író)
- `.agent/agents/analyst_agent.md` — scope: `_doc_/models_doc/` (kizárólagos író; forrás: modeling artifact + registry)
- `CLAUDE.md` — delegation table doc-hivatkozások
- `_doc_/0001_agentic_system.md` — 9. szakasz (számozási/zóna séma) frissítés
- Hivatkozás: t325 (docs_skill) a célstruktúra forrása

## Acceptance Criteria
- [ ] mindhárom manifest scope path-ja a megfelelő zóna-mappára mutat, kizárólagos íróként
- [ ] analyst_agent manifest: a `models_doc` modellenkénti `.ipynb`, Quarto+CSS, methodology-hivatkozás
- [ ] CLAUDE.md delegation table konzisztens az új zónákkal (a `_doc_` sorok átírva)
- [ ] 0001 9. szakasz az új 3-zónás sémát írja le (diagram frissítve)
- [ ] nincs hivatkozás a régi flat sémára egyik meta-fájlban sem

## Notes
- 3 manifest scope átírva kizárólagos íróra: code_doc→database_and_code_doc,
  methodology→methodology_doc, analyst→models_doc (forrás: modeling artifact+registry).
- CLAUDE.md delegation table 3 doc-sora átírva a zónákra.
- 0001 9. szakasz újraírva (3-zóna diagram + szint/zóna tábla + Entry Gate + egy-irányú link);
  0001 ownership-diagram, agent-referencia tábla, skills-tábla, 13. delegation tábla frissítve.
- Extra konzisztencia: 0000 (intro, repo-layout, ownership tábla), methodology_doc_skill,
  coding_skill, quarto_analysis_defaults régi `_doc_/analysis/` és flat hivatkozásai frissítve.
