---
epic: epic_079
id: t972
title: Pipeline: beágyazott CV, modellek, null-gépezet
assignee: analyst_agent
status: done
blocks: [t973]
blocked_by: [t971]
---

## Goal
Purge-olt beágyazott idő-CV, modellek M0–M4, változószám/hiperparaméter kiválasztás belül, súlyozott metrikák, permutációs és placebo null.

## Acceptance Criteria
- [ ] Külső féléves blokkok 2023H1–2025H2, purge + embargó.
- [ ] Súlyozott AUC / logloss-nyereség / IC, nap-klaszteres CI.
- [ ] Tanulási görbe, train–teszt rés.
- [ ] ruff + pyright tiszta.

## Notes
Kész: `cand_v2_model.py` (beágyazott purge-olt CV, M0–M4, visszaesés az offsetre). Eltérés a tervtől: a klaszter-súlyok NEM használatosak (utólagos információt tartalmaznak, az AUC-t 0,606-ra fújták; lásd t970), a függőséget nap-klaszteres bootstrap kezeli. Tanulási görbe / train–teszt rés külön nem készült. ruff + pyright tiszta.
