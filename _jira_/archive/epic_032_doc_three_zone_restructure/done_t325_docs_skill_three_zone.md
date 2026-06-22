---
epic: epic_032
id: t325
title: docs_skill átírása flat→3-zóna
assignee: code_doc_agent
status: done
blocks: [t326, t327]
blocked_by: []
---

## Goal
A `docs_skill.md` átírása: a flat, alkönyvtár nélküli séma helyett három globális zóna
(implementation / methodology / results), a téma-számozás megtartásával. Ez definiálja
a célstruktúrát a többi task számára.

## Scope
- `.agent/skills/docs_skill.md` — zóna-definíciók, alkönyvtár-szabály, számozás zónán belül
- zóna ↔ agent ↔ szint (X000/X100/X110/ipynb) leképezés
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 13; az epic.md Key Decisions

## Acceptance Criteria
- [ ] docs_skill leírja a három zónát: `database_and_code_doc/`, `methodology_doc/`, `models_doc/`
- [ ] **single writer / zóna**: code_doc_agent / methodology_agent / analyst_agent — kizárólagos
- [ ] a flat „nincs alkönyvtár" szabály lecserélve zóna-alkönyvtár szabályra
- [ ] téma-számozás (1xxx, 5xxx …) zónán belül megtartva, kereszthivatkozási konvencióval
- [ ] a 3. zóna (results) = registry + rendered report elv rögzítve
- [ ] **formátum-szabály zónánként**: database_and_code_doc + methodology_doc = `.md`; models_doc = `.ipynb` (+ rendered `.html`)
- [ ] **kereszthivatkozás egy-irányú**: code→methodology kötelező; methodology kód-mentes, nem linkel lefelé
- [ ] **models_doc**: modellenként egy `.ipynb`, methodology-hivatkozásokkal, Quarto+CSS+paletta
- [ ] Entry Gate (methodology előbb mint kód-doc) szabály megmarad
- [ ] navigáció a beszédes fájlnevek alapján (Glob/Grep); nincs külön TOC-index

## Notes
- `docs_skill.md` átírva: 3 zóna (database_and_code_doc / methodology_doc / models_doc),
  single-writer/zóna, tartalom-szerinti besorolás, zónán belüli számozás megtartva.
- Egy-irányú kereszthivatkozás (kód→methodology kötelező; methodology kód-mentes, nem linkel le).
- Formátum-szabály zónánként (.md vs .ipynb), models_doc = registry+rendered report elv,
  Entry Gate megtartva, navigáció Glob/Grep (nincs TOC-index). Mermaid/doc-típus szabályok megőrizve.
