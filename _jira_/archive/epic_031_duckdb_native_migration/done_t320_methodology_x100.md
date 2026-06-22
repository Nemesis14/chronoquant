---
epic: epic_031
id: t320
title: Metodológia X100 — 1400_snapshots + 1500_registry
assignee: methodology_agent
status: todo
blocks: [t321]
blocked_by: [t312]
---

## Goal
X100 metodológiai docok az új rétegekhez: snapshot (miért immutable, hash-séma,
range-szabályok) és registry (séma, entitás-életciklus, státuszok). A hat kötelező
X100 szekció a methodology_doc_skill szerint.

## Scope
- `_doc_/1400_snapshots.md` (X100)
- `_doc_/1500_registry.md` (X100)
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 4.2, 4.3, 13.1

## Acceptance Criteria
- [x] 1400_snapshots.md a hat kötelező X100 szekcióval + Mermaid diagramok
- [x] 1500_registry.md séma (erDiagram) + életciklus + a hat szekció
- [x] docs_skill számozási és Mermaid szabályok betartva
- [x] cross-reference a 0002-re, nem tartalom-ismétlés

## Notes

### Elvégezve (methodology_agent)

**Létrehozott / véglegesített fájlok:**
- `_doc_/methodology_doc/1400_snapshots.md` (X100) — egy korábbi futás már elkészítette; ellenőrizve: mind a hat kötelező X100 szekció jelen és nem üres, Mermaid szabályok betartva, kód-mentes, cross-reference a `0002_data_architecture.md`-re (nem ismétel topológiát). Változtatás nem kellett.
- `_doc_/methodology_doc/1500_registry.md` (X100) — **új**, ebben a futásban írva. Stílust az 1400-ból vette.

**1400_snapshots.md szekció-struktúra:**
`## Overview` → `## Üzleti és módszertani háttér` (Miért kritikus / Miért ezt a megközelítést? / Immutability / Content-hash / Range-szabályok+naming / Reprodukálhatóság / Paraméter alapértékek / Ismert kockázatok / Validációs checklist) → `## Kapcsolódó metodológia`.
Mermaid: 1× Overview flowchart TD, 1× alternatívák flowchart LR, 1× immutability stateDiagram-v2, 2× content-hash graph TD + flowchart TD, 1× naming flowchart TD, 1× reprodukció flowchart LR (7 diagram).

**1500_registry.md szekció-struktúra:**
`## Overview` → `## Üzleti és módszertani háttér` a hat kötelező szekcióval:
- Miért kritikus ez a lépés?
- Miért ezt a megközelítést? (alternatíva-táblázat 3 elvetett alternatívával + flowchart LR)
- A 8 entitás és relációik (erDiagram — a plan 4.3 szerint, mind a 8 entitás + FK-k)
- Entitás-életciklus és státuszok (stateDiagram-v2 draft→candidate→champion→active→archived + graph TD lánc)
- Idempotens upsert (flowchart TD)
- Config-gateway (graph TD)
- Paraméter alapértékek és indoklásuk
- Ismert kockázatok és korlátok
- Validációs checklist (9 pont)
→ `## Kapcsolódó metodológia`.
Mermaid diagramok: Overview flowchart TD, alternatívák flowchart LR, **erDiagram (8 entitás)**, életciklus stateDiagram-v2, lánc graph TD, upsert flowchart TD, gateway graph TD (7 diagram).

**Elvek betartva:**
- Kód-mentes; a „miért"/módszertan szintjén. Forrás: pr_t311 (reg 8 tábla, status default 'draft', migrations v1, default-séma döntés), pr_t312 (snapshot hash-séma, naming), plan 4.2/4.3/13.1.
- Cross-reference egy-irányú: a `methodology_doc` NEM linkel lefelé `database_and_code_doc`-ra; topológia → `0002_data_architecture.md` link, nem ismétlés.
- docs_skill Mermaid szabályok: opening fence column 0, lowercase `mermaid`, nincs emoji a node label-ekben (OK:/NO: használva), nem listába ágyazva.
- Path-konvenció: methodology_doc zóna, X100 számozás (1400, 1500) a domain 1000–1999 tartományban; a kód-X110 (`1410`, `1510`) magasabb szám, code_doc_agent territory.

**Minden acceptance criterion teljesült.**
