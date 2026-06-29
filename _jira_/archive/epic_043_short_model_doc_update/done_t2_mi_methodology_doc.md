---
epic: epic_043
id: t2
title: MI módszertani dokumentum létrehozása (2015_mutual_information.md)
assignee: methodology_agent
status: pr
blocks: []
blocked_by: [t1]
---

## Goal

Új fájl: `_doc_/methodology_doc/2015_mutual_information.md`

Átfogó módszertani összefoglaló a Mutual Information alapú feature szelekcióról, különös tekintettel a k-NN becslőre és a rank-transform motivációjára.

## Scope

- Új fájl: `_doc_/methodology_doc/2015_mutual_information.md`
- Nincs kódváltozás

## Acceptance Criteria

- [x] Fájl létezik és teljes
- [x] Képletek LaTeX inline math formátumban ($I(X;Y) = H(Y) - H(Y|X)$, stb.)
- [x] Legalább 1 Mermaid diagram (valójában 6 diagram készült)
- [x] Tartalmaz konkrét példát a short target eloszlásával
- [x] Mind a hat kötelező metodológiai szekció jelen van és nem üres

## Notes

2026-06-28 — Dokumentum elkészítve. Tartalom:

- MI definíció és entrópia-alapú képlet ($I(X;Y) = H(X) + H(Y) - H(X,Y)$)
- Korreláció vs. MI összehasonlítás táblázattal és diagrammal
- KSG k-NN becslő mechanizmusa flowchart-tal
- Skewed target probléma: short_mfe_fw60 és long_mfe_fw60 eloszlás-jellemzők
- Rank-transform MI-invariancia: bizonyítás vázlata ($H(f(Y)) - H(f(Y)|X) = H(Y) - H(Y|X)$)
- Implementáció: `co-iv-precompute` cella kódrészlet magyarázattal
- 6 Mermaid diagram: pipeline overview, alternatívák, k-NN mechanizmus, eloszlás-illusztráció, rank-transform flowchart, MI invariancia
- Mind a hat kötelező X100 szekció: Miért kritikus / Miért ezt / Kulcsfogalmak (3 db) / Paraméterek / Kockázatok / Validációs checklist
