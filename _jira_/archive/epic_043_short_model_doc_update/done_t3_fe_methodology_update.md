---
epic: epic_043
id: t3
title: FE methodology frissítés — rank-transform bekezdés (2010)
assignee: methodology_agent
status: pr
blocks: []
blocked_by: [t1, t2]
---

## Goal

`_doc_/methodology_doc/2010_feature_engineering.md` frissítése: rank-transform döntés és motiváció hozzáadása, hivatkozás az új `2015_mutual_information.md`-re.

## Scope

- `_doc_/methodology_doc/2010_feature_engineering.md`

## Acceptance Criteria

- [x] Rank-transform bekezdés hozzáadva (miért, mikor, mit old meg)
- [x] Hivatkozás: `→ részletesen: 2015_mutual_information.md`
- [x] Általánosítás (long + short) megemlítve

## Notes

2026-06-28 — methodology_agent elvégezte:
- Új `### MI számítás és rank-transform: miért kell és hogyan működik?` szekció hozzáadva a "Négydimenziós feature-döntés" és "Sample-scope konzisztencia" szekciók közé
- Tartalom: k-NN torzítás skewed targetnél, rank-transform mechanizmus, MI invariancia monoton transzformációra, long + short alkalmazás indoklása, flowchart diagram
- `→ részletesen: 2015_mutual_information.md` hivatkozás a szekció végén
- "Ismert kockázatok és korlátok" táblába új sor: skewed target rank-transform nélkül
- "Validációs checklist" kibővítve rank-transform ellenőrzési ponttal (mind long, mind short modelleknél)
