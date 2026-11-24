---
epic: epic_078
id: t964
title: Riport és cikk
assignee: analyst_agent
status: todo
blocks: []
blocked_by: [t963]
---

## Goal
Egyszerű, átlátható riport (`_doc_/reports/cand_pred/`) és artifact-cikk: időtáv-táblázat, majd „melyik változócsoport mennyit ad a véletlenhez képest", magyarázó ábrákkal.

## Acceptance Criteria
- [ ] Riport a ReportBuilder formátumban + `_doc_/research/` összefoglaló.
- [ ] Cikk (artifact): cél, módszer, időtáv-táblázat, csoportonkénti eredmény, verdikt.
- [ ] ruff + pyright + pytest tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Riport: `_doc_/reports/cand_pred/` (`src/reports/builders/cand_pred_report.py`; 5 ábra, 8 tábla). Összefoglaló: `_doc_/research/cand_pred.md` + index sor a `_doc_/research/README.md`-ben.
- Cikk (artifact): https://claude.ai/artifact/25zyoLv7sD9EFhopQMx83K (privát). Időtáv-ábra + táblázat, változócsoportok, AUC modellenként valós vs placebo (két cél), egyváltozós AUC, verdikt.
- ruff + pyright (0 hiba) tiszta; meglévő `pytest src/reports` 183 passed. Új modulokhoz pytest nincs (analyst_agent nem ír) — a validátor feladata.
- Utólagos javítás a cikk előtt: a változók száma 48 (nem 51): a DB 1m quant-oszlopai záróár-alapú analógokra cserélve (placebón újraszámolható), t961/t963 jegyzet frissítve.
