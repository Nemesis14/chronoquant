---
epic: epic_033
id: t39
title: Validálás — dead-link scan + konzisztencia
assignee: validator_agent
status: done
blocks: []
blocked_by: [t34, t35, t36, t37, t38]
---

## Goal
Az epic záró validációja: a database_and_code_doc zóna konzisztens, a linkek élnek, és a
felfelé-linkek létező methodology célokra mutatnak.

## Scope
- Dead-link scan az egész `_doc_/`-ra (különösen a database_and_code_doc → methodology felfelé-linkek)
- Konzisztencia-ellenőrzés: a doksi nem mond ellent a 0003/0004 folyamatleírásnak
- A megnyitott methodology `todo_` ticketek (Entry Gate) számbavétele
- (Nincs .py változás várhatóan; ha mégis, ruff+pyright a megérintett fájlon)

## Acceptance Criteria
- [x] Nincs törött relatív link a database_and_code_doc-ban
- [x] Minden felfelé-link létező methodology fájlra mutat
- [x] Minden pr_ task done_-ra mozgatva (vagy todo_-ra visszadobva indoklással)

## Notes
A doksi-audit nem futtatta végig az ML-pipeline-t; a futtatás-helyesség igazolása külön user-kérésre.

### Elvégzett validáció (2026-06-22)

- Dead-link scan lefutott a teljes `_doc_/database_and_code_doc/` zónára: `dead-links 0`
- Külön javítva a `4100_quant_train.md` három hibás forráslinkje (`../../src/...`)
- Konzisztencia drift javítva a `0004_model_lifecycle.md`-ben:
  nincs `model.__train_input` view, a search/train input közvetlen
  `snap."<snapshot_id>" ⋈ model."<model_id>__sample"` JOIN
- Stale-szöveg ellenőrzés lefutott a FE follow-up által érintett doksikra: `stale-count 0`
- Task fájlok átmozgatva `done_` prefixre: t34–t40
