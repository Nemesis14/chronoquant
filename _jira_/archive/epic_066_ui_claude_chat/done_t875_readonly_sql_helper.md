---
epic: epic_066
id: t875
title: Read-only DuckDB lekérdező helper a chatnek
assignee: database_agent
status: done
blocks: []
blocked_by: []
---

## Goal
CLI script, amit a chat Bash-sel hívhat: SQL lekérdezés a live/lab/registry DuckDB-n, szigorúan read-only.

## Scope
`src/data_handling/` alá új script (pl. `07_query_readonly.py`), a meglévő store/ATTACH minta szerint (`_doc_/database_and_code_doc/0002_data_architecture.md`). Config csak `src/utils.py`-on át.

## Acceptance Criteria
- [x] `uv run python <script> "<SQL>"` → eredmény JSON/markdown táblaként, sorlimit (default pl. 200)
- [x] read_only=True kapcsolatok (live, lab ATTACH, registry ATTACH); csak SELECT/WITH/DESCRIBE/SHOW engedett, minden más elutasítva
- [x] séma-listázó mód (táblák + oszlopok) a chatnek
- [x] a futó sync/trading írását nem blokkolja (read-only, rövid kapcsolat)
- [x] tesztek (elutasított DML/DDL, limit, séma-mód); ruff + pyright tiszta

## Notes
- Implementálva: `src/data_handling/07_query_readonly.py` (CLI + importálható függvények); tesztek: `src/data_handling/tests/query_readonly/test_query_readonly.py` (34 teszt, zöld; ruff + pyright tiszta).
- Kapcsolat: in-memory conn, `live`/`lab`/`reg` ATTACH (READ_ONLY), `USE lab` (snap./model./strat. unqualified), aliasok: `live.ohlcv`, `reg.models`. Hiányzó fájl kihagyva. Útvonalak csak `utils`-on át.
- Védelem: (1) pontosan 1 statement (`duckdb.extract_statements`), parser type SELECT, első kulcsszó SELECT/WITH/FROM/VALUES/DESCRIBE/SHOW/SUMMARIZE; COPY/ATTACH/INSTALL/LOAD/PRAGMA/SET/EXPORT/CALL/EXPLAIN/DML/DDL elutasítva; (2) `enable_external_access=false` + `lock_configuration=true` (read_csv stb. blokkolt); (3) READ_ONLY attach.
- CLI: `uv run python src/data_handling/07_query_readonly.py "<SQL>" [--limit 200] [--format json|markdown] [--timeout 60] [--asset-id X]`; `--schema [--filter substr] [--catalog live|lab|reg]`. Exit: 0 ok, 2 elutasítva/hibás használat, 3 DB hiba.
- Kimenet JSON: `{columns, rows, row_count, truncated, limit}`; schema JSON: `{tables:[{catalog,schema,table,type,columns:[{name,type}]}], table_count}`.
- Ismert korlát: ha másik folyamat RW-ben tartja a live.duckdb-t, a cross-process read-only ATTACH Windowson IOException (a helper 6x0.5s retry után exit 3). Teszt közben egyszer átmeneti lock előfordult (a Streamlit UI python processze), újrapróbálva rendben ment.
