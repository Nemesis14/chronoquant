---
epic: epic_072
id: t926
title: research séma dokumentálása a 0002_data_architecture-ben
assignee: code_doc_agent
status: done
blocks: []
blocked_by: []
---

## Goal
A lab DB új `research` sémája (`wave_events_v1` + `__funnel`, `__meta`; hamarosan `wave_events_v2`)
hiányzik a `_doc_/database_and_code_doc/0002_data_architecture.md`-ből. Vedd fel: cél (kutatási, nem éles),
ki írja (report builderek), elnevezési konvenció (`<name>_v<n>`, `__funnel`, `__meta` hash), életciklus.

## Acceptance Criteria
- [ ] 0002 tartalmazza a `research` sémát a lab DB leírásában
- [ ] Konzisztens a 1110_duckdb_store.md-vel (ha ott is van séma-lista)

## Notes
- 0002_data_architecture.md: `research` séma felvéve (Overview LAB subgraph, 3-fájlos táblázat, séma-táblázat, új `### research séma — lab.duckdb` szakasz). Tábla-nevek a builder kódból ellenőrizve: `research."wave_events_v1"`, `__funnel`, `__meta`.
- `__meta` pontosítva: a kódban `params_json` + `content_hash` (events CSV SHA-256, 16 hex) — nem "config hash" önmagában.
- 1110_duckdb_store.md: nem tartalmaz lab-séma listát (nincs `snap`/`model`/`strat` említés) — nem módosítottam.
- Nem commitolva; a ticket neve és státusza változatlan.
- [validator] Validated (doc-only) 2026-11-20: table names (`research."wave_events_v1"`, `__funnel`, `__meta`), `__meta` columns (version, table, generator, params_json, ohlcv_first/last/rows, n_events, content_hash, created_at), persist/load_events lifecycle match `src/reports/builders/wave_events.py`; mermaid LAB nodes use the existing quoted/escaped pattern. No fixes needed.
