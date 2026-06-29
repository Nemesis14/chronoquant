---
epic: epic_043
title: Short model & dual-strategy dokumentáció frissítés
status: active
---

## Goal

Dokumentálni a short champion modell fejlesztése során hozott módszertani döntéseket és kódváltozásokat:
- Rank-transform általánosítása az MI számításhoz (long + short)
- Két önálló stratégia session saját cutoff-okkal
- UI és pipeline változások

## Tasks

- t1: Rank-transform általánosítás FE notebook-ban → modeling_agent
- t2: MI módszertani dokumentum (2015_mutual_information.md) → methodology_agent
- t3: FE methodology frissítés rank-transformra (2010) → methodology_agent
- t4: Strategy methodology frissítés (6300 két session, cutoff split) → methodology_agent
- t5: Live trading methodology frissítés (7100 service config split) → methodology_agent
- t6: Dashboard methodology frissítés (8100 UI változások) → methodology_agent
- t7: Code doc frissítés (binance_data, data.py, service.py, strategy.py) → code_doc_agent
- t8: Notebook → artifact mapping (melyik .ipynb melyik artifact mappába) → analyst_agent
- t9: Validáció → validator_agent

## Key Decisions

- Rank-transform: monoton transzformáció, MI-ra invariáns → általánosítható long-ra is
- Stratégia split: `strategy_session_long_id` + `strategy_session_short_id` config.json-ban
- Backfill: chunked (6 hónapos ablak) a segfault elkerüléséhez
