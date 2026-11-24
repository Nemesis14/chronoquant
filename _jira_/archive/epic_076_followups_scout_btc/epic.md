# Epic 076: Utómunkák — epic_074/075 follow-upok + dokumentáció lezárás

**Created:** 2026-11-24
**Status:** DONE (2026-11-24)

## Goal
Az epic_074 (Scout élesben) és epic_075 (BTC élesítés) során talált kisebb hibák javítása és a dokumentáció lezárása.
User jóváhagyás 2026-11-24: dashboard vizuális ellenőrzés OK; code_doc lezárás OK; CLAUDE.md delegation `src/scout/` → modeling_agent OK.

## Tasks
- t947: SOL/BCH predikciók elszórt 1 perces gapjei (database_agent)
- t948: Chat gomb kitakarja a Scout panelt + MCP scout_signals barátságos hiba (ui_agent)
- t949: Store tesztek lock-retry + snapshot tartalom-hash (database_agent)
- t950: Dokumentáció lezárás epic_074/075/076 (code_doc_agent)

Sorrend: t947 ∥ t948 ∥ t949 → t950

## Quant-felülvizsgálat backlog (a következő, user által tervezett epichez)
- **Train/serve skew (t947 validátor):** `feat_obv`, `feat_obv_roc_14`, `feat_ad_line`, `feat_weekly_open_return` a fetch-ablak kezdetétől függ (kumulatív / ablak-horgonyzott) → train 3 hónapos chunk, live 2 napos ablak; backfill vs 1-soros számítás eltér (weekly_open_return ~72% rel.). Mind a 6 champion használja. Javítás vagy kizárás a feature-szűrésnél.
- VWAP-család (távolság, sávok, meredekség, anchored napi VWAP), előző napi H/L, volume profile, funding.
- Jelzéssűrűség-elemzés: elmúlt 1/2/4h top-q long/short arány → következő 2/4/8h hozam (blokkos statisztika).
