# Epic 074: Impulse Scout élesben + dashboard

**Created:** 2026-11-23
**Status:** OPEN

## Goal
Az epic_073-ban kiválasztott Impulse Scout alapmodell (S0+C logit, W4h_H8h, a15) befagyasztása és éles futtatása
SOL + BTC + ETH-re: minden új megerősített mélypont előre rögzített (immutable) predikciót kap a kimenet előtt
(forward-log = a holdout-2 legtisztább formája), és a dashboardon chart-rétegként + panelként jelenik meg.
A Scout NO-GO státuszú → a UI-n „kísérleti – nem kereskedési jel” jelöléssel. Nem kerül a quant modellbe.

## Tasks
- t939: BTC/ETH élő szinkron bekapcsolása (database_agent) — epic_075 is erre épül
- t940: Scout modell befagyasztása (artifact) + futtató modul (modeling_agent)
- t941: `scout_signals` tábla a live sémában (database_agent)
- t942: `sync_scout` worker job + API végpont (ui_agent)
- t943: Dashboard: Scout chart-réteg + panel (ui_agent) → ⛔ user vizuális ellenőrzőpont

Sorrend: t939 ∥ t940 → t941 → t942 → t943

## Key Decisions
- **2026-11-23 (user):** Scout élesben SOL/BTC/ETH-re (BCH később, „modellen kívüli” jelöléssel); dashboard chart + panel;
  orchestrált végrehajtás. A modell + szabályok befagyasztva, további hangolás nincs. A live predikció a kimenet előtt
  íródik, utólag nem módosul. Előzmény: epic_073 (archivált), `_doc_/research/impulse_scout.md`, 5300.
- **2026-11-23 (t940):** runtime: új `src/scout/` csomag (`load_artifact`, `run`), artifact `artifacts/scout_s0c_r1/`, live_start 2026-11-01, detect_anchor 2026-07-24. Epic végén code_doc_agent: overview modul-lista + CLAUDE.md delegation (`src/scout/` → modeling_agent) — utóbbi user jóváhagyással.
- **Epic végi code_doc_agent teendők (gyűjtő):** overview modul-lista (`src/scout/`, `artifacts/scout_s0c_r1/`, live `scout_signals`, API `/api/scout/*`, MCP 20 tool, worker `sync_scout`); `.agent/agents/ui_agent.md` „19 tool” → 20; CLAUDE.md delegation `src/scout/` → modeling_agent (user jóváhagyással). Kisebb follow-up: MCP `scout_signals` rossz `since` → barátságos hiba; store tests conftest lock-retry; snapshot content hash nem tartalmaz értékeket (BTC=BCH hash).
