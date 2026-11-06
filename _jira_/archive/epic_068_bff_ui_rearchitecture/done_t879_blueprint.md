---
epic: epic_068
id: t879
title: Blueprint: API-kontraktus, report-formátum, célstruktúra
assignee: code_doc_agent
status: pr
blocks: [t880, t881, t884, t886]
blocked_by: []
---

## Goal
Rögzítsd az epic kontraktusait, mielőtt kód készül.

## Scope
`_doc_/_plans_/068_bff_ui.md` (új); a life_plan `backend/main.py`, `chat_runner.py`, `frontend/src` mintaként.

## Acceptance Criteria
- [x] Célmappa-struktúra (src/api, src/worker, frontend/, scripts/, _doc_/research/)
- [x] REST/SSE endpoint-lista (market, trades, jobs, chat, reports)
- [x] Report-formátum: `reports/<scope>/<slug>/report.md` + `charts/*.json` (Plotly JSON) + `tables/*.json`, manifest séma
- [x] Törlési lista (fájl/mappa szinten) a t890-hez
- [x] Döntés: Plotly vs Vega-Lite (előbb `analyst/plot_utils` ellenőrzése)

## Notes
- Kimenet: `_doc_/_plans_/068_bff_ui.md` (az epic kontraktusa; eltérés csak ennek a fájlnak a javításával).
- Chart-lib: **Plotly JSON** megerősítve. Az `analyst/lib/plot_utils.py` (a ticket `analyst/plot_utils`-ként hivatkozta) matplotlib/seaborn, a 4 notebook-sablon is matplotlib -> sem Plotly, sem Vega-Lite nem átvehető, az újraírás úgyis kell; a `plotly` már függőség és a `src/ui/components/charts.py` Plotly.
- Eltérés az epic szövegétől: az MCP csomag `src/cq_mcp/` (nem `src/mcp/`), mert a `pytest`/runtime `pythonpath=src` miatt a `src/mcp` árnyékolná a PyPI `mcp` SDK-t.
- Riport-gyökerek: `artifacts/<model_id>/reports/<slug>/` (model) és `_doc_/reports/<slug>/` (general); a ticket `reports/<scope>/<slug>/` formáját ez konkretizálja (O-3).
- Nyitott: **O-1** `research/tv_report/` a chat piaci-hangulat funkciójának motorja (`utils.py:385` allowed_bash, tv_sentiment_skill, persona) -> a t886 ehhez csak a user döntése után nyúlhat; **O-2** `src/trading` sorsa (t889).
- Törlési lista megállapítások: `reg.artifacts` 90 sora hivatkozik az `oos_champ_*`/`wf_champ_*`/`netcheck` mappákra, 18 sor `.html/.ipynb` útvonalra -> t890-nek registry-tisztítás is kell. `docs/` csak 2 .agent sorban hivatkozott; `tools/` teljesen gitignored; `trading_reports/` a `config/trading.json` journal.report_dir kimenete (t889-függő).
- Kritikus kockázat rögzítve (§5.4): DuckDB-ben írásra nyitott fájlt másik processz read-only módban sem nyithat -> API rövid életű RO kapcsolat + retry + `db_busy`, worker csak az író szakaszokra nyit.
- Nem módosult alkalmazáskód; nem törölt semmit.
