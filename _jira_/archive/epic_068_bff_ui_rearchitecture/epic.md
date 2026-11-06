# Epic 068: BFF/UI újraarchitektúra, Quarto kivezetés, repo takarítás

## Goal
Streamlit helyett FastAPI BFF + React UI (a life_plan mintájára): a UI sosem blokkolódik, külön chat ablak közvetlen Claude-eléréssel, Quarto helyett natív Elemzések fül, rendezett és redundancia-mentes repo.

## Scope
`src/api`, `src/worker`, `src/mcp`, `frontend/`, `src/ui` (kivezetés), `analyst/`, `research/`, `artifacts/`, gyökér-takarítás, dokumentáció.

## Tasks
- t879: Blueprint: API-kontraktus, report-formátum, célstruktúra (code_doc_agent)
- t880: BFF váz (FastAPI) + read endpointok (ui_agent)
- t881: Worker/job-manager: sync és predikció háttérfolyamatban (ui_agent)
- t882: Frontend váz (React+Vite+Tailwind) + dashboard oldalak (ui_agent)
- t883: Chat: BFF + önálló ablak + chronoquant MCP (ui_agent)
- t884: Report-formátum + builder + /api/reports + Elemzések fül (analyst_agent)
- t885: Meglévő riportok konvertálása az új formátumra (analyst_agent)
- t886: research/ → rövid md összefoglalók, minden más törlése (analyst_agent)
- t887: Dead code + redundancia audit és refaktor: data_handling (database_agent)
- t888: Dead code + redundancia audit és refaktor: modeling + strategy (modeling_agent)
- t889: Audit: trading + régi Streamlit UI kivezetése (ui_agent)
- t890: Repo takarítás: gyökér, docs, tools, analyst, artifacts, függőségek (code_doc_agent)
- t891: Overview, agent manifestek, scriptek frissítése (code_doc_agent)
- t892: Epic 068 validálása (validator_agent)

## Key Decisions
- Chart-spec: Plotly JSON (t879 megerősíti az `analyst/plot_utils` ismeretében).
- Report: markdown + chart/table JSON, lazy betöltés kattintásra; nincs notebook/HTML.
- Live trading vezérlés kikerül (a user csak a journalt nézi); a `src/trading` sorsáról t889 javasol, a user dönt.
- research/ teljes törlése jóváhagyva; előtte rövid md összefoglaló (`_doc_/research/`).
- Végrehajtás: Mód A (szegmentált), agenttípusonként külön session.
- Epic_067 t878 (chat dock) beépül t883-ba.
