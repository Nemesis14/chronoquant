# Epic 066: Claude chat az UI-ban (előfizetéses CLI)

## Goal
A ChronoQuant Streamlit UI-n (egyetlen oldal) legyen egy chat-ablak, ami a helyi, bejelentkezett
`claude` CLI-hez (előfizetés, NEM API-kulcs) van kötve. A chatben lehessen: (1) DuckDB-adatot kérdezni
(read-only), (2) tv_market_sentiment piaci összképet kérni — a chat értse, mi az az elemzés, tudjon
netet használni és a meglévő scriptekkel (`research/tv_report/`) összegzést adni, hol tart a piac.

## Scope
- `src/ui/` (chat komponens + runner), új read-only SQL helper, skill/persona prompt.
- Minta: `d:\repos\life_plan\backend\chat_runner.py` (claude -p, stream-json, --resume, API-kulcs env kiszűrve).

## Tasks
- t873: chat_runner modul (ui_agent)
- t874: Streamlit chat komponens (ui_agent)
- t875: read-only DuckDB lekérdező helper (database_agent)
- t876: tv_market_sentiment skill + persona prompt (code_doc_agent)
- t877: validálás (validator_agent)

## Key Decisions (2026-11-06, user)
- Jogosultság: csak Read, WebSearch, WebFetch, Bash (szűk allowlist: `research/tv_report/*` scriptek + read-only SQL helper). Edit/Write TILOS. Szabad shell nincs.
- cwd: repo gyökér.
- Nincs fülönkénti persona: az UI egyetlen oldal, egyetlen chat-ablak, egyetlen session.
- ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN env-et a gyerekfolyamatból ki kell szűrni.
- Hosszú tv_report futás (percek): timeout konfigurálható + tool-események (haladás) megjelenítése.
