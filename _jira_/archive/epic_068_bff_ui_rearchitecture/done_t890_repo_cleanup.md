---
epic: epic_068
id: t890
title: Repo takarítás: gyökér, docs, tools, analyst, artifacts, függőségek
assignee: code_doc_agent
status: done
blocks: [t891]
blocked_by: [t885, t886, t887, t888, t889, t894, t895, t896, t897]
---

## Goal
Törlés a t879 listája szerint: `_chronoquant_docs_files/`, `_chronoquant_docs.ipynb`, `PLAN_tv_market_sentiment.md`, `control.ipynb`, `trading_reports/`, `docs/`, `tools/` (használat ellenőrzése), `analyst/` Quarto részek, artifacts alatt .ipynb/.html/_experimental, `__pycache__`. Függőségek eltávolítása: streamlit, papermill, jupyter, nbconvert, quarto. Gyökérben csak: CLAUDE.md, pyproject, uv.lock, pyrightconfig, mappák.

## Scope
repo gyökér, `pyproject.toml`, `.gitignore`

## Acceptance Criteria
- [x] Gyökér rendezett
- [x] Nincs Streamlit/Quarto/papermill hivatkozás a kódban és a függőségekben (doksi-szöveg: t891)
- [~] `uv lock`+`uv sync --dry-run` OK, tesztek zöldek; a valódi `uv sync` a zárolt .venv (futó Streamlit) miatt user-teendő

## Notes
### [code_doc_agent] Törlési lista (a törlés ELŐTT rögzítve; a user jóváhagyta). Állapot: commit `73f2156` + a dirty worktree (a korábbi t884-t897 törlések még nincsenek commitolva).
**Előtte (méret):** git index 1093 fájl (ebből a lemezen 1034); `artifacts/` 317 MB (504 tracked fájl); `docs/` 267 KB (46 fájl); `analyst/` 350 KB; `tools/` 15 MB (gitignore-os); `_chronoquant_docs_files/` 564 KB; `_chronoquant_docs.ipynb` 1,9 MB; repo (.git, .venv, node_modules, database, data nélkül) 878 MB; `.git` pack 167 MB.
**rg-ellenőrzés (src/, config/, scripts/, tests, pyproject, .agent, frontend):**
- `analyst/*`: `import analyst`/`from analyst` 0 találat; a `src/reports` csak docstringben említi (`theme.py:1`, `builders/feature_engineering.py:6` -> javítandó szöveg). A pipeline papermill/quarto importja t884-ben kikerült. Törölhető.
- `_chronoquant_docs.ipynb`/`_files`: csak `analyst/doc_renderer` és `control.ipynb` hivatkozza. `control.ipynb`: 0 hivatkozás. Törölhető.
- `PLAN_tv_market_sentiment.md`: 0 kód/config hivatkozás; a tartalom megvalósult (`src/sentiment/tv_report/`); egyedi indoklás (minőség-szűrt vs nyers irány-megoszlás) 6 sorban átkerül `_doc_/research/tv_report.md`-be; utána törlés.
- `docs/` (46 fájl): kódból/configból 0 hivatkozás; `.agent/skills/methodology_doc_skill.md:53` és `.agent/agents/methodology_agent.md:110` említi -> kivéve. Tartalom: 2-8 KB-os, részben Streamlit/live-trading-re elavult csonkok, a `_doc_/` fedi (database_and_code_doc, methodology_doc). EGYEDI és értékes: `docs/concepts/elliott_waves/` (3 fájl, 64 KB, algoritmikus spec; máshol nincs) -> áthelyezve `_doc_/research/elliott_waves/`. A többi törölve (git history: `73f2156`).
- `tools/` (15 MB, gitignore-os): `src/ config/ scripts/` 0 hivatkozás; csak `_doc_/research/reddit_attention_gate.md` szöveges említés. Törlés + `.gitignore` sor ki.
- `trading_reports/`: már nem létezik (t897); `config` `journal.report_dir` kulcs t897-ben kivéve.
- `__pycache__`: 0 tracked, gitignore-os; a gyökérbeli/`analyst/` példányok törölve (lemez).
- `artifacts/` kísérleti könyvtárak: `oos_champ_A/B`, `wf_champ_{1,2,3}_{sel,test}`, `strat_champion_long_netcheck`, `strat_solusdt_fw60_{long,short}_2101_2607` (md5-ellenőrzött DUPLIKÁTUM: a fájlok byte-azonosak a `lgbm_solusdt_{l,s}_fw60_2101_2607/strategy/`-val), `research_cluster_regime` (35 MB, a törölt `research/cluster_regime` kimenete; _doc_/research/cluster_regime.md fedi). rg: `src/ config/ scripts/` 0 hivatkozás mindegyikre. `_experimental` nem létezik.
- Modell-mappákban törölve: `0x_*.html` (4/modell), `analysis/` (4 ipynb), `feature_engineering/01_feature_engineering.{ipynb,html}`. MARAD: `feature_set.json`, `features.json`, `manifest.json`, `model.pkl`, `params.json`, `strategy/`, `search/`, `reports/`.
- Függőségek: `rg` import: `papermill`, `nbformat`, `ipykernel`, `jupyter`, `nbconvert`, `seaborn`, `mplfinance`, `taew`, `matplotlib`, `pyinstaller`: 0 import/hivatkozás a `src/ scripts/`-ben (a matplotlib-ot használó `training/reports.py` t888-ban törölt), nincs `.spec`. Törölhető mind.
- `config/trading.json` `asset_id`: nincs olvasó (`status.py` az asset-configból veszi, `load_trading_config` hívók: db_path, ohlcv_sync_delay_seconds, strategy_session_*). Törölve.
- `duckdb_stats.raw_manifest_audit`: csak a saját tesztfájlja (`test_duckdb_stats_audit.py`, 3 teszt) hívja. Törölve a tesztjével.

### [code_doc_agent] Elvégzve (2026-11-06)
**Törölve:** `analyst/` (teljes), `docs/` (46 fájl; az Elliott-hullám spec -> `_doc_/research/elliott_waves/`, README index sor), `tools/` (15 MB), `_chronoquant_docs_files/`, `_chronoquant_docs.ipynb`, `control.ipynb`, `PLAN_tv_market_sentiment.md` (6 soros kivonat -> `_doc_/research/tv_report.md`), `__pycache__` (gyökér/analyst), `.agent/tools/quarto_analysis_defaults.md`. `trading_reports/` már nem létezett. `artifacts/`: `oos_champ_A/B`, `wf_champ_{1,2,3}_{sel,test}`, `strat_champion_long_netcheck`, `strat_solusdt_fw60_{long,short}_2101_2607`, `research_cluster_regime`, + a 2 champion mappából a `0x_*.html` (8), `analysis/` (8 ipynb), `feature_engineering/01_feature_engineering.{ipynb,html}` (4). MARAD: model.pkl, features.json, params.json, manifest.json, `feature_set.json`, `strategy/`, `search/`, `reports/`. `_experimental` nem létezik.
**Kód/config/doksi:** `duckdb_stats.raw_manifest_audit` + `test_duckdb_stats_audit.py` (3 teszt) törölve, doksi-szekciók (1100, 1130, 1310) kivéve. `config/trading.json` `asset_id` törölve (nincs olvasó). `src/reports/theme.py` + `builders/feature_engineering.py` docstring `analyst/` hivatkozás javítva. `.agent`: `methodology_doc_skill.md`/`methodology_agent.md` `docs/` legacy sor kivéve, `quarto_analysis_defaults.md` hivatkozások kivéve (`analyst_agent.md`, `analyst_skill.md`, `0001_agentic_system.md` mermaid), `analyst_skill.md` ELAVULT jelzést kapott. `.gitignore`: `tools/`, `_chronoquant_docs*`, megszűnt `research/*` sorok ki; `data/tv_report/`, `frontend/node_modules|dist` marad/benne.
**Függőségek (`uv remove`):** `papermill, seaborn, mplfinance, taew, matplotlib, zstandard` (a reddit dump_ingest t894-ben törölt, 0 import) + dev: `jupyter, nbconvert, pyinstaller`. nbformat/ipykernel tranzitív volt (a lockból kiesett). Marad: plotly, tabulate, python-binance, fastapi, uvicorn, mcp, optuna, lightgbm. Lock: 171 -> 89 csomag; `uv lock --check` OK, `uv sync --dry-run` OK.
**Registry** (`database/_registry/registry.duckdb`, backup: `registry.duckdb.bak_068`; nincs FK a sémában; egy tranzakció): `reg.artifacts` 455 -> 22 (433 törölve = 51 most törölt fájl + 382 korábban is dangling sor: strat_* kinds, fe_html 9 + fe_notebook 9; a `pred_table` sorok DuckDB-táblák, maradtak, 8 db; a 2 champion 14 sora maradt). `reg.strategies` 38 -> 2 (36 kísérleti/régi `oos*`, `wf*`, `strat_*` törölve; MARAD `strat_solusdt_fw60_combo_2101_2605` [config/strategies.json + archivált deployment] és `strat_solusdt_fw60_long_2101_2607` [az AKTÍV deployment]). models/search_runs/feature_sets/snapshots/deployments érintetlen (9/10/15/5/2). `registry_validator` zöld.
**Méret:** git index 1093 -> 971 fájl (lemezen 1034 -> 912); `artifacts/` 317 MB -> 33 MB (tracked 504 -> 434 fájl); repo (.git/.venv/node_modules/database/data nélkül) 878 MB -> 576 MB (a `tools/` 15 MB, `research/` ~274 MB a t886-ban már előbb ment); gyökér most: CLAUDE.md, pyproject.toml, uv.lock, pyrightconfig.json, .gitignore, .gitattributes + `_doc_ _jira_ .agent .claude artifacts config data database frontend logs scripts src`. Egyéb gyökér-elemek, gitignore-os/lokális: `.venv`, `.git`, `.mcp.json` (gitignore), `.pytest_cache`, `.ruff_cache` (gitignore), `.vscode` (editor-beállítás, tracked-e: nem vizsgálva, maradt).
**Ellenőrzés:** `ruff check src` tiszta; `pyright src` 0 hiba; `pytest src -m "not perf"` (deselect: no_time_gap, target_row_count, predictions_score_range, test_features_target = ismert élő-DB sanity) 202 + 165 passed; `npm run build` OK; `registry_validator` OK.
**Nyitott / user-teendők:**
1. A régi Streamlit szerver (:8501) még fut és zárolja a `.venv`-et: `uv remove` közben a matplotlib/kiwisolver/PIL uninstall `Access is denied` -> a venv félig tisztított (a törölt csomagok maradékai bent; a használt csomagok működnek). Állítsd le a szervert, majd `uv sync` (43 csomag eltávolítása vár). Törölhetők még: `logs/ui.*.log`, `streamlit_dashboard.log`.
2. DB-tábla/fájl döntések (nem nyúltam): `database/_universe/` (11 GB), `altdata_5m`/`feat_altdata`, `analyst_signal` táblák, lab `strat.*` (lásd t894 Notes).
3. t891: `analyst_skill.md`/`analyst_agent.md`/`docs_skill.md` Quarto-szövegei, CLAUDE.md delegációs tábla (`research/`, Quarto), `_doc_/0000_project_overview.md`; `_doc_/database_and_code_doc/8200_analyst_templates.md` (törölt analyst/-ra hivatkozik).
4. Nem commitolt: minden változás a worktree-ben/indexben (nincs commit).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
rg-ellenőrzés: src/, scripts/, config/, pyproject.toml, frontend/src: nincs élő hivatkozás src/ui, streamlit, quarto, papermill, research/, analyst/ elemre (csak történeti komment/negatív teszt/doksi). Két elavult szöveg javítva: trading/live/journal.py docstring, sync_triple_barrier.py hibaüzenet ('Streamlit'). Gyökérben/repo-ban elveszett fájl nincs (git status: csak szándékos untracked: új modulok, doksik, riportok); a repóban kívüli (gitignore) maradék: logs/ui.*.log, logs/streamlit_dashboard.log, database/_registry/registry.duckdb.bak_068 — user-teendő. A :8501 régi Streamlit szerver (PID 11636) még fut.
