---
epic: epic_068
id: t884
title: Report-formátum + builder + /api/reports + Elemzések fül
assignee: analyst_agent
status: done
blocks: [t885]
blocked_by: [t879, t880]
---

## Goal
Quarto/papermill kiváltása: Python report-builder (`analyst/report_builder`) ír report.md + chart JSON + tables JSON-t. Az API manifestet ad, a tartalom lazy (kattintásra) töltődik. A frontend Elemzések fül scrollozható markdown nézet natív chartokkal. A 4 sablon (sampling, feature_engineering, hyperparameter_search, strategy) átírása builderré.

## Scope
`analyst/**`, `src/modeling/pipeline.py` analyze lépés, `src/api/routers/reports.py`, `frontend/src/pages/ReportsPage.tsx`.

## Acceptance Criteria
- [x] `pipeline --step analyze` az új formátumot állítja elő
- [x] `GET /api/reports` csak manifest, `GET /api/reports/{id}` kattintásra
- [x] Egy modell mind a 4 riportja megjelenik a UI-n, scrollozható
- [x] Notebook/HTML/Quarto kimenet nem keletkezik többé

## Notes

### Elkészült (analyst_agent)
- **Hely: `src/reports/`** (blueprint §1/§3.4, NEM `analyst/report_builder`): `schema.py` (pydantic Manifest/ChartRef/TableRef), `builder.py` (`ReportBuilder`: add_markdown/add_chart/add_table, write() atomikus tmp-mappa + rename, manifest utoljára, determinisztikus, idempotens; Plotly figure -> sima lista (bdata dekódolva), template törölve, NaN->null, trace max 5000 pont / gyertya 2000 / tábla 2000 sor), `store.py` (list/read, owner/slug/id regex + resolve() védelem, manifestben szereplő id-k), `theme.py`, `builders/{sampling,feature_engineering,hyperparameter_search,strategy,feature_groups,_common}.py`.
- **pipeline.py**: `step_analyze` a 4 buildert hívja (`artifacts/<id>/reports/<slug>/`), `step_feature_engineering` a `feature_engineering.build(write_selection=True)`-t (feature_set.json + 02 riport). papermill/nbformat/quarto/subprocess import eltávolítva. `provenance.ARTIFACT_KINDS` +`fe_feature_set`, `fe_report` (a régi `fe_notebook/fe_html` maradt).
- **API**: `routers/reports.py` + `services/reports.py`; `main.py`: `OPTIONAL_ROUTERS = ("jobs", "reports")`. Végpontok a blueprint §2.11 szerint (`/api/reports/{owner}/{slug}[/charts|/tables/{id}]`, ETag + max-age=60, 304).
- **Frontend**: `pages/ReportsPage.tsx` (szűrő modell/Általános, lista, olvasó; URL query: owner/slug/filter), `components/reports/{ReportMarkdown,ReportChart,ReportTable,useInView}.tsx`, `api/reports.ts`, `types/reports.ts`. Chart/tábla IntersectionObserver-rel lazy; Plotly külön chunk (4.7 MB, gzip 1.4 MB); sötét téma-merge + a #343a40 ACCENT világosra cserélve.

### Döntések / eltérések
1. **optbinning nincs a projekt függőségei között** (a notebook ad hoc telepítésre épült, az env-ben nincs). Helyette `feature_engineering.bin_feature`: egyváltozós regressziós fa (<=10 levél, >=5% sor/levél). A szelekció MI+korreláció alapú, ezért a feature_set.json logikája változatlan; az **IV definíciója eltér** (`sum |mean_i-mean_total|*n_i/n`), így a számértékek nem összevethetők a régi HTML-lel. A régi `feat_` binning-ábra (optbinning plot) helyett Plotly oszlop (megfigyelés %) + vonal (bin átlag target).
2. A tabset/PNG-ek (hónap/hét/nap fülek) helyett: 01 -> szegmens x év lift-ábrák; 04 -> heti ábrák (csak trades-es hetek) + új equity ábra. Info-tartalom egyezik, a napi bontás kimaradt.
3. **02 analyze**: újraszámol, de `feature_set.json`-t NEM írja felül (csak az FE lépés ír). Az újraszámolás ~8 perc (mutual_info_regression).
4. Az 1. táblázati/szöveges magyarázatok adatvezéreltek (a sablon hardcode-olt short számai helyett).
5. A user által említett champion `..._2101_2605` nincs meg; a `lgbm_solusdt_l_fw60_2101_2607` (és s) artifactokon futott. A riport mappák **nincsenek gitignore-olva** (blueprint: követett, mint az `analysis/`), de nem commitoltam; `git status`-ban untracked.
6. 04_strategy builderhez nincs egységteszt (valós stratégia-session kell hozzá), valós futással ellenőrizve.

### Mi marad (t890 / t885)
- `analyst/` teljes (notebooks, quarto, doc_renderer, lib), `src/modeling/01_feature_engineering.ipynb|.html` (a pipeline már nem használja -> törölhető), `artifacts/<id>/{0x_*.html, analysis/, feature_engineering/01_feature_engineering.*}`, `papermill` függőség (pyproject), `.agent/tools/quarto_analysis_defaults.md`, `_doc_/database_and_code_doc/8200_analyst_templates.md`.
- `GET /api/registry/models/{id}` `reports` mezője (blueprint §2.7) nincs bekötve; MCP `list_reports/read_report` (t883) a `reports.store`-ra épülhet.

### Futtatott parancsok
- `uv run ruff check src/reports src/api src/modeling/pipeline.py` ; `uv run pyright src/reports src/api/routers/reports.py src/api/services/reports.py src/modeling/pipeline.py` -> 0 hiba
- `uv run pytest src/reports src/api -q` -> zöld (reports 29 + api reports 11)
- `npx tsc --noEmit` -> tiszta; `npx vite build --outDir %TEMP%` -> OK
- `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step analyze` (lásd lent), majd uvicorn :8612 + curl `/api/reports`, `/{owner}/{slug}`, chart/tábla, path traversal -> 200/404 a vártak szerint.

### Valós futás (champion l, `lgbm_solusdt_l_fw60_2101_2607`)
`pipeline.py --step analyze` -> 4 riport (01: 11 ábra/4 tábla, 02: 149/21, 03: 4/3, 04: 28 ábra/2 tábla); az API listázza, chart/tábla 200. Az analyze alatt a live DB egy sync miatt zárolt volt, ezért a 04 riport első generálásakor a heti ábrák kimaradtak (a builder jelzi a riportban); újrafuttatva a `strategy.build`-del (27 heti ábra) pótolva. Ez a viselkedés (graceful skip) szándékos. A régi `analysis/*.ipynb` és `*.html` érintetlen (t890).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
/api/reports 9 riportot listáz (1 general + 2x4 modell); general/target_yearly_regime riport + chart + tábla 200, path traversal 404. Elemzések oldal screenshot rendben (lista, szűrő, olvasó placeholder), konzolhiba nincs. Az 'reports' mező a model_detail-ban most be van kötve (lásd t880).
