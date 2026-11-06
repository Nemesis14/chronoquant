---
epic: epic_068
id: t899
title: Methodology doc javítások a kód-változások után (gap_penalty, törölt útvonalak, Streamlit)
assignee: methodology_agent
status: done
blocks: [t892]
blocked_by: []
---

## Goal
A `_doc_/methodology_doc/` zóna tényszerű javítása a törölt kódok és útvonalak
után. A módszertani tartalom nem változott; csak az elavult állítások és
hivatkozások lettek igazítva. A zóna kód-mentes marad.

## Scope
- `_doc_/methodology_doc/5000_hyper_param_search.md`
- `_doc_/methodology_doc/6500_cluster_regime_methodology.md`
- `_doc_/methodology_doc/0000_overview.md`
- `_doc_/methodology_doc/3000_sampling.md` (ellenőrizve, nem változott)
- `_doc_/methodology_doc/7000_validacios_elvek.md` (ellenőrizve, nem változott)

## Acceptance Criteria
- [x] `5000_hyper_param_search.md`: a gap_penalty szakasz "megszüntetve" állapotot mutat (egy bekezdés), a paraméter-táblában és a checklistben nincs aktív gap_penalty állítás
- [x] `6500_cluster_regime_methodology.md`: a törölt `research/` útvonalak szöveges hivatkozásra cserélve (`_doc_/research/cluster_regime.md`)
- [x] `rg` a methodology_doc alatt a törölt elemekre (quarto, papermill, streamlit, analyst/, research/, experiments/, walk_forward, yearly_random_hour, src/trading/live/service) nem ad aktív hivatkozást
- [x] `3000_sampling.md`: a sampling leírás a train_valid split-et mutatja aktívnak; a walk-forward kivezetett (változatlan)
- [x] Nincs módosítás más zónában (`_doc_/database_and_code_doc`, `_doc_/0000_*`, `.agent`, `CLAUDE.md`)

## Notes
Megszakadt munka utáni ellenőrzés: a `_doc_/methodology_doc/` git diffje üres volt, tehát az öt pont az elejétől készült el.

Elvégzett javítások:
1. `5000_hyper_param_search.md`
   - "Gap penalty — aktív állapot" szakasz átírva "Gap penalty — megszüntetve"-re, egy bekezdésben: korábban használt, a keresésből kikerült, a régi gp-kísérletek nem reprodukálhatók.
   - Paraméter-tábla `gap_penalty` sora: "Megszüntetve".
   - Kockázati tábla: a "gap penalty bekapcsolása tévesen" sor törölve.
   - Validációs checklist: a `gap_penalty = 0.0` pont törölve.
   - Alternatívák tábla: gap penalty státusza "Megszüntetve" (korábban "Kutatási opció").
2. `6500_cluster_regime_methodology.md` (10. Referenciák)
   - `research/cluster_regime/data/*` és `artifacts/research_cluster_regime/*` útvonalak törölve, helyettük hivatkozás a `_doc_/research/cluster_regime.md` oldalra. Az `artifacts/` útvonalak a korábbi törlés (e3ffcac) miatt nem léteznek.
3. `0000_overview.md`
   - Live Trading ábra: "Streamlit dashboard + Binance" → "Dashboard UI + Binance" (a `src/ui/` már nincs, az epic_068 Streamlit-et vált FastAPI BFF + React UI-ra).

Ellenőrzött, változatlan:
- `3000_sampling.md`: walk-forward már "Kivezetett", a sampling_mode leírás nem említ aktív walk_forward / yearly_random_hour módot.
- `quarto`, `papermill`, `analyst/`, `experiments/`, `src/trading/live/service`: nincs találat a methodology_doc alatt.
- `_doc_/research/cluster_regime.md`, `_doc_/research/strategy_cost_impact.md` és `_doc_/_plans_/plan.md` léteznek, a rájuk mutató hivatkozások érvényesek.

Maradt nyitott pont: nincs.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
