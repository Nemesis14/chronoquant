---
epic: epic_068
id: t886
title: research/ → rövid md összefoglalók, minden más törlése
assignee: analyst_agent
status: done
blocks: [t890]
blocked_by: [t879]
---

## Goal
Minden research mappához (cluster_regime, ma_cross, rsi_divergence, tradingview_poc, tv_report) rövid .md: mit néztünk, módszer egy bekezdésben, konklúzió, mi lett a döntés. Utána a `research/` teljes tartalma (kód, png, csv, quarto, output) törlődik. Elhelyezés: `_doc_/research/<slug>.md`. A tv_report workflow (`tv_sentiment_skill.md`, persona prompt, query allowlist) hivatkozásait ellenőrizd és igazítsd.

## Scope
`research/**` (törlés, ~270 MB, nagy része gitignore-os, így visszavonhatatlan), `_doc_/research/*.md`.

## Acceptance Criteria
- [x] Mappánként 1 rövid .md (kb. 1 oldal)
- [~] `research/` nem létezik (a 2 tv_report script marad, t896 költözteti)
- [x] A törlendő fájlok listája a Notes-ban (a user a törlést jóváhagyta)

## Notes
[orchestrator] A user döntése (A): a tv_report két működő scriptje és a tradingview collector MARAD (a t896 költözteti `src/sentiment/`-be), tehát a `research/tv_report/` scriptjeihez NE nyúlj, csak az outputjait (csv, png, html, riportok) töröld, ha vannak. Minden más research mappa és a `src/modeling/experiments/` kísérletei (sol_8h_lgbm_gate, champion_streak_gate, cross_sectional_probe, altdata/analyst_signal és társaik) kapjanak rövid md-t (`_doc_/research/<slug>.md`; a meglévőket frissítsd, ne duplikáld), mielőtt a kód törlődik (t894/t895). A `research/` törlése: minden, ami nem tv_report script. A törlendő lista a Notes-ba, mielőtt töröl. Nem kell megerősítés kérdezni: a user jóváhagyta (ignored outputok is).

### [analyst_agent] Törlési lista (a törlés ELŐTT rögzítve; a user jóváhagyta)
Törlés előtti git-állapot: commit `73f2156`. Csak a tracked fájlok állíthatók vissza onnan; a gitignore-osak (jelölve) nem.
- `research/cluster_regime/` 249 KB, 16 fájl (7 .py + `__init__` tracked; `__pycache__` nem)
- `research/ma_cross/` 260 KB, 9 fájl (**gitignore-os, sosem volt commitban**: README, ema_cross_research.py, sma_mfe_15m.py, out/*.png, summary.csv)
- `research/rsi_divergence/` 13 KB, 2 fájl (**gitignore-os**: .py + out/summary.csv)
- `research/tradingview_poc/` 9,4 MB, 90 fájl (tracked: README, REVIEW, probe*.py, review_report.json, samples/, .gitignore; az `out/` 9,3 MB nyers HTML/JS/JSON gitignore-os)
- `research/tv_report/runs/` 264 MB, 1012 fájl (gitignore-os: nyers API-lapok, OHLCV cache, author_backtest riportok)
- `research/tv_report/` kimenetek 1,5 MB, 52 fájl: `latest.json`, `state.json` (dedup, a script újragenerálja), `*_latest*.json` (7), `scratch_*.txt` (~43)
- `artifacts/research_cluster_regime/` 35 MB, 7 fájl: NEM törölve (t890 dolga), csak elolvasva.
- MARAD (gitignore-os, SOSEM commitolt, nincs git-mentés!): `research/tv_report/fetch_report_data.py`, `research/tv_report/author_backtest.py`
Összesen törölve ~274 MB.

### [analyst_agent] Elvégzve
**Új md (`_doc_/research/`):** `cluster_regime.md`, `ma_cross.md`, `rsi_divergence.md`, `tradingview_poc.md`, `tv_report.md`, `multi_coin_panel_models.md` (az epic_064 ~14 kísérleti fájlja: baseline/woe/single_coin/direction/horizon_sweep/sol_* /multi_coin_walkforward) + `README.md` index táblázat (19 sor). Minden md-ben "Kód a git historyban" szakasz.
**Frissített meglévő md:** 10 fájl (sol_8h_lgbm_gate, champion_streak_gate, cross_sectional_probe, execution_cost_gate, triple_barrier_gate, atr_barrier_gate, altdata_gate, altdata_source_matrix, reddit_attention_gate, tradingview_author_skill) kapott "Kód a git historyban" láblécet (törlés előtti commit `73f2156` + útvonalak). Az `analyst_signal`, `collectors/{reddit,derivatives,universe}`, altdata sync-ek ezekben vannak lefedve (nincs külön md, nem duplikáltam). A `tradingview_author_skill.md` poc-linkje javítva.
**Törölve:** lásd a fenti lista (~274 MB); `research/` alatt csak `tv_report/{fetch_report_data,author_backtest}.py` maradt (t896 költözteti). Az AC "research/ nem létezik" ezért a t896 után teljesül.
**Hivatkozás-javítás:** `_doc_/models_doc/6502_*` (Output Files) és `6500_*_v2` (Code sor) törölt-jelzés + md-link.
**Jelezve, nem javítva (más hatáskör):**
- `_doc_/methodology_doc/6500_cluster_regime_methodology.md:448-449` a törölt `research/cluster_regime/data/*.parquet`-re hivatkozik (methodology_agent / t891).
- `_doc_/0000_project_overview.md` (sor 27, 66-68, 173, 437) `research/elliott/`-ot említ, ami nem létezik; a `research/` szakasz t891-gyel igazítandó.
- `CLAUDE.md` delegation tábla `research/` -> analyst_agent sora, és `_doc_/research/` zóna: t891.
- `_doc_/_plans_/plan.md`, `068_bff_ui.md` történeti/terv szöveg, nem módosítva.
- tv_report hivatkozások (`src/utils.py:385`, `src/ui/chat/persona_prompt.md`, `src/ui/tests/chat/smoke/test_runner.py`, `src/api/chat/persona_prompt.md`, `.agent/skills/tv_sentiment_skill.md`) érintetlenek, a t896/t883/t891 dolga; a skill `PLAN_tv_market_sentiment.md`-re és a törölt `runs/author_backtest/` kimenetekre is hivatkozik.
- **Figyelem t896-nak:** a két tv_report script git-ignorált, sosem volt commitban; a költöztetésnél `git mv` nem működik, sima másolás kell, és a lemezen lévő példány az egyetlen.
**Gitignore (t890):** a `.gitignore` 59-63. sora ("Research outputs", `research/tv_report/`, `research/ma_cross/`, `research/rsi_divergence/`) feleslegessé válik: a ma_cross/rsi_divergence sorok azonnal; a tv_report sor a script t896-os költözése után (az új `src/sentiment/` kimenet-útvonalat kell ignorálni, pl. `data/tv_report/`). A `research/tradingview_poc/.gitignore` a mappával törölve.
**AC:** [x] mappánként md  [~] `research/` nem létezik (csak a 2 megtartott script, a user pontosítása szerint)  [x] törlési lista a Notes-ban.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
