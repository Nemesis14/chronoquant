# Epic 043: Code & Doc Review Cleanup

## Goal
Review-alapú cleanup: halott kód eltávolítása, elavult dokumentáció javítása, kísérleti artifact-ok rendbetétele.

## Scope
- `src/trading/live/journal.py` + `src/ui/components/trade_panel.py` — COOLDOWN dead code
- `src/strategy/01_rebuild_long_d10_strategy.py` — dead strategy script
- `artifacts/strat_solusdt_fw60_joint_2101_2605/` — kísérleti strategy artifact
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/search_joint*/`, `search_top10/`, `comparison/` — kísérleti search variánsok
- `_doc_/database_and_code_doc/7120_trading_service.md`, `7150_trading_state_strategy.md`, `5510_training.md`, `6120_calibrate.md` — elavult code docs
- `_doc_/methodology_doc/6200_strategy_optimization.md`, `4000_quant_train.md`, `6300_strategy_grid_search.md`, `5500_hyper_param_search.md` — elavult methodology docs
- Új docs: pipeline orchestrátor, short score szemantika, intrabar TP/SL

## Tasks
- t1: COOLDOWN dead code eltávolítás (ui_agent)
- t2: Code-ref docs javítás: 7120, 7150, 5510, 6120 + pipeline doc (code_doc_agent)
- t3: Methodology docs javítás: 6200, 4000, 6300, 5500 + új: short score, intrabar TP/SL (methodology_agent)
- t4: Dead file törlés + artifact reorganizáció (orchestrator, direkt)

## Key Decisions
- COOLDOWN state epic_036-ban kivezetésre került; minden maradék referencia törlendő
- `01_rebuild_long_d10_strategy.py` research-be sem kerül — nincs mentő értéke
- Kísérleti search variánsok `_experimental/` almappába kerülnek (nem törlés — historikus érték)
- `strat_solusdt_fw60_joint_2101_2605/` törölhető (combined session kivezetett, dual-session váltotta fel)
