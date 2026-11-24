---
epic: epic_069
id: t902
title: Beégetett solusdt kiirtása + asset-onkénti stratégia-session config
assignee: modeling_agent
status: done
blocks: ["t903", "t906"]
blocked_by: ["t900"]
---

## Goal
A modeling/strategy/deploy kód asset-független lesz, a trading.json asset-onkénti session-kiosztást kap.

## Scope
Beégetések (rg `solusdt`, nem tesztek): `strategy/session_naming.py`, `strategy/costs.py` (SOL-os komment/funding), `00_run_strategy_session.py`, `06_trigger_deploy.py`, `05_create_snapshot.py`, `store/snapshots.py`, `00_create_sample.py`, `02_hyper_param_search.py`, `snapshot_sampler.py`, `03_build_quant_train.py`, `01_validate_stats.py`, `sync_targets.py`, `sync_triple_barrier.py`, `sync_predictions.py`, `reports/builders/*`; `config/trading.json`; `utils.py` default-ok.

## Acceptance Criteria
- [x] Minden CLI/pipeline lépés `--asset-id`/a modell `asset_id`-jából dolgozik; nincs beégetett `solusdt` az `src`-ben (kivéve default_asset fallback és tesztfixture, listázva a Notes-ban)
- [x] `config/trading.json` formátum: asset-onkénti `strategy_session_long_id/short_id` (visszafelé kompatibilis olvasó a `utils`-ban; a SOL értékei változatlanul érvényesek)
- [x] A SOL pipeline/teszt viselkedése nem változik (modeling, strategy, data_handling tesztek zöldek)
- [x] Az új formátum olvasására a t903 `ui_agent` építhet: írd le a Notes-ba a `utils` új API-ját
- [x] ruff, pyright zöld

## Notes

### Elvégzett változtatások
- `utils`: `FALLBACK_ASSET_ID` (az egyetlen kódbeli `"solusdt"` literál, default fallback), `default_asset_id`/`load_asset_config` ezt használja.
- `00_run_strategy_session.py`: asset = `--asset-id` -> a long modell `asset_id`-ja -> default; long/short asset-eltérésre hiba. (Korábban None -> default SOL ment tovább a search/artifacts-ba.)
- `sync_triple_barrier.py`: `--asset-id` default `None` (-> `resolve_asset_id`).
- `reports/builders`: `read_live(..., asset_id=)`; sampling/strategy/feature_engineering modell-asset alapján nyitja a lab/live DB-t (`utils.model_asset_id`), a sampling riport "Asset" sora dinamikus, `feature_set.json` `asset_id` a modell configból (nem a model_id split-ből).
- `config/trading.json`: új `assets.{asset}.strategy_session_{long,short}_id`; a lapos kulcsok TOVÁBB megmaradtak (a futó `api/services/common.py::active_sessions` még azt olvassa) - t903 után törölhetők.
- Teszt: `src/strategy/tests/strategy/smoke/test_trading_sessions.py`.

### utils új API (t903-hoz)
- `utils.strategy_session_ids(asset_id=None) -> {"long": str|None, "short": str|None}`: az `assets[asset]` blokkból; ha nincs és az asset a default, a régi lapos kulcsokból (`strategy_session_long_id/short_id`, `strategy_session_id` fallback); különben `None`-ok.
- `utils.trading_asset_ids() -> list[str]`: a `trading.json["assets"]` kulcsai (lapos formátumnál `[default_asset_id]`).
- `utils.model_asset_id(model_id) -> str`: `models[model_id]["asset_id"]`, különben default.
- Meglévők: `utils.default_asset_id()`, `utils.resolve_asset_id(x)`, `utils.load_trading_config()` (nyers dict).
- BCH-hoz még nincs session a `trading.json`-ban (nincs modell) - a modell elkészülte után kell felvenni az `assets.bchusdt` blokkot.

### Maradék beégetések (indokolt)
- `utils.FALLBACK_ASSET_ID` (default fallback).
- Docstring/usage példák (nem futó kód): `session_naming.py`, `00_run_strategy_session.py`, `02_hyper_param_search.py`, `00_create_sample.py`, `05_create_snapshot.py`, `06_trigger_deploy.py`, `03_build_quant_train.py`, `01_validate_stats.py`, `02_sync_pipeline.py`, `store/snapshots.py`, `snapshot_sampler.py`, `sync_targets.py`, `sync_predictions.py`, `duckdb_store.py`, `reports/schema.py`, `utils.py` (modell-id példák).
- `strategy/costs.py`: tényszerű komment (SOLUSDT funding 2h/4h); a funding schedule asset-paraméteres.
- Kívül scope-on (nem érintve): `sentiment/tv_report/author_backtest.py` (SOLUSD->SOLUSDT ticker map, adatleképezés), `api/chat/persona_prompt.md` (ui_agent), `api/services/common.py` (t903).
- Tesztek: a 3 ismert SOL live-data sanity hiba marad; többi zöld (314 passed + új tesztek), ruff és pyright 0 hiba.
