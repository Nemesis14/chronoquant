---
epic: epic_069
id: t900
title: assets.json: bchusdt, btcusdt csonk törlése, bchusdt_fw60 feature profile
assignee: database_agent
status: done
blocks: ["t901", "t902"]
blocked_by: []
---

## Goal
Új asset a configban és a törlendő csonk eltávolítása.

## Scope
`config/assets.json`, `config/features.json` (profiles), `database/btcusdt/` (törlés, a user jóváhagyta), hivatkozások rg-vel.

## Acceptance Criteria
- [x] `assets.json`: `bchusdt` (symbol BCHUSDT, 1m, futures, db_path `database/bchusdt/bchusdt.duckdb`, metadata_path, features_profile `bchusdt_fw60`, qty_step a Binance exchangeInfo szerint); `default_asset_id` marad `solusdt`
- [x] `features.json` profiles: `bchusdt_fw60` a `solusdt_fw60` másolata (a FE lépés úgyis újraválogat)
- [x] `btcusdt` config-bejegyzés és `database/btcusdt/` törölve (rg bizonyíték: nincs hivatkozás)
- [x] `utils.load_asset_config('bchusdt')` működik; ruff, pyright zöld (érintett fájlok); a `data_handling` tesztek 202 zöld, 3 meglévő, nem ehhez kapcsolódó adat-check bukik (lásd Notes) — a solusdt store-on futnak

## Notes

**database_agent, 2026-11-07**

- `config/assets.json`: `btcusdt` törölve, `bchusdt` felvéve: symbol `BCHUSDT`, interval `1m`, market `futures`, db_path `database/bchusdt/bchusdt.duckdb`, metadata_path `database/bchusdt/bchusdt.json`, features_profile `bchusdt_fw60`, `qty_step: 0.001`. `default_asset_id` = `solusdt` változatlan; a solusdt bejegyzés diff-ben érintetlen.
- `qty_step` forrása: `https://fapi.binance.com/fapi/v1/exchangeInfo`, BCHUSDT status TRADING, contractType PERPETUAL, LOT_SIZE stepSize `0.001` (minQty 0.001, maxQty 20000). MARKET_LOT_SIZE stepSize is 0.001.
- `config/features.json`: új profil `bchusdt_fw60` = `solusdt_fw60` másolata (csak a description más); az indikátor-halmaz összehasonlítva azonos. CRLF sorvégek megmaradtak.
- `database/btcusdt/` törölve (user jóváhagyás). A törlést a rendszer nem tiltotta meg. Az adat gitignore-olt (`/database/`), git-ből nem állítható vissza.
- `rg -i btcusdt` (kivéve `_jira_/archive/`): élő kódhivatkozás nincs. Egyetlen teszt-fixture igazítva: `src/strategy/tests/strategy/smoke/test_session_naming.py` (mismatched pair példa `btcusdt` -> `bchusdt`, 3 hely). Szándékosan NEM módosítva, mert történeti/külső hivatkozás: `_doc_/research/*` (kutatási összefoglaló, az akkori btc store-ra hivatkozik), `_jira_/epic_053_*` done/cancelled ticketek, `_jira_/epic_064_*/pr_t854`, `src/data_handling/collectors/tradingview/*` és `src/sentiment/tv_report/author_backtest.py` (`BTCUSDT` mint TradingView külső szimbólum, nem config-asset).
- `qty_step` jelenleg nincs kódbeli olvasója. A `utils.load_asset_config` normalizált dict-je nem adja vissza (None). Nyitott kérdés: ha t901/t902 használja, a loader-be kell felvenni.
- Tesztek:
  - `uv run pytest src/strategy/tests/strategy/smoke/test_session_naming.py`: 4 passed
  - `uv run pytest src/data_handling/tests -q`: 202 passed, 3 failed. A bukók: `store/sanity/test_features_target.py::test_feat_ohlcv_quant_no_time_gap` (16 time gap a `feat_ohlcv_quant`-ban), `::test_target_row_count_matches_features` (3 232 447 vs 3 120 401), `test_predictions.py::test_predictions_score_range` (3 075 907 score [0,1]-en kívül). Mindhárom a default (solusdt) live store-t ellenőrzi; a solusdt config és kód nem változott, ezért az adat-állapot a bukás oka, nem ez a ticket. Nem javítottam (adat-probléma, nem config).
  - `ruff check` (test fájl): All checks passed. `pyright` (test fájl): 0 errors.

[validator] Done — 2026-11-07. ruff/pyright/pytest zöld (lásd t909 Notes).
