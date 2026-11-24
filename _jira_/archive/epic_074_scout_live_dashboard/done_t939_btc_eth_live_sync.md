---
epic: epic_074
id: t939
title: BTC/ETH élő szinkron bekapcsolása (OHLCV, features, target, funding)
assignee: database_agent
status: done
blocks: [t941, t944]
blocked_by: []
---

## Goal
A `btcusdt` és `ethusdt` élő DB-k most 2026-11-21-nél megállnak, mert nincsenek a `config/trading.json` asset-blokkjai között (auto-sync szabály: konfigurált asset ÉS live DB ÉS trading.json blokk). A BCH (epic_069) mintájára kapcsold be az auto-syncet: OHLCV gap-fill máig, `feat_ohlcv_quant` (fw60 profil), `target`, funding_rate. Predikció még nincs (modell az epic_075-ben készül) → a trading.json blokk/sync úgy legyen beállítva, hogy modell nélkül is hibamentesen fusson (prediction lépés kihagyva vagy no-op). Szabályok: a worker-t ne állítsd le; az API csak read-only DuckDB; minden írás worker job. Holdout-2 (≥ 2026-08-01) adat nem kerülhet modell-illesztésbe.

## Scope
`config/trading.json`, `config/assets.json` (ha kell), `src/data_handling/`, worker sync lánc (csak ha a modell-hiány kezelése igényli — egyeztess a doksiban)

## Acceptance Criteria
- [x] BTC/ETH live DB-k naprakészek, az auto-sync ütemezve fut, hézagmentes 1m OHLCV
- [x] Modell hiánya nem okoz job-hibát
- [x] Doksi: 0002, 1200, 8310 érintett részei

## Notes

**2026-11-23 (database_agent) — kész, validálásra vár.**

### Mit találtam
- A BCH auto-sync valójában csak OHLCV → features → predictions (worker `sync_full`); a `target`-et és a `funding_rate`-et a worker egyik assetnél sem frissíti. A `target` teljes rebuild (`02_sync_pipeline --tables targets`), a funding eddig ad-hoc volt (nem volt `sync_funding` modul).
- BTC/ETH DB-ben csak `ohlcv` volt (2026-11-21 10:28-ig); a `feat_ohlcv_quant` nem létezett, a `target`/`predictions` üres volt.
- Modell nélkül a `sync_predictions` már eddig is no-op volt (`champion_models_for_asset` → `ValueError` → warning + return), job-hibát nem okoz.

### Változtatások
- `config/trading.json`: `"btcusdt": {}`, `"ethusdt": {}`. Üres blokk = sync-aktív, session id-k `None`-ok (az API ezt „nincs aktív session”-ként kezeli). Ha az epic_075 modell kész, ide kerülnek a `strategy_session_*_id`-k.
- **Új:** `src/data_handling/sync_tables/sync_funding.py`: Binance `fapi/v1/fundingRate`, inkrementális (a `MAX(funding_ts)` után), idempotens. `only_if_due=True` (worker mód): csak esedékes elszámoláskor hív hálózatot; az intervallum az utolsó két elszámolás különbsége (min. 1 h), nincs fix 8 órás rács.
- `duckdb_store.py`: v7-es migráció `create_funding_rate` (`IF NOT EXISTS`, így a régi SOL/BCH tábla változatlan) + `insert_funding_rate` (csak a közös oszlopokat írja, dedup `funding_ts`-re).
- `02_sync_pipeline.py`: új `funding` tábla-lépés (az `ALL_TABLES` része).
- **Worker (ui_agent domain, minimális):** `src/worker/handlers/sync_full.py`: az OHLCV után `sync_funding(asset_id, only_if_due=True)` hívás try/except-ben (hiba esetén warning, a job nem bukik). ⚠️ A futó worker ezt csak a **következő újraindítás után** tölti be (nem állítottam le). Addig a funding a CLI-vel frissíthető. A BTC/ETH sync a trading.json-ből újraindítás nélkül bekapcsolt.

### Bootstrap (CLI, mielőtt a trading.json blokk bekerült; a `feat_ohlcv_quant` append-only, ezért ez a sorrend kötelező)
`02_sync_pipeline.py --asset-id <a>`: `--tables ohlcv --start "2026-11-21 10:29:00"` → `--tables targets,features --chunk-months 3` (~11 perc/asset) → `--tables funding`.

### Ellenőrzés (2026-11-23 ~08:07, read-only)
| | ohlcv | feat_ohlcv_quant | 1m gap | target | funding_rate | predictions |
|---|---|---|---|---|---|---|
| BTC | 3 099 306 (2021-01-01 → 2026-11-23 07:05) | 3 099 306, 208 feat_* | 0 / 0 | 2026-11-23 06:42-ig | 6457 (→ 2026-11-23 00:00) | 0 |
| ETH | 3 099 306 (azonos) | 3 099 306, 208 feat_* | 0 / 0 | 06:42-ig | 6457 | 0 |

- Worker auto jobok: BTC `20261123_065809_ce6b0a` és ETH `20261123_070619_5c3b85` **succeeded**; a log szerint a predikciós lépésben csak „Champion model feloldas sikertelen … 'btcusdt'” warning volt.
- A `target` a percenkénti láncban (SOL/BCH-nál is) nem frissül. Tanítás előtt (epic_075) futtasd újra: `--tables targets`. Holdout-2 (≥ 2026-08-01) szűrése a modell-illesztésnél az epic_075 felelőssége, itt nem volt illesztés.

### Tesztek
- Új: `src/data_handling/tests/sync_tables/smoke/test_sync_funding.py` (5), `src/worker/tests/test_sync_full_funding.py` (2). Frissítve: `test_sync_pipeline_helpers.py` (`funding` az `ALL_TABLES`-ben).
- `uv run pytest src/data_handling src/worker`: 261 passed, 3 failed. Mindhárom bukás `store/sanity` teszt, amely a **default (SOL)** live DB-t olvassa: `test_feat_ohlcv_quant_no_time_gap` (28 gap), `test_target_row_count_matches_features`, `test_predictions_score_range`. A SOL DB-hez nem nyúltam, ezek meglévő SOL-adatproblémák (külön ticketet érdemelnek). A futás közbeni lock-ERROR-ok átmenetiek voltak (a worker és a bootstrap egyszerre futott).
- `ruff check` + `pyright` a módosított fájlokon: tiszta.

### Doksi
0002 (funding sor + élő assetek), 1200 (`sync_funding` szekció, élő lánc, idempotencia), 8310 (`sync_full` funding lépés, data-only asset, aktív assetek). Egysoros javítás a 0003, 0004 és 0000_project_overview fájlokban („nincs `sync_funding` modul” → már van).

---

**2026-11-23 (validator_agent) — VALIDÁLVA → done.**

- **Statikus:** `ruff check` + `pyright` (sync_funding, duckdb_store, 02_sync_pipeline, worker sync_full, utils, új tesztek): 0 hiba.
- **Tesztek:** `uv run pytest src/data_handling src/worker -q` → 261 passed, 3 failed. A 3 bukás (`test_feat_ohlcv_quant_no_time_gap`, `test_target_row_count_matches_features`, `test_predictions_score_range`) a default **SOL** live DB-t olvassa (store/conftest `load_asset_config()`), amelyet a t939 nem érintett (SOL még migráció v6). Meglévő, a t939-től független hibák → külön ticket javasolt.
- **Élő DB-k (read-only, 07:1x):** BTC/ETH `ohlcv` és `feat_ohlcv_quant` 3 099 31x sor, **0 gap, 0 duplikátum**, utolsó bar 07:09/07:10; `funding_rate` 6457 sor (0 dup, → 2026-11-23 00:00, `mark_price`-szal); `_schema_migrations` v7. SOL/BCH v6, régi 2-oszlopos `funding_rate` érintetlen (a v7 `IF NOT EXISTS`; az `insert_funding_rate` a közös oszlopokra ír).
- **Worker jobok (jobs.sqlite):** BTC 13 / ETH 7 sikeres `sync_full`; a 2 BTC-bukás `ReadTimeout api.binance.com` (hálózati, SOL/BCH-nál is tömegesen ma reggel), nem t939-hiba; a következő tick hézagmentesen pótolt. Job log: modell nélkül csak „Champion model feloldas sikertelen” warning, a job `finished`.
- **API (:8600, read-only GET, `?asset=btcusdt|ethusdt`):** status, registry/active, strat/active, strat/sessions, equity, equity/summary, predictions/history|signals, market/* → 200, `null` session id-k / üres listák; `predictions/latest` → 404 `no_data` (tervezett). Frontend típusok `string | null`, `?? DASH` fallback.
- **Kód-megjegyzés (nem blokkoló):** `sync_funding._client()` a Binance kulcsfájlt olvassa, pedig a `fundingRate` publikus endpoint (kulcs nem kerül kódba/logba); az `asset_id` paramétere nem használt. A worker funding-lépése csak a következő worker-újraindítás után aktív (SOL funding ekkor 2026-07-31-től pótlódik).
- **Doksi-javítás (validator):** `0000_project_overview.md` asset-szekció (BTC/ETH sync-aktív, data-only, modell: epic_075; üres trading.json blokk szemantikája; a `target`-et a worker nem frissíti → kézi `02_sync_pipeline --tables targets`), Key Conventions „Assets” sor; `0002` könyvtárfa-komment + `target` megjegyzés. 0003/0004/1200/8310 pontos.
