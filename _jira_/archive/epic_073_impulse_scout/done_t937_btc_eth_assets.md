---
epic: epic_073
id: t937
title: BTCUSDT + ETHUSDT asset felvétele (DB, 1m OHLCV backfill)
assignee: database_agent
status: done
blocks: [t930]
blocked_by: []
---

## Goal
A SOL (és a meglévő `bchusdt`) mintájára vedd fel a `btcusdt` és `ethusdt` assetet: `config/assets.json` bejegyzés,
`database/<asset>/<asset>.duckdb` + metadata, 1m futures OHLCV backfill 2021-01-01-től (a SOL-lal azonos oszlopkészlet,
beleértve a taker buy volument és a kötésszámot, ha a SOL tábla tartalmazza), és azok az alap sync lépések, amelyek a
kutatási eseményadatkészlethez kellenek (legalább OHLCV). Éles trading, modell vagy worker-integráció nem kell.
A `default_asset_id` nem változik.

## Acceptance Criteria
- [ ] Mindkét DB hézagmentes 1m OHLCV-vel (hiányzó percek jelentve)
- [ ] Ugyanazzal a loaderrel olvasható, mint a SOL
- [ ] Doksi: érintett `_doc_/database_and_code_doc/` oldalak

## Notes

### database_agent — végrehajtás (2026-11-21)
- `config/assets.json`: `btcusdt`, `ethusdt` (futures, 1m, `qty_step` 0.001, `features_profile` `<a>_fw60`); `default_asset_id` marad `solusdt`.
- `config/features.json`: `btcusdt_fw60`, `ethusdt_fw60` profil (a `bchusdt_fw60` másolata, csak hogy az asset-config feloldható legyen; features nem épült).
- Backfill: `02_sync_pipeline.py --asset-id <a> --tables ohlcv --start "2021-01-01 00:00:00"` (Binance futures REST, publikus adat), párhuzamosan a két assetre, ~20 perc. Csak OHLCV; targets/features/levels nem futott; nincs modell/trading.json/worker integráció (az asset nem sync-aktív).
- Oszlopok azonosak a SOL-lal: open_time, open, high, low, close, volume, quote_volume, trades, taker_buy_base, taker_buy_quote.
- Ellenőrzés (`sr_detection.load_ohlcv_1m(asset_id)` ugyanazzal a loaderrel olvasható):

| asset | sorok | első open_time | utolsó open_time | hiányzó perc | duplikátum |
|---|---|---|---|---|---|
| btcusdt | 3 096 629 | 2021-01-01 00:00 | 2026-11-21 10:28 | 0 | 0 |
| ethusdt | 3 096 629 | 2021-01-01 00:00 | 2026-11-21 10:28 | 0 | 0 |

- Megjegyzés: a sync log "latest" értéke 10:49, a DB utolsó sora 10:28 (a sorszám egyezik az appended számmal, hézag nincs; a különbséget nem vizsgáltam tovább). A DB-k a sync futás végén frissek; a további frissítés `02_sync_pipeline.py --asset-id <a> --tables ohlcv` (inkrementális).
- Doksi: `_doc_/0000_project_overview.md` (asset tábla), `_doc_/database_and_code_doc/0002_data_architecture.md` (database/ fa).

### validator — PASS (2026-11-21)
- DB: btc/eth 3 096 629 sor, 2021-01-01 00:00 → 2026-11-21 10:28, 0 hézag, 0 duplikátum, séma azonos a SOL ohlcv-vel, árak épek (BTC 15 443–126 208, ETH 714–4 958), OHLC invariánsok OK; `load_ohlcv_1m` működik. (180 nulla-volumenű perc mindkettőn: valós Binance adat.)
- Mellékhatás: scheduler csak `trading.json`-ban szereplő + live DB-s assetet szinkronizál (`worker/assets.py::sync_asset_ids`) → btc/eth nem sync-aktív, KeyError nincs. UI/MCP: `has_data` miatt tabként megjelennek "(nincs auto-sync)" címkével (mint bchusdt); nem káros.
- features.json profilok érvényesek, a bchusdt_fw60-tól csak a description tér el.
- ruff/pyright tiszta; pytest (data_handling, worker, api, cq_mcp) 323 pass, csak a 3 ismert DB-állapot hiba.
- Doksi: overview asset tábla + 0002 database/ fa frissítve.
