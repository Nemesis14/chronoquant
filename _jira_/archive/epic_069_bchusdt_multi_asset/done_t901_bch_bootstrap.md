---
epic: epic_069
id: t901
title: BCH adat-bootstrap: OHLCV + funding + features + targets, adatminőség
assignee: database_agent
status: done
blocks: ["t906"]
blocked_by: ["t900"]
---

## Goal
A BCH live DB feltöltése a SOL-lal azonos folyamattal, majd adatminőség-ellenőrzés.

## Scope
`src/data_handling/` CLI-k (`02_sync_pipeline.py`, sync_ohlcv/funding/features/targets, `03_build_quant_train.py`, `01_validate_stats.py`) `--asset-id bchusdt`.

## Acceptance Criteria
- [x] Binance `onboardDate` / első elérhető 1m gyertya rögzítve a Notes-ban
- [x] `database/bchusdt/bchusdt.duckdb`: ohlcv, funding_rate, feat_ohlcv_quant, target(_fw60), quant_train, predictions (üres) táblák a SOL sémával
- [x] Adatminőség a Notes-ban: lyukak (idősor-hézag), nulla-volume sávok, outlierek, sorszámok; a SOL `src/data_handling/tests` sanity tesztjei BCH-ra futtatva (ismert live-data sanity hibák jelölve)
- [x] Javaslat a train/valid ablakra a rendelkezésre álló történet alapján
- [x] A hosszú letöltés háttérben fusson (run_in_background), a Binance rate limitet tartsd be

## Notes

**Futtatva:** `02_sync_pipeline.py --asset-id bchusdt --tables ohlcv --start "2019-12-19 00:00:00"` (1353 s), majd `--tables targets,features --chunk-months 3` (28 chunk), majd `03_build_quant_train.py --asset-id bchusdt`. Mind a `--asset-id` paraméterrel ment, beégetett solusdt-be nem ütköztem. `src/` nem módosult. (Windows: `PYTHONIOENCODING=utf-8` kell.)

**onboardDate** (fapi exchangeInfo, BCHUSDT PERPETUAL): 1576745700000 = 2019-12-19 08:55 UTC. Első tárolt 1m gyertya: 2019-12-19 08:57:00 (volume>0 az első sortól).

**Sorszámok** (`database/bchusdt/bchusdt.duckdb`, 2019-12-19 08:57 -> 2026-11-07 07:54):
| tábla | sor |
|---|---|
| ohlcv | 3 621 538 |
| funding_rate | 7 545 (2019-12-19 16:00 -> 2026-11-07 08:00) |
| feat_ohlcv_quant | 3 621 538 |
| target | 3 621 538 (60 sor long_mfe_fw60 NULL a vég miatt) |
| quant_train | 3 621 478 (-> 2026-11-07 06:54) |
| predictions | 0 (üres) |
target_fw360 nincs (BCH-n nem épült; a SOL DB-ben van, nem a pipeline része).

**Adatminőség:**
- Idősor-hézag: 0 (max open_time delta = 60 s). Nincs lyuk.
- Nulla-volume bárok: 579 (0,016%): 2019: 219, 2020: 152, 2021: 59, 2022: 68, 2023: 41, 2024: 21, 2025: 19 (2021-01-től 208). Szórt, nem összefüggő sávok.
- OHLC-inkonzisztencia (high<low, close/open a high-low kívül, open<=0): 0.
- Outlier: 90 bár |1m return|>5%, 27 bár >10%, max |return| 27,5%; 83 bár high-low/low>10%, a legnagyobb 2021-05-19 13:45 (+55,9%) a májusi crash.
- Funding: 8 órás ütem (SOL-hoz hasonlóan lehet 2h/4h szakasz, nem vizsgáltam); 2021-01-től 6410 sor.
- Sanity tesztek (`tests/store/sanity` + `tests/sync_tables/sanity`): a conftest az alapértelmezett assetet olvassa (nem paraméterezhető), ezért `utils.default_asset_id` monkeypatch-csel futtattam (scratchpadunt.py, src érintetlen). Eredmény: 62 pass, 2 skip, 2 fail:
  - `test_feat_close_position_differs_from_current_close`: 31/500 minta egyezik (küszöb <5) -> valószínűleg a 1m bárok close==high/low kis likviditású BCH-n gyakoribb (a küszöb SOL-ra hangolt), NEM igazolt hipotézis; ellenőrizendő (t902/t906).
  - `test_target_short_mfe_range`: short_mfe max=0,1525 (küszöb <0,1) -> a BCH extrém kitörései (pl. 2021-05-19) miatt valószínű, a küszöb SOL-specifikus.
  - A SOL 3 ismert hibája (no_time_gap, target_row_count, predictions_score_range) BCH-n nem jelent meg.

**Javasolt train/valid ablak** (a SOL mintájára: train 2021-01 -> 2026-01, valid 2026-02 -> 2026-07; embargo 240 perc, purge 60 perc): a BCH történet 2019-12 óta teljes, de 2019-2020 vékony/ritka (219+152 nulla-volume bár, korai piac). Javaslat: train 2021-01-01 -> 2026-01-31, valid 2026-02-01 -> 2026-07 (egyező a SOL-lal; quant_train sorok: 2021-01..2026-01 = 2 674 080, 2026-02-tól a végéig 402 175). Opcionális: 2020-ra bővített train csak kísérletként. A 2021-05-19 outlier-tartományt ne vágjuk ki automatikusan, de modeling_agent döntsön róla.

**Funding sync hiányzik a pipeline-ból** (`src/` alatt nincs fundingRate letöltő; a SOL tábla máshonnan került be). Ad-hoc script (scratchpadch_funding.py, kivonat):
```python
con.execute("CREATE TABLE IF NOT EXISTS funding_rate (funding_ts TIMESTAMP PRIMARY KEY, funding_rate DOUBLE)")
start = ms(2019-12-19 UTC)
while True:
    rows = requests.get("https://fapi.binance.com/fapi/v1/fundingRate",
        params={"symbol":"BCHUSDT","startTime":start,"limit":1000}).json()
    if not rows: break
    for x in rows: INSERT OR REPLACE (utcfromtimestamp(fundingTime/1000), float(fundingRate))
    start = rows[-1]["fundingTime"]+1
    if len(rows)<1000: break
    time.sleep(0.5)
```
Javaslat: külön ticket (database_agent) `sync_funding` modulra + pipeline `--tables funding` lépésre, hogy a BCH/SOL funding is frissüljön élőben.
